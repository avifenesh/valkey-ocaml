module Config = struct
  type t = {
    seeds : (string * int) list;
    connection : Connection.Config.t;
    agreement_ratio : float;
    min_nodes_for_quorum : int;
    max_redirects : int;
    prefer_hostname : bool;
    refresh_interval : float;
    refresh_jitter : float;
  }

  let default ~seeds = {
    seeds;
    connection = Connection.Config.default;
    agreement_ratio = 0.2;
    min_nodes_for_quorum = 3;
    max_redirects = 5;
    prefer_hostname = false;
    refresh_interval = 15.0;
    refresh_jitter = 15.0;
  }
end

let address_of_node ~prefer_hostname (node : Topology.Node.t) =
  let non_empty = function
    | Some "" | Some "?" | None -> None
    | Some s -> Some s
  in
  let from_hostname = if prefer_hostname then non_empty node.hostname else None in
  match from_hostname with
  | Some h -> Some h
  | None ->
      (match non_empty node.ip with
       | Some ip -> Some ip
       | None -> non_empty node.endpoint)

let port_of_node ~tls (node : Topology.Node.t) =
  if tls then
    match node.tls_port with
    | Some p -> Some p
    | None -> node.port
  else
    match node.port with
    | Some p -> Some p
    | None -> node.tls_port

let pick_node_by_read_from (rf : Router.Read_from.t) (shard : Topology.Shard.t) =
  let find_az az nodes =
    List.find_opt
      (fun (n : Topology.Node.t) -> n.availability_zone = Some az)
      nodes
  in
  match rf with
  | Router.Read_from.Primary -> shard.primary
  | Router.Read_from.Prefer_replica ->
      (match shard.replicas with
       | [] -> shard.primary
       | r :: _ -> r)
  | Router.Read_from.Az_affinity { az } ->
      (match find_az az shard.replicas with
       | Some r -> r
       | None -> shard.primary)
  | Router.Read_from.Az_affinity_replicas_and_primary { az } ->
      let all_nodes = shard.primary :: shard.replicas in
      (match find_az az all_nodes with
       | Some n -> n
       | None -> shard.primary)

let build_pool ~sw ~net ~clock ?domain_mgr ~connection_config ~prefer_hostname
    topology =
  let pool = Node_pool.create () in
  let tls_enabled = connection_config.Connection.Config.tls <> None in
  List.iter
    (fun (node : Topology.Node.t) ->
      if node.health = Topology.Node.Online then begin
        match
          address_of_node ~prefer_hostname node,
          port_of_node ~tls:tls_enabled node
        with
        | Some host, Some port ->
            (try
               let conn =
                 Connection.connect ~sw ~net ~clock ?domain_mgr
                   ~config:connection_config ~host ~port ()
               in
               Node_pool.add pool node.id conn
             with _ -> ())
        | _ -> ()
      end)
    (Topology.all_nodes topology);
  pool

let err_protocol fmt =
  Format.kasprintf
    (fun s -> Error (Connection.Error.Protocol_violation s))
    fmt

let err_terminal fmt =
  Format.kasprintf
    (fun s -> Error (Connection.Error.Terminal s))
    fmt

(* Execute [args] once against [conn]. Used for both the initial dispatch
   and retries after a redirect. *)
let send_once ?timeout conn args = Connection.request ?timeout conn args

let handle_redirect ~pool ~topology_ref ~max_redirects ~trigger_refresh
    ?timeout first_result args =
  let rec loop attempt result =
    match result with
    | Ok _ -> result
    | Error (Connection.Error.Server_error ve) when attempt < max_redirects ->
        (match Redirect.of_valkey_error ve with
         | None -> result
         | Some { kind; host; port; _ } ->
             (match
                Topology.find_node_by_address !topology_ref ~host ~port
              with
              | None ->
                  (* Unknown address: wake the refresh fiber so the next
                     call sees the new layout. Surface this attempt's
                     error now rather than blocking on the refresh. *)
                  trigger_refresh ();
                  result
              | Some node ->
                  (match Node_pool.get pool node.id with
                   | None -> result
                   | Some conn ->
                       (* For ASK, send ASKING before the original. For
                          MOVED, just retry on the new node. *)
                       (match kind with
                        | Redirect.Ask ->
                            (match send_once ?timeout conn [| "ASKING" |] with
                             | Ok _ -> ()
                             | Error _ -> ());
                        | Redirect.Moved -> ());
                       let next = send_once ?timeout conn args in
                       loop (attempt + 1) next)))
    | Error _ -> result
  in
  loop 0 first_result

let make_exec ~pool ~topology_ref ~max_redirects ~trigger_refresh ?timeout
    (target : Router.Target.t) (rf : Router.Read_from.t)
    (args : string array) =
  let topology = !topology_ref in
  let dispatch_initial () =
    match target with
    | Router.Target.By_slot slot ->
        (match Topology.shard_for_slot topology slot with
         | None -> err_protocol "no shard owns slot %d" slot
         | Some shard ->
             let node = pick_node_by_read_from rf shard in
             (match Node_pool.get pool node.id with
              | None ->
                  err_terminal "no live connection for node %s" node.id
              | Some conn -> send_once ?timeout conn args))
    | Router.Target.By_node node_id ->
        (match Node_pool.get pool node_id with
         | None -> err_terminal "unknown node %s" node_id
         | Some conn -> send_once ?timeout conn args)
    | Router.Target.Random ->
        (match Node_pool.connections pool with
         | [] -> err_terminal "cluster has no live connections"
         | c :: _ -> send_once ?timeout c args)
    | Router.Target.All_nodes | Router.Target.All_primaries ->
        err_terminal "cluster router: fan-out targets not yet implemented"
    | Router.Target.By_channel _ ->
        err_terminal "cluster router: sharded pub/sub not yet implemented"
  in
  handle_redirect ~pool ~topology_ref ~max_redirects ~trigger_refresh
    ?timeout (dispatch_initial ()) args

let from_pool_and_topology ?(max_redirects = 5) ~pool ~topology () =
  let topology_ref = ref topology in
  let trigger_refresh () = () in
  let exec ?timeout target rf args =
    make_exec ~pool ~topology_ref ~max_redirects ~trigger_refresh
      ?timeout target rf args
  in
  let close () = Node_pool.close_all pool in
  let primary () =
    match Node_pool.connections pool with [] -> None | c :: _ -> Some c
  in
  Router.make ~exec ~close ~primary

(* ---------- refresh fiber ---------- *)

let diff_pool ~sw ~net ~clock ?domain_mgr ~connection_config
    ~prefer_hostname ~pool ~new_topology () =
  let new_nodes = Topology.all_nodes new_topology in
  let new_ids =
    List.map (fun (n : Topology.Node.t) -> n.id) new_nodes
  in
  let old_ids = Node_pool.node_ids pool in
  (* Close removed *)
  List.iter
    (fun id ->
      if not (List.mem id new_ids) then
        match Node_pool.remove pool id with
        | Some c -> (try Connection.close c with _ -> ())
        | None -> ())
    old_ids;
  (* Add new (online nodes only) *)
  let tls_enabled = connection_config.Connection.Config.tls <> None in
  List.iter
    (fun (n : Topology.Node.t) ->
      if not (List.mem n.id old_ids) && n.health = Topology.Node.Online
      then
        match
          address_of_node ~prefer_hostname n,
          port_of_node ~tls:tls_enabled n
        with
        | Some host, Some port ->
            (try
               let conn =
                 Connection.connect ~sw ~net ~clock ?domain_mgr
                   ~config:connection_config ~host ~port ()
               in
               Node_pool.add pool n.id conn
             with _ -> ())
        | _ -> ())
    new_nodes

let query_pool_for_topology pool =
  let conns = Node_pool.connections pool in
  Eio.Fiber.List.map
    (fun c ->
      match Connection.request c [| "CLUSTER"; "SHARDS" |] with
      | Ok reply ->
          (match Topology.of_cluster_shards reply with
           | Ok t -> Some t
           | Error _ -> None)
      | Error _ -> None)
    conns

let refresh_once ~sw ~net ~clock ?domain_mgr ~cfg ~pool ~topology_atomic () =
  let views = query_pool_for_topology pool in
  let queried = List.length views in
  match
    Discovery.select
      ~agreement_ratio:cfg.Config.agreement_ratio
      ~min_nodes_for_quorum:cfg.Config.min_nodes_for_quorum
      ~queried ~views
  with
  | Agreed new_topo | Agreed_fallback new_topo ->
      let current = Atomic.get topology_atomic in
      if Topology.sha new_topo <> Topology.sha current then begin
        diff_pool ~sw ~net ~clock ?domain_mgr
          ~connection_config:cfg.Config.connection
          ~prefer_hostname:cfg.Config.prefer_hostname
          ~pool ~new_topology:new_topo ();
        Atomic.set topology_atomic new_topo
      end
  | No_agreement -> ()

let refresh_loop ~sw ~net ~clock ?domain_mgr ~cfg ~pool
    ~topology_atomic ~refresh_signal ~refresh_mutex ~closing () =
  let rec loop () =
    if Atomic.get closing then ()
    else
      let interval =
        cfg.Config.refresh_interval
        +. Random.float cfg.Config.refresh_jitter
      in
      let _ : [ `Elapsed | `Signal ] =
        Eio.Fiber.first
          (fun () -> Eio.Time.sleep clock interval; `Elapsed)
          (fun () ->
            Eio.Mutex.use_rw ~protect:true refresh_mutex (fun () ->
                Eio.Condition.await refresh_signal refresh_mutex);
            `Signal)
      in
      if Atomic.get closing then ()
      else begin
        (try
           refresh_once ~sw ~net ~clock ?domain_mgr ~cfg ~pool
             ~topology_atomic ()
         with _ -> ());
        loop ()
      end
  in
  try loop () with _ -> ()

let create ~sw ~net ~clock ?domain_mgr ~config:(cfg : Config.t) () =
  match
    Discovery.discover_from_seeds
      ~sw ~net ~clock ?domain_mgr
      ~connection_config:cfg.connection
      ~agreement_ratio:cfg.agreement_ratio
      ~min_nodes_for_quorum:cfg.min_nodes_for_quorum
      ~seeds:cfg.seeds ()
  with
  | Error e -> Error e
  | Ok topology ->
      let pool =
        build_pool ~sw ~net ~clock ?domain_mgr
          ~connection_config:cfg.connection
          ~prefer_hostname:cfg.prefer_hostname
          topology
      in
      let topology_atomic = Atomic.make topology in
      let closing = Atomic.make false in
      let refresh_signal = Eio.Condition.create () in
      let refresh_mutex = Eio.Mutex.create () in
      Eio.Fiber.fork ~sw (fun () ->
          refresh_loop ~sw ~net ~clock ?domain_mgr ~cfg ~pool
            ~topology_atomic ~refresh_signal ~refresh_mutex ~closing ());
      let topology_ref = ref topology in
      let trigger_refresh () = Eio.Condition.broadcast refresh_signal in
      let exec ?timeout target rf args =
        (* Thread the atomic's latest value through the existing ref-based
           dispatch. On refresh the ref is re-synced. *)
        topology_ref := Atomic.get topology_atomic;
        make_exec ~pool ~topology_ref ~max_redirects:cfg.max_redirects
          ~trigger_refresh ?timeout target rf args
      in
      let close () =
        Atomic.set closing true;
        Eio.Condition.broadcast refresh_signal;
        Node_pool.close_all pool
      in
      let primary () =
        match Node_pool.connections pool with
        | [] -> None
        | c :: _ -> Some c
      in
      Ok (Router.make ~exec ~close ~primary)
