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

(* Back-off intervals (seconds) for the various retryable conditions.
   CLUSTERDOWN:  slots are unassigned cluster-wide; operator action
                 or failover in progress.
   TRYAGAIN:     the keys span a slot currently being migrated.
   Conn_lost:    the connection was torn down mid-flight (server-side
                 kill, docker restart, network partition). Retried
                 up to max_redirects times for every command, matching
                 GLIDE / SE.Redis / lettuce convention: at-least-once
                 semantics over at-most-once. The bound prevents
                 runaway duplication; applications that cannot
                 tolerate a tiny double-apply window under chaos
                 should use MULTI/EXEC or application-level
                 idempotency keys. *)
(* Fixed waits for the low-severity conditions. A slot migration
   (TRYAGAIN) and a torn-down connection typically clear in tens of
   ms, so a small fixed pause works fine. *)
let tryagain_backoff = 0.05
let conn_lost_backoff = 0.05

(* CLUSTERDOWN often lasts longer than the others — a failover has
   to elect + gossip + accept writes again, which can take a few
   seconds. Fixed 100 ms × max_redirects (≈ 500 ms total) was too
   tight and routinely exhausted the retry budget. Exponential
   back-off spends most of that budget at the tail:
     attempt 0 → 100 ms
     attempt 1 → 200 ms
     attempt 2 → 400 ms
     attempt 3 → 800 ms
     attempt 4 → 1.6 s     (cumulative ≈ 3.1 s)
   Capped at 1.6 s so we never block a user command indefinitely. *)
let clusterdown_backoff_for_attempt attempt =
  let base = 0.1 in
  let cap = 1.6 in
  let multiplier = 2. ** float_of_int attempt in
  Float.min cap (base *. multiplier)

let handle_retries ~pool ~topology_ref ~clock ~max_redirects ~trigger_refresh
    ?timeout ~dispatch args =
  let rec loop attempt result =
    match result with
    | Ok _ -> result
    | Error (Connection.Error.Server_error ve)
      when attempt < max_redirects ->
        (match Redirect.of_valkey_error ve with
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
                       loop (attempt + 1) next))
         | None ->
             (match ve.code with
              | "CLUSTERDOWN" ->
                  Eio.Time.sleep clock
                    (clusterdown_backoff_for_attempt attempt);
                  trigger_refresh ();
                  loop (attempt + 1) (dispatch ())
              | "TRYAGAIN" ->
                  Eio.Time.sleep clock tryagain_backoff;
                  loop (attempt + 1) (dispatch ())
              | _ -> result))
    | Error (Connection.Error.Interrupted | Connection.Error.Closed)
      when attempt < max_redirects ->
        (* Connection went away under this request. Wake the refresh
           fiber (the node may have fallen over) and re-dispatch;
           the pool entry will either reconnect, or the refreshed
           topology will route us to the new owner. Bounded by
           max_redirects so a persistently-down target still
           surfaces the error to the caller. *)
        trigger_refresh ();
        Eio.Time.sleep clock conn_lost_backoff;
        loop (attempt + 1) (dispatch ())
    | Error _ -> result
  in
  loop 0 (dispatch ())

let make_exec ~pool ~topology_ref ~clock ~max_redirects ~trigger_refresh
    ?timeout (target : Router.Target.t) (rf : Router.Read_from.t)
    (args : string array) =
  let dispatch () =
    let topology = !topology_ref in
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
    | Router.Target.By_channel _ ->
        err_terminal "cluster router: sharded pub/sub not yet implemented"
  in
  handle_retries ~pool ~topology_ref ~clock ~max_redirects
    ~trigger_refresh ?timeout ~dispatch args

(* Fan a single command out to every node in the selected set. Each
   query runs in its own fiber; a per-node failure never collapses the
   whole batch — it comes back as [Error _] in that node's slot. *)
let make_exec_multi ~pool ~topology_ref ?timeout
    (fan : Router.Fan_target.t) (args : string array) =
  let topology = !topology_ref in
  let nodes =
    match fan with
    | Router.Fan_target.All_nodes -> Topology.all_nodes topology
    | Router.Fan_target.All_primaries -> Topology.primaries topology
    | Router.Fan_target.All_replicas -> Topology.replicas topology
  in
  let dispatch_one (n : Topology.Node.t) =
    match Node_pool.get pool n.id with
    | None ->
        (n.id,
         Error (Connection.Error.Terminal
                  (Printf.sprintf "no live connection for node %s" n.id)))
    | Some conn ->
        (n.id, send_once ?timeout conn args)
  in
  Eio.Fiber.List.map dispatch_one nodes

let from_pool_and_topology ?(max_redirects = 5) ~clock ~pool ~topology () =
  let topology_ref = ref topology in
  let trigger_refresh () = () in
  let exec ?timeout target rf args =
    make_exec ~pool ~topology_ref ~clock ~max_redirects ~trigger_refresh
      ?timeout target rf args
  in
  let exec_multi ?timeout fan args =
    make_exec_multi ~pool ~topology_ref ?timeout fan args
  in
  let close () = Node_pool.close_all pool in
  let primary () =
    match Node_pool.connections pool with [] -> None | c :: _ -> Some c
  in
  Router.make ~exec ~exec_multi ~close ~primary

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

let apply_new_topology ~sw ~net ~clock ?domain_mgr ~cfg ~pool
    ~topology_atomic new_topo =
  let current = Atomic.get topology_atomic in
  if Topology.sha new_topo <> Topology.sha current then begin
    diff_pool ~sw ~net ~clock ?domain_mgr
      ~connection_config:cfg.Config.connection
      ~prefer_hostname:cfg.Config.prefer_hostname
      ~pool ~new_topology:new_topo ();
    Atomic.set topology_atomic new_topo
  end

let refresh_from_seeds ~sw ~net ~clock ?domain_mgr ~cfg ~pool
    ~topology_atomic () =
  match
    Discovery.discover_from_seeds
      ~sw ~net ~clock ?domain_mgr
      ~connection_config:cfg.Config.connection
      ~agreement_ratio:cfg.Config.agreement_ratio
      ~min_nodes_for_quorum:cfg.Config.min_nodes_for_quorum
      ~seeds:cfg.Config.seeds ()
  with
  | Ok new_topo ->
      apply_new_topology ~sw ~net ~clock ?domain_mgr ~cfg ~pool
        ~topology_atomic new_topo
  | Error _ -> ()

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
      apply_new_topology ~sw ~net ~clock ?domain_mgr ~cfg ~pool
        ~topology_atomic new_topo
  | No_agreement ->
      (* Pool is empty or every node is unreachable / disagrees. Fall
         back to the seed list — that is how the cluster was bootstrapped,
         and the only address set we can be sure is still meaningful to
         the operator. *)
      refresh_from_seeds ~sw ~net ~clock ?domain_mgr ~cfg ~pool
        ~topology_atomic ()

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
      let sync_ref () = topology_ref := Atomic.get topology_atomic in
      let exec ?timeout target rf args =
        (* Thread the atomic's latest value through the existing ref-based
           dispatch. On refresh the ref is re-synced. *)
        sync_ref ();
        make_exec ~pool ~topology_ref ~clock
          ~max_redirects:cfg.max_redirects ~trigger_refresh
          ?timeout target rf args
      in
      let exec_multi ?timeout fan args =
        sync_ref ();
        make_exec_multi ~pool ~topology_ref ?timeout fan args
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
      Ok (Router.make ~exec ~exec_multi ~close ~primary)
