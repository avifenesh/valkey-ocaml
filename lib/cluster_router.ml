module Config = struct
  type t = {
    seeds : (string * int) list;
    connection : Connection.Config.t;
    agreement_ratio : float;
    min_nodes_for_quorum : int;
    max_redirects : int;
    prefer_hostname : bool;
  }

  let default ~seeds = {
    seeds;
    connection = Connection.Config.default;
    agreement_ratio = 0.2;
    min_nodes_for_quorum = 3;
    max_redirects = 5;
    prefer_hostname = false;
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

let pick_node_by_read_from (rf : Client.Read_from.t) (shard : Topology.Shard.t) =
  let find_az az nodes =
    List.find_opt
      (fun (n : Topology.Node.t) -> n.availability_zone = Some az)
      nodes
  in
  match rf with
  | Client.Read_from.Primary -> shard.primary
  | Client.Read_from.Prefer_replica ->
      (match shard.replicas with
       | [] -> shard.primary
       | r :: _ -> r)
  | Client.Read_from.Az_affinity { az } ->
      (match find_az az shard.replicas with
       | Some r -> r
       | None -> shard.primary)
  | Client.Read_from.Az_affinity_replicas_and_primary { az } ->
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

let make_exec ~pool ~topology_ref ?timeout (target : Client.Target.t)
    (rf : Client.Read_from.t) (args : string array) =
  let topology = !topology_ref in
  match target with
  | Client.Target.By_slot slot ->
      (match Topology.shard_for_slot topology slot with
       | None -> err_protocol "no shard owns slot %d" slot
       | Some shard ->
           let node = pick_node_by_read_from rf shard in
           (match Node_pool.get pool node.id with
            | None ->
                err_terminal "no live connection for node %s" node.id
            | Some conn -> Connection.request ?timeout conn args))
  | Client.Target.By_node node_id ->
      (match Node_pool.get pool node_id with
       | None -> err_terminal "unknown node %s" node_id
       | Some conn -> Connection.request ?timeout conn args)
  | Client.Target.Random ->
      (match Node_pool.connections pool with
       | [] -> err_terminal "cluster has no live connections"
       | c :: _ -> Connection.request ?timeout c args)
  | Client.Target.All_nodes | Client.Target.All_primaries ->
      err_terminal "cluster router: fan-out targets not yet implemented"
  | Client.Target.By_channel _ ->
      err_terminal "cluster router: sharded pub/sub not yet implemented"

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
      let topology_ref = ref topology in
      let exec ?timeout target rf args =
        make_exec ~pool ~topology_ref ?timeout target rf args
      in
      let close () = Node_pool.close_all pool in
      let primary () = None in
      Ok (Client.Router.make ~exec ~close ~primary)
