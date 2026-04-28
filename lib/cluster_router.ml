module Config = struct
  type topology_hooks = {
    on_node_removed : node_id:string -> unit;
    on_node_refreshed : node_id:string -> unit;
  }
  (** Callbacks fired from the topology refresh path when a
      node disappears ([on_node_removed]) or keeps its id but
      changes endpoint / role ([on_node_refreshed]).
      Default hooks are no-ops; [Client] threads these through
      to [Blocking_pool.drain_node] / [refresh_node] when a
      pool is configured, so pool buckets for removed nodes
      close their idle conns and mark in-flight leases dirty. *)

  let ignore_hooks = {
    on_node_removed = (fun ~node_id:_ -> ());
    on_node_refreshed = (fun ~node_id:_ -> ());
  }

  type t = {
    seeds : (string * int) list;
    connection : Connection.Config.t;
    agreement_ratio : float;
    min_nodes_for_quorum : int;
    max_redirects : int;
    prefer_hostname : bool;
    refresh_interval : float;
    refresh_jitter : float;
    connections_per_node : int;
    topology_hooks : topology_hooks;
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
    connections_per_node = 1;
    topology_hooks = ignore_hooks;
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

(* Randomised pick from a candidate list. Spreads readonly traffic
   across replicas instead of pinning every read from one client to
   the first replica. Uses the global Random module — for routing,
   crypto-grade randomness isn't needed and the slight per-process
   determinism is preferable to threading per-router RNG state.
   Callers who care about distribution across processes can call
   [Random.self_init ()] at startup. *)
let pick_random nodes =
  match nodes with
  | [] -> None
  | _ -> Some (List.nth nodes (Random.int (List.length nodes)))

(* AZ-affinity fallback chain matches GLIDE / lettuce / SE.Redis:
     tier 1  preferred candidate set (in-AZ replicas, or in-AZ
             nodes for the +primary variant)
     tier 2  any other replica (still off the primary)
     tier 3  primary
   The earlier implementation jumped straight from tier 1 to
   tier 3, which gave up unnecessary read capacity whenever no
   replica lived in the requested AZ. *)
let pick_node_by_read_from (rf : Router.Read_from.t) (shard : Topology.Shard.t) =
  let in_az az nodes =
    List.filter
      (fun (n : Topology.Node.t) -> n.availability_zone = Some az)
      nodes
  in
  let any_replica_or_primary () =
    match pick_random shard.replicas with
    | Some r -> r
    | None -> shard.primary
  in
  match rf with
  | Router.Read_from.Primary -> shard.primary
  | Router.Read_from.Prefer_replica -> any_replica_or_primary ()
  | Router.Read_from.Az_affinity { az } ->
      (match pick_random (in_az az shard.replicas) with
       | Some r -> r
       | None -> any_replica_or_primary ())
  | Router.Read_from.Az_affinity_replicas_and_primary { az } ->
      let in_az_pool = in_az az (shard.primary :: shard.replicas) in
      (match pick_random in_az_pool with
       | Some n -> n
       | None -> any_replica_or_primary ())

(* Connect [n] independent Connections to [(host, port)]. All-or-
   nothing: if any one fails, close the ones we'd already opened
   and return [None]. A bundle with fewer than [n] conns would
   violate the invariant every call site assumes (round-robin
   distribution, slot-affinity indexing), and silently degraded
   throughput is the exact failure mode the review flagged.

   Handshakes run concurrently. On a slow network where a TLS
   handshake is RTT-bound (50-200 ms), sequential [Array.init]
   serialises N round-trips; [Fiber.List.init] parallelises them
   under one switch. The outer node loop is also parallel —
   cluster startup time becomes max(single-node handshake),
   not sum. *)
let connect_bundle ~sw ~net ~clock ?domain_mgr ~connection_config
    ~host ~port n =
  let outcomes =
    Eio.Fiber.List.map
      (fun _ ->
        try
          Ok (Connection.connect ~sw ~net ~clock ?domain_mgr
                ~config:connection_config ~host ~port ())
        with
        | Connection.Handshake_failed _
        | Eio.Io _
        | End_of_file
        | Unix.Unix_error _ as e -> Error e)
      (List.init n (fun _ -> ()))
  in
  let oks = List.filter_map (function Ok c -> Some c | Error _ -> None) outcomes in
  let any_err = List.exists (function Error _ -> true | _ -> false) outcomes in
  if any_err then begin
    List.iter
      (fun c ->
        try Connection.close c
        with Eio.Io _ | End_of_file | Invalid_argument _
           | Unix.Unix_error _ -> ())
      oks;
    None
  end
  else Some (Array.of_list oks)

let build_pool ~sw ~net ~clock ?domain_mgr ~connection_config
    ~connections_per_node ~prefer_hostname topology =
  let pool = Node_pool.create () in
  let tls_enabled = connection_config.Connection.Config.tls <> None in
  (* Fan-out per node: each node's [connect_bundle] itself
     parallelises the per-conn handshakes, and this outer
     [Fiber.List.iter] parallelises across nodes. Startup time
     is max(slowest-node handshake), not sum. *)
  Eio.Fiber.List.iter
    (fun (node : Topology.Node.t) ->
      if node.health = Topology.Node.Online then begin
        match
          address_of_node ~prefer_hostname node,
          port_of_node ~tls:tls_enabled node
        with
        | Some host, Some port ->
            (match
               connect_bundle ~sw ~net ~clock ?domain_mgr
                 ~connection_config ~host ~port connections_per_node
             with
             | Some bundle ->
                 Node_pool.add_bundle pool ~node_id:node.id bundle
             | None -> ())
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
    ?(sync_ref = ignore) ?timeout ~dispatch args =
  let dispatch () = sync_ref (); dispatch () in
  let rec loop attempt result =
    match result with
    | Ok _ -> result
    | Error (Connection.Error.Server_error ve)
      when attempt < max_redirects ->
        (match Redirect.of_valkey_error ve with
         | Some { kind; slot; host; port } ->
             (match
                Topology.find_node_by_address !topology_ref ~host ~port
              with
              | None ->
                  (* Redirect target not in topology — topology has
                     shifted; the eager Cache.clear in trigger_refresh
                     is correct. *)
                  trigger_refresh ();
                  result
              | Some node ->
                  (* MOVED + node already in topology can mean either
                     (a) an in-flight redirect against an already-fresh
                     topology (no refresh needed), or (b) the cached
                     topology disagrees and is stale. Distinguish by
                     comparing the slot's owner. ASK is per-key and
                     never implies ownership change. *)
                  (match kind with
                   | Redirect.Ask -> ()
                   | Redirect.Moved ->
                       let topology_agrees =
                         match Topology.shard_for_slot !topology_ref slot with
                         | Some shard -> shard.primary.id = node.id
                         | None -> false
                       in
                       if not topology_agrees then trigger_refresh ());
                  (match
                     Node_pool.pick_for_slot pool ~node_id:node.id ~slot
                   with
                   | None ->
                       (* Pool doesn't have the redirect target —
                          race against pool-diff. Trigger refresh so
                          the next attempt sees the new pool entry. *)
                       trigger_refresh ();
                       result
                   | Some conn ->
                       let next =
                         match kind with
                         | Redirect.Moved -> send_once ?timeout conn args
                         | Redirect.Ask ->
                             (* ASKING is one-shot: consumed by the
                                very next command on the connection
                                regardless of which fiber sent it.
                                Two separate [send_once] calls would
                                let another fiber's command interleave
                                between [ASKING] and the retry on a
                                shared node connection, eating the
                                flag and bouncing this command as
                                MOVED. Pipeline both wires as one
                                indivisible submit. *)
                             (match
                                Connection.request_pair ?timeout conn
                                  [| "ASKING" |] args
                              with
                              | Error e -> Error e
                              | Ok (asking_reply, cmd_reply) ->
                                  (match asking_reply with
                                   | Ok (Resp3.Simple_string "OK") ->
                                       cmd_reply
                                   | Ok unexpected ->
                                       Error
                                         (Connection.Error.Protocol_violation
                                            (Format.asprintf
                                               "ASKING: unexpected reply %a"
                                               Resp3.pp unexpected))
                                   | Error e -> Error e))
                       in
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

(* Pair-shape mirror of [handle_retries] for OPTIN CSC reads.
   Retry policy matches [exec] one-to-one. ASK on the read uses
   [request_triple] to send [ASKING + CACHING YES + read] as
   three wire-adjacent frames on the new owner; the ASKING
   reply is hidden so the caller still sees the pair shape. *)
let make_pair ~pool ~topology_ref ~clock ~max_redirects ~trigger_refresh
    ?(sync_ref = ignore) ?timeout
    (target : Router.Target.t)
    (args1 : string array) (args2 : string array) =
  let send_on conn = Connection.request_pair ?timeout conn args1 args2 in
  let dispatch_inner () =
    match target with
    | Router.Target.By_slot slot ->
        (match Topology.shard_for_slot !topology_ref slot with
         | None -> err_protocol "no shard owns slot %d" slot
         | Some shard ->
             (match
                Node_pool.pick_for_slot pool ~node_id:shard.primary.id ~slot
              with
              | None ->
                  err_terminal "no live connection for node %s"
                    shard.primary.id
              | Some conn -> send_on conn))
    | Router.Target.By_node node_id ->
        (match Node_pool.pick pool ~node_id with
         | None -> err_terminal "unknown node %s" node_id
         | Some conn -> send_on conn)
    | Router.Target.Random | Router.Target.By_channel _ ->
        err_terminal
          "Router.pair: only By_slot / By_node supported (OPTIN reads)"
  in
  let dispatch () = sync_ref (); dispatch_inner () in
  let rec loop attempt result =
    match result with
    | Error (Connection.Error.Interrupted | Connection.Error.Closed)
      when attempt < max_redirects ->
        trigger_refresh ();
        Eio.Time.sleep clock conn_lost_backoff;
        loop (attempt + 1) (dispatch ())
    | Error _ -> result
    | Ok (Error (Connection.Error.Interrupted | Connection.Error.Closed), _)
      when attempt < max_redirects ->
        (* CACHING transport error mid-pair: refresh + re-dispatch
           with the same backoff as outer Closed. *)
        trigger_refresh ();
        Eio.Time.sleep clock conn_lost_backoff;
        loop (attempt + 1) (dispatch ())
    | Ok (Error _, _) -> result
    | Ok (Ok _, Ok _) -> result
    | Ok (Ok _, Error (Connection.Error.Server_error ve))
      when attempt < max_redirects ->
        (match Redirect.of_valkey_error ve with
         | Some { kind = Redirect.Moved; host; port; slot } ->
             (match
                Topology.find_node_by_address !topology_ref ~host ~port
              with
              | None ->
                  trigger_refresh ();
                  result
              | Some node ->
                  let topology_agrees =
                    match Topology.shard_for_slot !topology_ref slot with
                    | Some shard -> shard.primary.id = node.id
                    | None -> false
                  in
                  if not topology_agrees then trigger_refresh ();
                  (match
                     Node_pool.pick_for_slot pool ~node_id:node.id ~slot
                   with
                   | None ->
                       trigger_refresh ();
                       result
                   | Some conn ->
                       loop (attempt + 1) (send_on conn)))
         | Some { kind = Redirect.Ask; host; port; slot; _ } ->
             (* ASK retry: send [CACHING YES + ASKING + read] as
                three wire-adjacent frames on the new owner.
                Order is load-bearing — the server's [ASKING] flag
                is consumed by the very next command on that
                connection regardless of what it is, so [ASKING]
                must sit immediately before the slot-keyed read.
                Putting [ASKING] first would let [CACHING YES]
                eat the flag, and the read would land on the
                importing primary without ASKING and bounce as
                MOVED. We hide the ASKING reply (must be +OK)
                and return only the CACHING + read replies, so
                the caller still sees the pair shape. *)
             (match
                Topology.find_node_by_address !topology_ref ~host ~port
              with
              | None -> trigger_refresh (); result
              | Some node ->
                  (match
                     Node_pool.pick_for_slot pool ~node_id:node.id ~slot
                   with
                   | None -> trigger_refresh (); result
                   | Some conn ->
                       let triple =
                         Connection.request_triple ?timeout conn
                           args1 [| "ASKING" |] args2
                       in
                       (match triple with
                        | Error e -> Error e
                        | Ok (caching, asking, read) ->
                            (match asking with
                             | Ok (Resp3.Simple_string "OK") ->
                                 Ok (caching, read)
                             | Ok unexpected ->
                                 Error
                                   (Connection.Error.Protocol_violation
                                      (Format.asprintf
                                         "ASKING: unexpected reply %a"
                                         Resp3.pp unexpected))
                             | Error e -> Error e))))
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
    | Ok (Ok _, Error _) -> result
  in
  loop 0 (dispatch ())

let make_exec ~pool ~topology_ref ~clock ~max_redirects ~trigger_refresh
    ?(sync_ref = ignore) ?timeout
    (target : Router.Target.t) (rf : Router.Read_from.t)
    (args : string array) =
  let dispatch () =
    let topology = !topology_ref in
    match target with
    | Router.Target.By_slot slot ->
        (match Topology.shard_for_slot topology slot with
         | None -> err_protocol "no shard owns slot %d" slot
         | Some shard ->
             let node = pick_node_by_read_from rf shard in
             (* Round-robin across the node's bundle — single-frame
                dispatch doesn't need slot affinity. *)
             (match Node_pool.pick pool ~node_id:node.id with
              | None ->
                  err_terminal "no live connection for node %s" node.id
              | Some conn -> send_once ?timeout conn args))
    | Router.Target.By_node node_id ->
        (match Node_pool.pick pool ~node_id with
         | None -> err_terminal "unknown node %s" node_id
         | Some conn -> send_once ?timeout conn args)
    | Router.Target.Random ->
        (match Node_pool.all_connections pool with
         | [] -> err_terminal "cluster has no live connections"
         | conns ->
             (match pick_random conns with
              | Some c -> send_once ?timeout c args
              | None -> err_terminal "cluster has no live connections"))
    | Router.Target.By_channel _ ->
        err_terminal "cluster router: sharded pub/sub not yet implemented"
  in
  handle_retries ~pool ~topology_ref ~clock ~max_redirects
    ~trigger_refresh ~sync_ref ?timeout ~dispatch args

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
    match Node_pool.pick pool ~node_id:n.id with
    | None ->
        (n.id,
         Error (Connection.Error.Terminal
                  (Printf.sprintf "no live connection for node %s" n.id)))
    | Some conn ->
        (n.id, send_once ?timeout conn args)
  in
  Eio.Fiber.List.map dispatch_one nodes

let connection_for_slot_via ~pool ~topology_ref slot =
  match Topology.shard_for_slot !topology_ref slot with
  | None -> None
  | Some shard ->
      (* Slot-affinity pin: MULTI/EXEC and WATCH/EXEC span a
         connection-scoped transaction, so the caller needs a
         stable conn for [slot]. At N = 1 this is the single
         bundle entry; at N > 1 it's [bundle.(slot mod N)]. *)
      Node_pool.pick_for_slot pool ~node_id:shard.primary.id ~slot

(* Resolve [slot] to (primary_id, host, port) using the current
   topology and the provided [prefer_hostname] preference. The
   address choice mirrors [address_of_node] so every dedicated
   consumer (Cluster_pubsub today) reaches the same endpoint the
   pool dials. *)
let endpoint_for_slot_via ~topology_ref ~prefer_hostname
    ~connection_config slot =
  match Topology.shard_for_slot !topology_ref slot with
  | None -> None
  | Some shard ->
      let tls_enabled =
        connection_config.Connection.Config.tls <> None
      in
      match
        address_of_node ~prefer_hostname shard.primary,
        port_of_node ~tls:tls_enabled shard.primary
      with
      | Some host, Some port ->
          Some (shard.primary.id, host, port)
      | _ -> None

(* By-node lookup for [Blocking_pool]. Walks the current
   topology, matches [node_id], returns [(host, port)] using
   the same address / port helpers as the pool-dial path so
   every dedicated consumer (pubsub, blocking pool) reaches
   the same endpoint. *)
let endpoint_for_node_via ~topology_ref ~prefer_hostname
    ~connection_config ~node_id =
  let tls_enabled =
    connection_config.Connection.Config.tls <> None
  in
  List.find_map
    (fun (n : Topology.Node.t) ->
      if n.id <> node_id then None
      else
        match
          address_of_node ~prefer_hostname n,
          port_of_node ~tls:tls_enabled n
        with
        | Some host, Some port -> Some (host, port)
        | _ -> None)
    (Topology.all_nodes !topology_ref)

(* Per-primary mutex table for atomic-batch serialisation. The
   key is the primary's node_id (stable across refreshes for
   unchanged nodes). Lazily grown; the guard mutex protects the
   hashtable from concurrent insert races. See Router.mli
   [atomic_lock_for_slot]. *)
let make_atomic_lock_table () =
  let locks : (string, Eio.Mutex.t) Hashtbl.t = Hashtbl.create 16 in
  let guard = Eio.Mutex.create () in
  fun primary_id ->
    Eio.Mutex.use_rw ~protect:true guard (fun () ->
        match Hashtbl.find_opt locks primary_id with
        | Some m -> m
        | None ->
            let m = Eio.Mutex.create () in
            Hashtbl.add locks primary_id m;
            m)

let atomic_lock_for_slot_via ~topology_ref ~for_primary slot =
  match Topology.shard_for_slot !topology_ref slot with
  | Some shard -> for_primary shard.primary.id
  | None ->
      (* Slot is unowned in the current topology. The atomic op
         will fail when it asks for the connection; return a
         throwaway mutex so the lookup itself doesn't raise. *)
      Eio.Mutex.create ()

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
  (* Standalone-as-cluster pair: one node, no redirect handling.
     With the bundle model we round-robin across the single node's
     conns; at N = 1 this is the single bundle entry. *)
  let pair ?timeout _target args1 args2 =
    match Node_pool.node_ids pool with
    | [] ->
        Error
          (Connection.Error.Terminal
             "cluster router: no live connections")
    | node_id :: _ ->
        (match Node_pool.pick pool ~node_id with
         | None ->
             Error
               (Connection.Error.Terminal
                  "cluster router: no live connections")
         | Some conn -> Connection.request_pair ?timeout conn args1 args2)
  in
  let close () = Node_pool.close_all pool in
  let primary () =
    match Node_pool.all_connections pool with [] -> None | c :: _ -> Some c
  in
  let connection_for_slot slot =
    connection_for_slot_via ~pool ~topology_ref slot
  in
  (* Standalone/synthetic-topology path: without a real connection
     config here, just use defaults. [Cluster_pubsub] is cluster-only
     so this branch should never be queried in anger. *)
  let endpoint_for_slot slot =
    endpoint_for_slot_via ~topology_ref
      ~prefer_hostname:false
      ~connection_config:Connection.Config.default
      slot
  in
  let endpoint_for_node ~node_id =
    endpoint_for_node_via ~topology_ref
      ~prefer_hostname:false
      ~connection_config:Connection.Config.default
      ~node_id
  in
  let is_standalone =
    match Topology.primaries topology, Topology.replicas topology with
    | [ primary ], [] ->
        primary.Topology.Node.id = Topology.standalone_node_id
    | _ -> false
  in
  let for_primary = make_atomic_lock_table () in
  let atomic_lock_for_slot slot =
    atomic_lock_for_slot_via ~topology_ref ~for_primary slot
  in
  Router.make ~exec ~exec_multi ~pair ~close ~primary
    ~connection_for_slot ~endpoint_for_slot ~endpoint_for_node
    ~is_standalone ~atomic_lock_for_slot

(* ---------- refresh fiber ---------- *)

let diff_pool ~sw ~net ~clock ?domain_mgr ~connection_config
    ~connections_per_node ~prefer_hostname ~topology_hooks
    ~pool ~new_topology () =
  let new_nodes = Topology.all_nodes new_topology in
  let new_ids =
    List.map (fun (n : Topology.Node.t) -> n.id) new_nodes
  in
  let old_ids = Node_pool.node_ids pool in
  (* Close removed, then fire [on_node_removed] so the
     Blocking_pool (if any) drops idle conns and marks
     in-flight leases dirty. Callback fires AFTER the bundle
     is gone so the pool's view of the Router-visible
     topology is consistent when it drains. *)
  List.iter
    (fun id ->
      if not (List.mem id new_ids) then begin
        (match Node_pool.remove_bundle pool ~node_id:id with
         | Some arr ->
             Array.iter
               (fun c ->
                 try Connection.close c
                 with Eio.Io _ | End_of_file | Invalid_argument _
                    | Unix.Unix_error _ -> ())
               arr
         | None -> ());
        (try topology_hooks.Config.on_node_removed ~node_id:id
         with _ -> ())
      end)
    old_ids;
  (* Add new (online nodes only) *)
  let tls_enabled = connection_config.Connection.Config.tls <> None in
  Eio.Fiber.List.iter
    (fun (n : Topology.Node.t) ->
      if not (List.mem n.id old_ids) && n.health = Topology.Node.Online
      then
        match
          address_of_node ~prefer_hostname n,
          port_of_node ~tls:tls_enabled n
        with
        | Some host, Some port ->
            (match
               connect_bundle ~sw ~net ~clock ?domain_mgr
                 ~connection_config ~host ~port connections_per_node
             with
             | Some bundle ->
                 Node_pool.add_bundle pool ~node_id:n.id bundle
             | None -> ())
        | _ -> ())
    new_nodes

let query_pool_for_topology pool =
  let conns = Node_pool.all_connections pool in
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
      ~connections_per_node:cfg.Config.connections_per_node
      ~prefer_hostname:cfg.Config.prefer_hostname
      ~topology_hooks:cfg.Config.topology_hooks
      ~pool ~new_topology:new_topo ();
    Atomic.set topology_atomic new_topo;
    (* Topology changed — slot ownership may have shifted, failover
       may have rotated primaries. The server on any reconnected
       shard has forgotten our CLIENT TRACKING context, so entries
       in the CSC cache keyed by keys that route to those shards
       are now un-tracked on the server side: an external write
       would not produce an invalidation for us. Conservative fix:
       clear the whole CSC cache on any topology change. Matches
       redis-py. See docs/client-side-caching.md step 7. *)
    (match cfg.Config.connection.Connection.Config.client_cache with
     | None -> ()
     | Some ccfg -> Cache.clear ccfg.Client_cache.cache)
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
  Observability.refresh_span (fun span ->
    let views = query_pool_for_topology pool in
    let queried = List.length views in
    match
      Discovery.select
        ~agreement_ratio:cfg.Config.agreement_ratio
        ~min_nodes_for_quorum:cfg.Config.min_nodes_for_quorum
        ~queried ~views
    with
    | Agreed new_topo ->
        Observability.record_discovery_outcome span `Agreed;
        apply_new_topology ~sw ~net ~clock ?domain_mgr ~cfg ~pool
          ~topology_atomic new_topo
    | Agreed_fallback new_topo ->
        Observability.record_discovery_outcome span `Agreed_fallback;
        apply_new_topology ~sw ~net ~clock ?domain_mgr ~cfg ~pool
          ~topology_atomic new_topo
    | No_agreement ->
        Observability.record_discovery_outcome span `No_agreement;
        (* Pool is empty or every node is unreachable / disagrees. Fall
           back to the seed list — that is how the cluster was bootstrapped,
           and the only address set we can be sure is still meaningful to
           the operator. *)
        refresh_from_seeds ~sw ~net ~clock ?domain_mgr ~cfg ~pool
          ~topology_atomic ())

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
  Node_pool.validate_bundle_size cfg.connections_per_node;
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
          ~connections_per_node:cfg.connections_per_node
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
      (* Eager Cache.clear in addition to the refresh fiber's own
         clear-on-sha-change: refresh is async (CLUSTER SHARDS
         round-trip), so without an eager clear a second cached
         read on a different key from the same migrated shard
         would serve stale before refresh completes. *)
      let trigger_refresh () =
        Eio.Condition.broadcast refresh_signal;
        match cfg.connection.client_cache with
        | None -> ()
        | Some ccfg -> Cache.clear ccfg.Client_cache.cache
      in
      let sync_ref () = topology_ref := Atomic.get topology_atomic in
      let exec ?timeout target rf args =
        make_exec ~pool ~topology_ref ~clock
          ~max_redirects:cfg.max_redirects ~trigger_refresh
          ~sync_ref ?timeout target rf args
      in
      let exec_multi ?timeout fan args =
        sync_ref ();
        make_exec_multi ~pool ~topology_ref ?timeout fan args
      in
      let pair ?timeout target args1 args2 =
        make_pair ~pool ~topology_ref ~clock
          ~max_redirects:cfg.max_redirects ~trigger_refresh
          ~sync_ref ?timeout target args1 args2
      in
      let close () =
        if Atomic.exchange closing true then ()
        else begin
          Eio.Condition.broadcast refresh_signal;
          Node_pool.close_all pool
        end
      in
      let primary () =
        match Node_pool.all_connections pool with
        | [] -> None
        | c :: _ -> Some c
      in
      let connection_for_slot slot =
        sync_ref ();
        connection_for_slot_via ~pool ~topology_ref slot
      in
      let endpoint_for_slot slot =
        sync_ref ();
        endpoint_for_slot_via ~topology_ref
          ~prefer_hostname:cfg.prefer_hostname
          ~connection_config:cfg.connection
          slot
      in
      let endpoint_for_node ~node_id =
        sync_ref ();
        endpoint_for_node_via ~topology_ref
          ~prefer_hostname:cfg.prefer_hostname
          ~connection_config:cfg.connection
          ~node_id
      in
      let for_primary = make_atomic_lock_table () in
      let atomic_lock_for_slot slot =
        sync_ref ();
        atomic_lock_for_slot_via ~topology_ref ~for_primary slot
      in
      Ok (Router.make ~exec ~exec_multi ~pair ~close ~primary
            ~connection_for_slot ~endpoint_for_slot ~endpoint_for_node
            ~is_standalone:false
            ~atomic_lock_for_slot)

module For_testing = struct
  let handle_retries = handle_retries
  let clusterdown_backoff_for_attempt = clusterdown_backoff_for_attempt
  let tryagain_backoff = tryagain_backoff
  let conn_lost_backoff = conn_lost_backoff
end
