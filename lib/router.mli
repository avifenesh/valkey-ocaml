(** Abstract routing dispatcher. Both the standalone and cluster modes
    produce a [Router.t]; command layers (Client) use it uniformly. *)

module Read_from : sig
  type t =
    | Primary
    | Prefer_replica
    | Az_affinity of { az : string }
    | Az_affinity_replicas_and_primary of { az : string }

  val default : t
end

module Target : sig
  (** Where to send a single-reply command.

      Fan-out (every primary / every node) is a separate concern: it
      returns one reply per node, not one reply total. See [Fan_target]
      and [exec_multi]. *)
  type t =
    | Random
    | By_slot of int
    | By_node of string
    | By_channel of string
end

module Fan_target : sig
  (** Which subset of the cluster to fan a command out to. *)
  type t =
    | All_nodes
    | All_primaries
    | All_replicas
end

type t

type exec_fn =
  ?timeout:float -> Target.t -> Read_from.t -> string array ->
  (Resp3.t, Connection.Error.t) result

type exec_multi_fn =
  ?timeout:float -> Fan_target.t -> string array ->
  (string * (Resp3.t, Connection.Error.t) result) list

type pair_fn =
  ?timeout:float -> Target.t ->
  string array -> string array ->
  ( (Resp3.t, Connection.Error.t) result
    * (Resp3.t, Connection.Error.t) result
  , Connection.Error.t) result
(** Pipelined two-frame submit. Frame 1 arms (e.g. [CLIENT
    CACHING YES] for OPTIN CSC); frame 2 is the keyed read.
    Both frames go to the same connection in one indivisible
    enqueue. On MOVED, the whole pair is re-submitted on the
    new owner so frame 1 stays adjacent to frame 2.

    [Read_from] is not a parameter: this primitive only exists
    for OPTIN, where replicas don't run [CLIENT TRACKING] —
    routing always pins to the slot's primary. *)

type connection_for_slot_fn =
  int -> Connection.t option
(** Resolve the live [Connection.t] for the primary that owns the
    given slot. Used by [Transaction] to pin a MULTI/EXEC block to a
    specific node; returns [None] if the slot has no live connection
    in the pool (topology out of date or node unreachable). *)

type endpoint_for_slot_fn =
  int -> (string * string * int) option
(** Resolve the {i address} (primary_id, host, port) of the primary
    that owns the given slot. Used by [Cluster_pubsub] to open a
    dedicated subscriber socket to the right node — a pool-backed
    connection is multiplexed and cannot be switched into subscribe
    mode. Returns [None] if the slot is unowned in the current
    topology. *)

type endpoint_for_node_fn =
  node_id:string -> (string * int) option
(** Resolve [(host, port)] for a live [node_id] in the current
    topology. Used by [Blocking_pool] to open dedicated conns for
    blocking commands against the slot's primary — the pool is
    keyed by node_id, so the lookup is by-id rather than by-slot.
    Returns [None] if the node is not in the current topology
    (drained / unknown id). *)

val make :
  exec:exec_fn ->
  exec_multi:exec_multi_fn ->
  pair:pair_fn ->
  close:(unit -> unit) ->
  primary:(unit -> Connection.t option) ->
  connection_for_slot:connection_for_slot_fn ->
  endpoint_for_slot:endpoint_for_slot_fn ->
  endpoint_for_node:endpoint_for_node_fn ->
  is_standalone:bool ->
  atomic_lock_for_slot:(int -> Eio.Mutex.t) ->
  t

val standalone : Connection.t -> t

val exec :
  ?timeout:float -> t -> Target.t -> Read_from.t -> string array ->
  (Resp3.t, Connection.Error.t) result

val exec_multi :
  ?timeout:float -> t -> Fan_target.t -> string array ->
  (string * (Resp3.t, Connection.Error.t) result) list

val pair :
  ?timeout:float -> t -> Target.t ->
  string array -> string array ->
  ( (Resp3.t, Connection.Error.t) result
    * (Resp3.t, Connection.Error.t) result
  , Connection.Error.t) result
(** Send [args] to every node in the fan-out set in parallel.
    Returns one [(node_id, result)] pair per node, in unspecified order.

    On a standalone router (cluster-of-one), this returns a single-entry
    list for all fan targets — the one node is both primary and full set. *)

val close : t -> unit

val primary_connection : t -> Connection.t option

val connection_for_slot : t -> int -> Connection.t option
(** Returns the live [Connection.t] whose primary owns [slot], or
    [None] if none is in the pool. On standalone this always
    returns the single connection. *)

val endpoint_for_slot : t -> int -> (string * string * int) option
(** Returns [Some (primary_id, host, port)] of the current primary
    for [slot], or [None] if the slot is unowned in the router's
    topology. *)

val endpoint_for_node : t -> node_id:string -> (string * int) option
(** Returns [Some (host, port)] for [node_id] if that id is live in
    the current topology, or [None] otherwise (drained / unknown).
    Consumed by {!Blocking_pool}. *)

val is_standalone : t -> bool
(** [true] only for the synthetic standalone/single-node path
    created by {!standalone} (and wrappers built from it). *)

val atomic_lock_for_slot : t -> int -> Eio.Mutex.t
(** Mutex to acquire for the duration of an atomic operation
    (MULTI/EXEC block) on the primary that owns [slot]. Serialises
    concurrent atomic batches and transactions on the same shared
    connection, preventing protocol corruption from interleaved
    MULTI/EXEC state.

    Non-atomic commands do not need to acquire this lock — they
    pipeline through the same connection without taking per-op
    state. In a cluster, slots that map to different primaries
    return different mutexes so their atomic ops run in parallel;
    slots on the same primary share one mutex and serialise. *)
