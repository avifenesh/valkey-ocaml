(** Narrow per-node lease pool for Valkey's blocking commands:
    [BLPOP], [BRPOP], [BLMPOP], [BLMOVE], [BZPOPMIN] / [BZPOPMAX],
    [XREAD BLOCK], [XREADGROUP BLOCK], [WAIT], [WAITAOF].

    A multiplexed connection cannot serve a blocking command
    without freezing the matching FIFO for every other fiber on
    that socket. This pool sidesteps that by leasing an
    exclusive connection for the duration of each blocking
    call.

    {1 Deliberate scope constraints}

    Pool connections:

    - never enable [CLIENT TRACKING] (keeps the main
      multiplexed bundle's [CLIENT ID] count unchanged from
      the CSC cache's point of view),
    - never participate in [MULTI]/[EXEC] (transactions stay
      on the main multiplexed conn via per-slot atomic mutex),
    - never participate in OPTIN CSC pair submits or
      ASK-redirect pair/triple submits (those are single-frame
      from the pool's perspective anyway),
    - are leased for exactly one blocking call. On clean
      return the conn goes back to idle; on any exception or
      Eio cancellation the conn is closed (blocking-command
      wire state is opaque — we don't try to reset).

    {1 Sizing}

    Default [max_per_node = 0] disables the feature: blocking
    commands on a [Client.t] without a configured
    [Blocking_pool] surface [Pool_not_configured]. Users who
    call [BLPOP] set a positive [max_per_node] on
    [Client.Config.blocking_pool].

    The pool is keyed by [node_id] (same ids
    [Node_pool] uses). A blocking command for slot [s]
    resolves the slot's primary via the router's
    [endpoint_for_slot] and leases from that node's bucket.

    {1 Topology changes}

    Each borrow is tagged with the node's {i generation} at
    lease time. Topology diff bumps the generation; when a
    stale-generation conn is returned it is closed rather
    than re-idled. In-flight blocking calls are not
    cancelled — they run to completion (or their
    server-side timeout), but the conn is discarded on
    return.

    A node removed entirely from the topology is {i drained}:
    idle conns are closed immediately, new borrows refuse
    with [Node_gone]. *)

module Config : sig
  type t = {
    max_per_node : int;
    (** Upper bound on conns per node. [0] disables the pool
        entirely ([Pool_not_configured] on any borrow). *)

    min_idle_per_node : int;
    (** Pre-warm count. [0] is lazy-first-use. Pre-warming
        hides handshake latency at the cost of idle conns. *)

    borrow_timeout : float option;
    (** Wall-clock cap on waiting for an available conn when
        the pool is at [max_per_node] capacity.
        [None] waits forever. *)

    on_exhaustion : [ `Block | `Fail_fast ];
    (** [`Block] waits up to [borrow_timeout] for a free slot.
        [`Fail_fast] returns [Pool_exhausted] immediately. *)

    max_idle_age : float option;
    (** Close idle conns older than this many seconds. [None]
        keeps conns forever. *)
  }

  val default : t
  (** [max_per_node = 0], [min_idle_per_node = 0],
      [borrow_timeout = Some 5.0], [on_exhaustion = `Fail_fast],
      [max_idle_age = Some 300.0]. Feature disabled until the
      user opts in. *)
end

module Stats : sig
  type t = {
    in_use : int;
    idle : int;
    waiters : int;
    total_borrowed : int;
    total_created : int;
    total_closed_dirty : int;
    (** Conns closed because the lease scope raised, was
        cancelled, or the conn's generation went stale
        mid-lease. High rate signals either heavy
        cancellation or frequent topology churn. *)
    total_borrow_timeouts : int;
    total_exhaustion_rejects : int;
  }
end

type t

type borrow_error =
  | Pool_not_configured
  (** [max_per_node = 0] at [Client.connect] time. *)
  | Pool_exhausted
  (** [on_exhaustion = `Fail_fast] and all
      [max_per_node] conns are in use. *)
  | Borrow_timeout
  (** [on_exhaustion = `Block] but [borrow_timeout] expired
      before a conn became available. *)
  | Node_gone
  (** The [node_id] requested is no longer in the topology
      (drained). *)
  | Connect_failed of Connection.Error.t
  (** A fresh conn had to be opened (pool under capacity)
      but its handshake failed. *)

val pp_borrow_error : Format.formatter -> borrow_error -> unit

val create :
  sw:Eio.Switch.t ->
  net:[> [> `Generic | `Unix ] Eio.Net.ty ] Eio.Resource.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  config:Config.t ->
  connection_config:Connection.Config.t ->
  endpoint_for_node:(node_id:string -> (string * int) option) ->
  unit ->
  t
(** [connection_config] is used for fresh-conn handshakes.
    [connection_config.client_cache] must be [None] — pool
    conns never enable CLIENT TRACKING. [create] raises
    [Invalid_argument] otherwise.

    [endpoint_for_node] is supplied by the Router. It returns
    the current [(host, port)] for [node_id], or [None] if
    the node has been removed from topology. *)

val close : t -> unit
(** Close every idle conn and refuse new borrows. In-flight
    leases run to completion; the conn is closed on return. *)

val with_borrowed :
  t ->
  node_id:string ->
  (Connection.t -> ('a, Connection.Error.t) result) ->
  ('a, [ `Borrow of borrow_error | `Exec of Connection.Error.t ]) result
(** Lease one conn for [node_id], run [f] on it, return the
    conn. [f] owns the conn exclusively for its duration.

    - [Ok v] — [f] ran and returned [Ok v].
    - [Error (`Exec e)] — [f] ran but returned [Error e] from
      a command on the leased conn. Conn is closed (blocking
      command wire state is opaque — safer to discard than
      re-idle).
    - [Error (`Borrow err)] — the borrow itself failed;
      [f] was never called.

    On any exception raised inside [f] (including Eio
    cancellation), the conn is closed and the exception
    re-raised. Callers don't see this module's type system
    unless they catch explicitly. *)

val stats : t -> Stats.t
(** Aggregate counters across every node bucket. *)

val stats_by_node : t -> (string * Stats.t) list

val drain_node : t -> node_id:string -> unit
(** Mark [node_id] as removed. Idle conns close immediately;
    in-flight leases return [Error (`Borrow Node_gone)] on
    completion (their own command may still succeed but the
    conn is discarded rather than re-idled). New borrows for
    [node_id] return [Error (`Borrow Node_gone)].

    Called by the topology refresh fiber when a node
    disappears from [CLUSTER SHARDS]. *)

val refresh_node : t -> node_id:string -> unit
(** Bump the generation for [node_id] without draining. Used
    when the node is still in the topology but its role /
    endpoint changed (e.g. after a failover). In-flight
    leases complete their command, then the conn is closed
    on return rather than re-idled. *)
