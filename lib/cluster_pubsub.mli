(** Cluster-aware pub/sub subscriber.

    Regular [SUBSCRIBE] / [PSUBSCRIBE]: one connection, broadcast
    cluster-wide (Valkey 7+ propagates messages across shards).

    Sharded [SSUBSCRIBE]: per-slot. Each channel is pinned to the
    primary that owns its slot; this subscriber opens a dedicated
    connection per distinct slot it is subscribed on, and rebinds
    automatically on primary failover (topology shift).

    Deliveries from all connections are merged into a single
    ordered stream surfaced via [next_message].

    {1 When to use}

    - Use {!Pubsub} in standalone mode or when you only need
      cluster-wide (non-sharded) pub/sub against a single node.
    - Use [Cluster_pubsub] when the app uses [SSUBSCRIBE] in cluster
      mode, or when you want one API that handles both regular and
      sharded subscriptions. *)

type t

val create :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?connection_config:Connection.Config.t ->
  router:Router.t ->
  unit ->
  t
(** Build a cluster pub/sub handle on top of an existing
    [Router.t] (typically the one returned by
    [Cluster_router.create]). [router] is used for topology lookups
    only; no pool connections are reused for subscribe-mode traffic.

    An initial connection is opened eagerly to a primary picked by
    the router so non-sharded subscribes (SUBSCRIBE / PSUBSCRIBE)
    have a home right away. Sharded connections are opened lazily
    per slot on first [ssubscribe]. *)

val close : t -> unit
(** Close every open subscriber connection and stop the topology
    watchdog. Outstanding [next_message] calls return [`Closed]. *)

val subscribe : t -> string list -> (unit, Connection.Error.t) result
val unsubscribe : t -> string list -> (unit, Connection.Error.t) result
val psubscribe : t -> string list -> (unit, Connection.Error.t) result
val punsubscribe : t -> string list -> (unit, Connection.Error.t) result

val ssubscribe : t -> string list -> (unit, Connection.Error.t) result
(** Sharded [SSUBSCRIBE c1 c2 ...]. Channels are grouped by slot;
    one [SSUBSCRIBE] request is issued per slot on a dedicated
    connection to that slot's primary. Opens new slot connections
    as needed. *)

val sunsubscribe : t -> string list -> (unit, Connection.Error.t) result
(** Remove from the sharded subscription set. *)

val next_message :
  ?timeout:float ->
  t ->
  (Pubsub.message, [ `Timeout | `Closed ]) result
(** Block for one delivery from any of the subscriber's open
    connections. Subscription ACKs are filtered silently. *)
