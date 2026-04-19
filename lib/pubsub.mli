(** Pub/sub subscriber.

    Pub/sub is per-connection state on the Valkey side: after a
    [SUBSCRIBE], the connection enters "subscribe mode" and its
    replies arrive as RESP3 Push frames instead of normal replies.
    That is incompatible with our multiplexed [Client.t] (which
    assumes request/reply ordering on a shared connection), so a
    [Pubsub.t] owns its own dedicated socket.

    Produce side (PUBLISH, SPUBLISH) is still a regular typed
    command and lives on [Client] — see {!Client.publish} and
    {!Client.spublish}. *)

type t

(** Incoming delivery. [Channel] is a plain [SUBSCRIBE] match,
    [Pattern] is a [PSUBSCRIBE] match, [Shard] is an [SSUBSCRIBE]
    match. *)
type message =
  | Channel of { channel : string; payload : string }
  | Pattern of { pattern : string; channel : string; payload : string }
  | Shard of { channel : string; payload : string }

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?config:Connection.Config.t ->
  host:string ->
  port:int ->
  unit ->
  t
(** Open a dedicated pub/sub connection. *)

val close : t -> unit

val subscribe : t -> string list -> (unit, Connection.Error.t) result
(** [SUBSCRIBE c1 c2 ...]. Returns [Ok ()] once the command is
    written; confirmation pushes ([subscribe, channel, count]) are
    consumed silently by [next_message]. *)

val unsubscribe : t -> string list -> (unit, Connection.Error.t) result
(** [UNSUBSCRIBE c1 c2 ...]. Empty list means unsubscribe from all. *)

val psubscribe : t -> string list -> (unit, Connection.Error.t) result
(** [PSUBSCRIBE p1 p2 ...]. Glob patterns per Valkey syntax. *)

val punsubscribe : t -> string list -> (unit, Connection.Error.t) result

val ssubscribe : t -> string list -> (unit, Connection.Error.t) result
(** [SSUBSCRIBE c1 c2 ...] — sharded pub/sub. In cluster mode, all
    channels must hash to the slot owned by the node this
    [Pubsub.t] is connected to; the server rejects cross-slot
    sharded subscriptions with [CROSSSLOT]. *)

val sunsubscribe : t -> string list -> (unit, Connection.Error.t) result

val next_message :
  ?timeout:float -> t -> (message, [ `Timeout ]) result
(** Block until a message arrives. With [?timeout] returns
    [Error `Timeout] when nothing arrived in the window. Filters out
    subscription-ack pushes silently so the caller only sees real
    deliveries. *)

(** {1 Low-level helpers — exposed for [Cluster_pubsub]} *)

val classify_push : Resp3.t -> [ `Ack | `Deliver of message | `Other ]
(** Inspect a RESP3 Push frame and classify it as a subscription
    acknowledgement, a real delivery, or something unrecognised. *)
