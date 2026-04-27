module Error = Connection_error

module Circuit_breaker : sig
  module Config : sig
    type t = {
      window_size : float;
      error_threshold : int;
      open_timeout : float;
    }
    val default : t
    (** Default: 10000 errors in 60s trips; 10s before half-open probe. *)
  end
end

module Handshake : sig
  type t = {
    protocol : int;
    auth : (string * string) option;
    client_name : string option;
    select_db : int option;
  }
  val default : t
end

module Reconnect : sig
  type t = {
    initial_backoff : float;
    max_backoff : float;
    jitter : float;
    max_attempts : int option;
    max_total : float option;
    handshake_timeout : float;
  }
  val default : t
end

module Config : sig
  type t = {
    handshake : Handshake.t;
    reconnect : Reconnect.t;
    command_timeout : float option;
    keepalive_interval : float option;
    push_buffer_size : int;
    max_queued_bytes : int;
    tls : Tls_config.t option;
    circuit_breaker : Circuit_breaker.Config.t option;
    client_cache : Client_cache.t option;
    (** Client-side caching (Phase 8). When [Some], [CLIENT
        TRACKING ON ...] is issued after every (re)connect per
        [mode]/[noloop]. When [None], tracking is not enabled
        and the cache is unused. *)
  }
  val default : t
end

type t

type state =
  | Connecting
  | Alive
  | Recovering
  | Dead of Error.t

exception Handshake_failed of Error.t

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?config:Config.t ->
  ?on_connected:(unit -> unit) ->
  host:string ->
  port:int ->
  unit ->
  t
(** If [domain_mgr] is supplied, the supervisor + reader + writer + keepalive
    fibers run on a dedicated OS thread (Domain). Parser CPU cost no longer
    competes with user-fiber scheduling on the main domain.

    [on_connected] is invoked after every successful handshake —
    both the initial connect and every reconnect after a recovery
    loop. Used by [Pubsub] to replay the subscription set. Runs
    synchronously on the state-machine thread; keep it cheap
    (bounded number of [send_fire_and_forget] calls). *)

val request :
  ?timeout:float ->
  t ->
  string array ->
  (Resp3.t, Error.t) result

val request_pair :
  ?timeout:float ->
  t ->
  string array ->
  string array ->
  ( (Resp3.t, Error.t) result * (Resp3.t, Error.t) result
  , Error.t) result
(** Pipelined two-frame submit through the matching FIFO. Wires
    are enqueued as a single indivisible unit and flushed
    back-to-back via scatter-gather, so no other fiber's command
    can land between them on the wire. Internal: used by the
    OPTIN client-side-caching read path (where the spec
    requires [CLIENT CACHING YES] to be sent immediately before
    the tracked read).

    The outer [Error.t] covers failures that prevented the
    submit (Closed, Circuit_open, Queue_full). The inner pair
    carries the two frames' independent replies; either may
    be a [Server_error] without the other failing.

    [timeout] applies as a single wall-clock deadline to the
    whole pair. *)

val request_triple :
  ?timeout:float ->
  t ->
  string array ->
  string array ->
  string array ->
  ( (Resp3.t, Error.t) result
    * (Resp3.t, Error.t) result
    * (Resp3.t, Error.t) result
  , Error.t) result
(** Pipelined three-frame variant of {!request_pair}. Internal:
    used to recover OPTIN reads from an ASK redirect, where the
    server-side migration window requires
    [CLIENT CACHING YES + ASKING + <read>] as three
    wire-adjacent frames. [ASKING] is one-shot — the server
    consumes the flag from the next command regardless of what
    it is — so it must sit immediately before the slot-keyed
    read. [CACHING YES] arms tracking for that read, and the
    read carries the actual key. Same error semantics as
    {!request_pair}. *)

val send_fire_and_forget :
  t ->
  string array ->
  (unit, Error.t) result
(** Write a command to the socket without waiting for a reply. For
    commands that have no synchronous reply — notably [SUBSCRIBE] /
    [UNSUBSCRIBE] / [PSUBSCRIBE] / [SSUBSCRIBE] and friends — whose
    acknowledgements arrive as Push frames on {!pushes}.

    Do NOT mix [send_fire_and_forget] with [request] on the same
    connection: a subsequent non-push reply would match no pending
    request and corrupt the matching queue. Use a dedicated
    connection (via [Pubsub]) for subscribe-mode traffic. *)

val pushes : t -> Resp3.t Eio.Stream.t

val invalidations : t -> Invalidation.t Eio.Stream.t
(** CSC invalidation pushes routed out of the general [pushes]
    stream. The invalidator fiber (spawned automatically when
    [Config.t.client_cache] is [Some _]) drains this; applications
    shouldn't need to touch it. Exposed for tests and for users
    wiring their own bespoke cache. *)

val availability_zone : t -> string option

val server_info : t -> Resp3.t option

val state : t -> state

val keepalive_count : t -> int
(** Number of successful keepalive PINGs since [connect]. For observability
    and tests; zero if [keepalive_interval] is None. *)

val interrupt : t -> unit
(** Force-close the current transport without marking the connection
    permanently closed. If the owning switch is still alive, the
    supervisor treats this like a disconnect and runs the normal
    reconnect path. Used by higher layers when a timed-out
    MULTI/EXEC-like sequence leaves server-side connection state
    potentially tainted. *)

val close : t -> unit
