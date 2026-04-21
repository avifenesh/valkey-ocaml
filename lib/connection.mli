module Error : sig
  type t =
    | Tcp_refused of string
    | Dns_failed of string
    | Tls_failed of string
    | Handshake_rejected of Valkey_error.t
    | Auth_failed of Valkey_error.t
    | Protocol_violation of string
    | Timeout
    | Interrupted
    | Queue_full
    | Circuit_open
    | Closed
    | Server_error of Valkey_error.t
    | Terminal of string

  val equal : t -> t -> bool
  val pp : Format.formatter -> t -> unit
  val is_terminal : t -> bool
end

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
