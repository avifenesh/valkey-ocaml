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
    | Closed
    | Server_error of Valkey_error.t
    | Terminal of string

  val equal : t -> t -> bool
  val pp : Format.formatter -> t -> unit
  val is_terminal : t -> bool
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
  ?config:Config.t ->
  host:string ->
  port:int ->
  unit ->
  t

val request :
  ?timeout:float ->
  t ->
  string array ->
  (Resp3.t, Error.t) result

val pushes : t -> Resp3.t Eio.Stream.t

val availability_zone : t -> string option

val server_info : t -> Resp3.t option

val state : t -> state

val keepalive_count : t -> int
(** Number of successful keepalive PINGs since [connect]. For observability
    and tests; zero if [keepalive_interval] is None. *)

val close : t -> unit
