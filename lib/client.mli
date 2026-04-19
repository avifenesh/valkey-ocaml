(** High-level Valkey client. Standalone in v1; cluster router lands later
    without changing this surface. *)

module Read_from : sig
  type t =
    | Primary
    | Prefer_replica
    | Az_affinity of { az : string }
    | Az_affinity_replicas_and_primary of { az : string }

  val default : t
  (** [Primary] — all reads to primary. *)
end

module Target : sig
  type t =
    | Random
    | All_nodes
    | All_primaries
    | By_slot of int
    | By_node of string
    | By_channel of string
end

module Config : sig
  type t = {
    connection : Connection.Config.t;
    client_az : string option;
    read_from : Read_from.t;
  }
  val default : t
end

type t

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?config:Config.t ->
  host:string ->
  port:int ->
  unit ->
  t

val close : t -> unit

val raw_connection : t -> Connection.t

val exec :
  ?timeout:float ->
  ?target:Target.t ->
  ?read_from:Read_from.t ->
  t ->
  string array ->
  (Resp3.t, Connection.Error.t) result

(** {1 Strings, counters, TTL} *)

val get :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (string option, Connection.Error.t) result

val set :
  ?timeout:float ->
  ?ex_seconds:float ->
  ?nx:bool ->
  ?xx:bool ->
  ?ifeq:string ->
  t -> string -> string -> (bool, Connection.Error.t) result
(** [true] on success; [false] when NX/XX/IFEQ prevented the write. *)

val set_and_get :
  ?timeout:float ->
  ?ex_seconds:float ->
  ?nx:bool ->
  ?xx:bool ->
  ?ifeq:string ->
  t -> string -> string -> (string option, Connection.Error.t) result
(** SET ... GET variant. Returns the old value (None if key didn't exist). *)

val del :
  ?timeout:float ->
  t -> string list -> (int, Connection.Error.t) result

val unlink :
  ?timeout:float ->
  t -> string list -> (int, Connection.Error.t) result
(** Async-delete; returns count of keys removed. *)

val delifeq :
  ?timeout:float ->
  t -> string -> value:string -> (bool, Connection.Error.t) result
(** Valkey 9+. Deletes key only if its current value equals [value]. *)

val setex :
  ?timeout:float ->
  t -> string -> seconds:int -> string -> (unit, Connection.Error.t) result

val setnx :
  ?timeout:float ->
  t -> string -> string -> (bool, Connection.Error.t) result

val mget :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string list -> (string option list, Connection.Error.t) result

type value_type =
  | T_none
  | T_string
  | T_list
  | T_hash
  | T_set
  | T_zset
  | T_stream
  | T_other of string

val type_of :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (value_type, Connection.Error.t) result

type expiry_state =
  | Absent
      (** Key does not exist. *)
  | Persistent
      (** Key exists but has no TTL. *)
  | Expires_in of int
      (** Remaining time. Unit depends on which function you called:
          seconds for [ttl], milliseconds for [pttl]. *)

val ttl :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (expiry_state, Connection.Error.t) result

val pttl :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (expiry_state, Connection.Error.t) result

val expire :
  ?timeout:float ->
  t -> string -> seconds:int -> (bool, Connection.Error.t) result

val pexpire :
  ?timeout:float ->
  t -> string -> millis:int -> (bool, Connection.Error.t) result

val pexpireat :
  ?timeout:float ->
  t -> string -> at_unix_millis:int -> (bool, Connection.Error.t) result

val incr :
  ?timeout:float ->
  t -> string -> (int64, Connection.Error.t) result

val incrby :
  ?timeout:float ->
  t -> string -> int -> (int64, Connection.Error.t) result
