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

type set_cond = Set_nx | Set_xx
(** Mutually exclusive existence conditions on SET. *)

type set_ttl =
  | Set_ex_seconds of int
  | Set_px_millis of int
  | Set_exat_unix_seconds of int
  | Set_pxat_unix_millis of int
  | Set_keepttl
(** Mutually exclusive TTL modifiers on SET. *)

val set :
  ?timeout:float ->
  ?cond:set_cond ->
  ?ttl:set_ttl ->
  ?ifeq:string ->
  t -> string -> string -> (bool, Connection.Error.t) result
(** [true] on success; [false] when NX/XX/IFEQ prevented the write. *)

val set_and_get :
  ?timeout:float ->
  ?cond:set_cond ->
  ?ttl:set_ttl ->
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

(** {1 Hashes} *)

val hget :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string -> (string option, Connection.Error.t) result

val hset :
  ?timeout:float ->
  t -> string -> (string * string) list -> (int, Connection.Error.t) result
(** Returns the number of fields that were newly added (not updated). *)

val hmget :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list -> (string option list, Connection.Error.t) result

val hgetall :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> ((string * string) list, Connection.Error.t) result

val hincrby :
  ?timeout:float ->
  t -> string -> string -> int -> (int64, Connection.Error.t) result

(** {1 Hash field TTL (Valkey 9+)} *)

type hexpire_cond = H_nx | H_xx | H_gt | H_lt

type field_ttl_set =
  | Hfield_missing          (** -2: no such field / no such key *)
  | Hfield_condition_failed (** 0: [cond] modifier prevented the write *)
  | Hfield_ttl_set          (** 1: expiration applied *)
  | Hfield_expired_now      (** 2: zero-second TTL caused immediate deletion *)

val hexpire :
  ?timeout:float ->
  ?cond:hexpire_cond ->
  t -> string -> seconds:int -> string list ->
  (field_ttl_set list, Connection.Error.t) result

val hpexpire :
  ?timeout:float ->
  ?cond:hexpire_cond ->
  t -> string -> millis:int -> string list ->
  (field_ttl_set list, Connection.Error.t) result

val hexpireat :
  ?timeout:float ->
  ?cond:hexpire_cond ->
  t -> string -> at_unix_seconds:int -> string list ->
  (field_ttl_set list, Connection.Error.t) result

val hpexpireat :
  ?timeout:float ->
  ?cond:hexpire_cond ->
  t -> string -> at_unix_millis:int -> string list ->
  (field_ttl_set list, Connection.Error.t) result

val httl :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list ->
  (expiry_state list, Connection.Error.t) result

val hpttl :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list ->
  (expiry_state list, Connection.Error.t) result

val hexpiretime :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list ->
  (expiry_state list, Connection.Error.t) result
(** Replies are absolute Unix-seconds (when field expires), not remaining.
    Uses [Expires_in] constructor with absolute-time interpretation. *)

val hpexpiretime :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list ->
  (expiry_state list, Connection.Error.t) result

type field_persist =
  | Persist_field_missing   (** -2 *)
  | Persist_had_no_ttl      (** -1 *)
  | Persist_ttl_removed     (** 1 *)

val hpersist :
  ?timeout:float ->
  t -> string -> string list ->
  (field_persist list, Connection.Error.t) result

(** HGETEX simplified: one TTL modifier at a time. Pass [None] for
    plain get. *)
type hgetex_ttl =
  | Hge_no_change
  | Hge_ex_seconds of int
  | Hge_px_millis of int
  | Hge_exat_unix_seconds of int
  | Hge_pxat_unix_millis of int
  | Hge_persist

val hgetex :
  ?timeout:float ->
  t -> string -> ttl:hgetex_ttl -> string list ->
  (string option list, Connection.Error.t) result

type hsetex_ttl =
  | Hse_ex_seconds of int
  | Hse_px_millis of int
  | Hse_exat_unix_seconds of int
  | Hse_pxat_unix_millis of int
  | Hse_keepttl

val hsetex :
  ?timeout:float ->
  ?ttl:hsetex_ttl ->
  t -> string -> (string * string) list ->
  (bool, Connection.Error.t) result
(** Returns [true] on success, [false] if an [FNX]/[FXX] condition blocked
    the write. Omitting [ttl] writes without TTL. *)

(** {1 Sets} *)

val sadd :
  ?timeout:float ->
  t -> string -> string list -> (int, Connection.Error.t) result
(** Returns the number of elements newly added (not counting those that
    already existed). *)

val scard :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (int, Connection.Error.t) result

val smembers :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (string list, Connection.Error.t) result

val sismember :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string -> (bool, Connection.Error.t) result
