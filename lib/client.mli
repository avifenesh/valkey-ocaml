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

module Router : sig
  type t

  val make :
    exec:(?timeout:float -> Target.t -> Read_from.t -> string array ->
          (Resp3.t, Connection.Error.t) result) ->
    close:(unit -> unit) ->
    primary:(unit -> Connection.t option) ->
    t

  val standalone : Connection.t -> t

  val exec :
    ?timeout:float -> t -> Target.t -> Read_from.t -> string array ->
    (Resp3.t, Connection.Error.t) result

  val close : t -> unit

  val primary_connection : t -> Connection.t option
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

val from_router : config:Config.t -> Router.t -> t
(** Wrap an arbitrary [Router.t] (e.g. a cluster router) as a [Client.t].
    Used by [Cluster_router] and other custom routers. *)

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

(** {1 Lists} *)

val lpush :
  ?timeout:float ->
  t -> string -> string list -> (int, Connection.Error.t) result
(** Returns the new list length. *)

val rpush :
  ?timeout:float ->
  t -> string -> string list -> (int, Connection.Error.t) result

val lpop :
  ?timeout:float ->
  t -> string -> (string option, Connection.Error.t) result

val lpop_n :
  ?timeout:float ->
  t -> string -> int -> (string list, Connection.Error.t) result

val rpop :
  ?timeout:float ->
  t -> string -> (string option, Connection.Error.t) result

val rpop_n :
  ?timeout:float ->
  t -> string -> int -> (string list, Connection.Error.t) result

val lrange :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> start:int -> stop:int ->
  (string list, Connection.Error.t) result

val llen :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (int, Connection.Error.t) result

(** {1 Sorted sets} *)

type score_bound =
  | Score of float
      (** Inclusive. *)
  | Score_excl of float
      (** Exclusive ("(5" on the wire). *)
  | Score_neg_inf
  | Score_pos_inf

val zrange :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?rev:bool ->
  t -> string -> start:int -> stop:int ->
  (string list, Connection.Error.t) result

val zrangebyscore :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?limit:(int * int) ->
  t -> string -> min:score_bound -> max:score_bound ->
  (string list, Connection.Error.t) result

val zremrangebyscore :
  ?timeout:float ->
  t -> string -> min:score_bound -> max:score_bound ->
  (int, Connection.Error.t) result

val zcard :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (int, Connection.Error.t) result

(** {1 Scripting (Lua)} *)

val eval :
  ?timeout:float ->
  t -> script:string -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result
(** Returns the raw RESP3 reply — Lua scripts can return any type. *)

val evalsha :
  ?timeout:float ->
  t -> sha:string -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result

val script_load :
  ?timeout:float ->
  t -> string -> (string, Connection.Error.t) result
(** Returns the SHA1 of the loaded script. *)

val script_exists :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string list -> (bool list, Connection.Error.t) result

type script_flush_mode = Flush_sync | Flush_async

val script_flush :
  ?timeout:float ->
  ?mode:script_flush_mode ->
  t -> (unit, Connection.Error.t) result

module Script : sig
  type t
  val create : string -> t
  (** Pre-computes the script's SHA1 at construction so subsequent
      executions can skip the hash. *)

  val source : t -> string
  val sha : t -> string
end

val eval_script :
  ?timeout:float ->
  t -> Script.t -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result
(** Optimistic EVALSHA; on the first ever use (or after server-side
    script flush), transparently falls back to EVAL. The client tracks
    which SHAs this server is known to have loaded so callers don't
    juggle NOSCRIPT handling. *)

val eval_cached :
  ?timeout:float ->
  t -> script:string -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result
(** Convenience wrapper over [eval_script]. Wraps [script] in a
    fresh [Script.t] each call; prefer [eval_script] when running the
    same script many times (saves repeated SHA1 computation). *)

(** {1 Iteration} *)

type scan_page = {
  cursor : string;
    (** ["0"] when iteration is complete; any other value is the next
        cursor to pass back. *)
  keys : string list;
}

val scan :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?match_:string ->
  ?count:int ->
  ?type_:string ->
  t -> cursor:string -> (scan_page, Connection.Error.t) result

val keys :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (string list, Connection.Error.t) result
(** [KEYS pattern]. Use [scan] in production; [KEYS] is O(N) and blocks
    the server. *)

(** {1 Streams (non-blocking)}

    Blocking variants (XREAD BLOCK, XREADGROUP BLOCK) will ship with the
    blocking-commands session since they share the exclusive-connection
    machinery. *)

type stream_entry = {
  id : string;
  fields : (string * string) list;
}

type xadd_trim =
  | Xadd_maxlen of { approx : bool; threshold : int }
  | Xadd_minid of { approx : bool; threshold : string }

val xadd :
  ?timeout:float ->
  ?id:string ->
  ?nomkstream:bool ->
  ?trim:xadd_trim ->
  t -> string -> (string * string) list ->
  (string, Connection.Error.t) result
(** Returns the assigned entry ID. [id] defaults to ["*"] — server-assigned. *)

val xlen :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (int, Connection.Error.t) result

val xrange :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?count:int ->
  t -> string -> start:string -> end_:string ->
  (stream_entry list, Connection.Error.t) result

val xrevrange :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?count:int ->
  t -> string -> end_:string -> start:string ->
  (stream_entry list, Connection.Error.t) result

val xdel :
  ?timeout:float ->
  t -> string -> string list -> (int, Connection.Error.t) result

type xtrim_strategy =
  | Xtrim_maxlen of { approx : bool; threshold : int }
  | Xtrim_minid of { approx : bool; threshold : string }

val xtrim :
  ?timeout:float ->
  t -> string -> xtrim_strategy -> (int, Connection.Error.t) result

val xread :
  ?timeout:float ->
  ?count:int ->
  t -> streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result

type xgroup_create_option =
  | Xgroup_mkstream
  | Xgroup_entries_read of int

val xgroup_create :
  ?timeout:float ->
  ?opts:xgroup_create_option list ->
  t -> string -> group:string -> id:string ->
  (unit, Connection.Error.t) result

val xgroup_destroy :
  ?timeout:float ->
  t -> string -> group:string -> (bool, Connection.Error.t) result

val xreadgroup :
  ?timeout:float ->
  ?count:int ->
  ?noack:bool ->
  t -> group:string -> consumer:string ->
  streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result

val xack :
  ?timeout:float ->
  t -> string -> group:string -> string list ->
  (int, Connection.Error.t) result

(** {2 Stream admin (XPENDING / XCLAIM / XAUTOCLAIM / XINFO)} *)

type xpending_summary = {
  count : int;
  min_id : string option;       (** [None] iff [count] is 0. *)
  max_id : string option;
  consumers : (string * int) list;  (** consumer -> pending count *)
}

val xpending :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> group:string ->
  (xpending_summary, Connection.Error.t) result

type xpending_entry = {
  id : string;
  consumer : string;
  idle_ms : int;
  delivery_count : int;
}

val xpending_range :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?idle_ms:int ->
  ?consumer:string ->
  t -> string -> group:string ->
  start:string -> end_:string -> count:int ->
  (xpending_entry list, Connection.Error.t) result

val xclaim :
  ?timeout:float ->
  ?idle_ms:int ->
  ?time_unix_ms:int ->
  ?retry_count:int ->
  ?force:bool ->
  t -> string -> group:string -> consumer:string ->
  min_idle_ms:int -> ids:string list ->
  (stream_entry list, Connection.Error.t) result

val xclaim_ids :
  ?timeout:float ->
  ?idle_ms:int ->
  ?time_unix_ms:int ->
  ?retry_count:int ->
  ?force:bool ->
  t -> string -> group:string -> consumer:string ->
  min_idle_ms:int -> ids:string list ->
  (string list, Connection.Error.t) result
(** XCLAIM with JUSTID — returns only the IDs of claimed entries. *)

type xautoclaim_result = {
  next_cursor : string;           (** ["0-0"] signals end of scan. *)
  claimed : stream_entry list;
  deleted_ids : string list;
}

val xautoclaim :
  ?timeout:float ->
  ?count:int ->
  t -> string -> group:string -> consumer:string ->
  min_idle_ms:int -> cursor:string ->
  (xautoclaim_result, Connection.Error.t) result

type xautoclaim_ids_result = {
  next_cursor : string;
  claimed_ids : string list;
  deleted_ids : string list;
}

val xautoclaim_ids :
  ?timeout:float ->
  ?count:int ->
  t -> string -> group:string -> consumer:string ->
  min_idle_ms:int -> cursor:string ->
  (xautoclaim_ids_result, Connection.Error.t) result

val xinfo_stream :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (Resp3.t, Connection.Error.t) result
(** Returns the raw reply — shape is rich, varies by server version, and
    includes nested stream entries. Users decode what they need. *)

val xinfo_groups :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (Resp3.t list, Connection.Error.t) result

val xinfo_consumers :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> group:string -> (Resp3.t list, Connection.Error.t) result

(** {1 Blocking commands}

    Open a DEDICATED [Client.t] for these. Sending a blocking command
    on the Client you use for normal traffic stalls every other fiber
    sharing that socket until the block resolves.

    Our multiplexed design does not auto-switch connections for blocking
    calls. Typed commands here are safe to call only on a Client scoped
    to blocking use. *)

type list_side = Left | Right

val lmove :
  ?timeout:float ->
  t -> source:string -> destination:string ->
  from:list_side -> to_:list_side -> (string option, Connection.Error.t) result
(** Atomic non-blocking LMOVE. Included alongside [blmove] since they
    share [list_side]. *)

val blpop :
  ?timeout:float ->
  t -> keys:string list -> block_seconds:float ->
  ((string * string) option, Connection.Error.t) result
(** [Some (key, value)] or [None] on server-side timeout.
    [block_seconds = 0.] blocks indefinitely — in that case set
    [?timeout] or cancel the Client's switch to get out. *)

val brpop :
  ?timeout:float ->
  t -> keys:string list -> block_seconds:float ->
  ((string * string) option, Connection.Error.t) result

val blmove :
  ?timeout:float ->
  t -> source:string -> destination:string ->
  from:list_side -> to_:list_side -> block_seconds:float ->
  (string option, Connection.Error.t) result

val wait_replicas :
  ?timeout:float ->
  t -> num_replicas:int -> block_ms:int ->
  (int, Connection.Error.t) result
(** WAIT. Returns number of replicas that acknowledged.
    [block_ms = 0] blocks indefinitely. *)

val xread_block :
  ?timeout:float ->
  ?count:int ->
  t -> block_ms:int ->
  streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result
(** Blocking XREAD. On server-side timeout returns an empty list. *)

val xreadgroup_block :
  ?timeout:float ->
  ?count:int ->
  ?noack:bool ->
  t -> block_ms:int -> group:string -> consumer:string ->
  streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result
