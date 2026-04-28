(** High-level Valkey client. Standalone in v1; cluster router lands later
    without changing this surface. *)

(** Re-exports of routing types. The canonical home is [Valkey.Router]. *)

module Read_from = Router.Read_from
module Target = Router.Target

module Config : sig
  type t = {
    connection : Connection.Config.t;
    client_az : string option;
    read_from : Read_from.t;
    client_cache : Client_cache.t option;
    (** Enable client-side caching at the Client level. When set,
        [Client.connect] propagates this into the inner
        [Connection.Config] for every shard connection it creates
        (standalone and cluster), so all shards share a single
        [Cache.t] and agree on tracking mode.

        Setting both this field and [connection.client_cache]
        simultaneously is an error: [Client.connect] raises
        [Invalid_argument]. Users who want CSC should set this
        field; users who build raw [Connection.t] directly set
        [Connection.Config.client_cache] instead. *)

    connections_per_node : int;
    (** Number of multiplexed connections to open per cluster node
        (or per standalone endpoint). Default [1].

        One multiplexed connection already pipelines an unbounded
        number of concurrent fiber-issued commands through a single
        writev + FIFO matcher; this knob is {e not} for throughput
        under normal workloads. Raise it only for specific cases
        where a single connection is known to bottleneck:

        - TLS single-stream CPU saturation (one AES core per conn).
        - Very large value transfers causing head-of-line blocking
          against small commands on the same socket.
        - Heavy CPU-bound server commands (EVAL / FUNCTION CALL /
          SORT) where the server processes one command per
          connection at a time.

        Tradeoffs:
        - [N > 1] costs [N × CLIENT ID]s on the server, [N] separate
          CLIENT TRACKING registrations when [client_cache] is
          configured, and [N] handshake round-trips at (re)connect.
        - Round-robin load-balances single-command dispatch across
          the bundle. Multi-frame atomic submits (OPTIN CSC pair,
          ASK triple) pin to [bundle.(slot mod N)] for wire
          adjacency.
        - Transactions (WATCH/MULTI/EXEC) slot-affinity-pin to one
          conn per slot via the same rule, so transaction state
          stays connection-local as today.

        Must be [>= 1]; [Client.connect] raises [Invalid_argument]
        otherwise. *)

    blocking_pool : Blocking_pool.Config.t;
    (** Lease pool for blocking commands
        ([BLPOP]/[BRPOP]/[BLMOVE]/[BLMPOP]/[BZPOPMIN]/
        [BZPOPMAX]/[XREAD BLOCK]/[XREADGROUP BLOCK]).

        Default is [Blocking_pool.Config.default], where
        [max_per_node = 0] disables the feature — every
        blocking command wrapper returns
        [`Pool_not_configured] until the user opts in.

        Opt in by setting [max_per_node >= 1]. Pool
        connections never enable CLIENT TRACKING and never
        participate in MULTI/EXEC or OPTIN pair submits.
        The pool is closed in {!close}.

        [WAIT] / [WAITAOF] do {b not} route through this
        pool — they require an explicit dedicated connection
        via {!with_dedicated_conn}. *)
  }
  val default : t
end

type t

val connect :
  sw:Eio.Switch.t ->
  net:[> [> `Generic | `Unix ] Eio.Net.ty ] Eio.Resource.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?config:Config.t ->
  host:string ->
  port:int ->
  unit ->
  t

val topology_hooks_for_pool_ref :
  Blocking_pool.t option ref -> Cluster_router.Config.topology_hooks
(** Build a [Cluster_router.Config.topology_hooks] that closes
    over [pool_ref] and calls [Blocking_pool.drain_node] /
    [Blocking_pool.refresh_node] when a pool is present.

    Resolves the chicken-and-egg problem for cluster-mode
    users of {!from_router}: they must supply a
    [Cluster_router.Config] before the [Blocking_pool.t]
    exists. Pattern:

    {[
      let pool_ref = ref None in
      let cfg = {
        (Cluster_router.Config.default ~seeds) with
        topology_hooks = Client.topology_hooks_for_pool_ref pool_ref;
        connection = conn_cfg;
      } in
      match Cluster_router.create ~sw ~net ~clock ~config:cfg () with
      | Error e -> ...
      | Ok router ->
          let client =
            Client.from_router ~sw ~net ~clock ~config:client_cfg router
          in
          pool_ref := Client.For_testing.blocking_pool client;
          client
    ]}

    [from_router] does NOT fill the ref for you — you do it
    after the call to wire the pool into the already-running
    refresh loop's hook callbacks. *)

val from_router :
  ?sw:Eio.Switch.t ->
  ?net:[> [> `Generic | `Unix ] Eio.Net.ty ] Eio.Resource.t ->
  ?clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  config:Config.t -> Router.t -> t
(** Wrap an arbitrary [Router.t] (e.g. cluster router) as a
    [Client.t].

    The Eio environment arguments ([~sw], [~net], [~clock],
    [?domain_mgr]) are optional — omit them when you just want
    the typed command surface without blocking-command support.
    Supply them (plus a
    [config.blocking_pool.max_per_node >= 1]) to enable the
    Blocking_pool. *)

val close : t -> unit

val cache_metrics : t -> Cache.metrics option
(** Returns a snapshot of the client-side cache counters, or
    [None] if caching is not enabled on this client. *)

val raw_connection : t -> Connection.t

val connection_for_slot : t -> int -> Connection.t option
(** Return the live connection whose primary owns [slot], or [None]
    if the pool has no entry for it. Standalone always returns the
    single connection. Used by modules like [Transaction] that need
    to pin a sequence of commands to one server. *)

val is_standalone : t -> bool
(** [true] only for the synthetic standalone/single-node path.
    Cluster mode remains [false] even when several slots currently
    map to the same primary. *)

val atomic_lock_for_slot : t -> int -> Eio.Mutex.t
(** Mutex serialising atomic operations (MULTI/EXEC blocks from
    {!Batch} with [~atomic:true], and from {!Transaction}) on the
    primary that owns [slot]. Concurrent atomic ops on the same
    primary queue behind each other; ops on different primaries
    run in parallel; non-atomic pipeline traffic bypasses it. Held
    for the duration of a single WATCH/MULTI/EXEC window. *)

val exec :
  ?timeout:float ->
  ?target:Target.t ->
  ?read_from:Read_from.t ->
  t ->
  string array ->
  (Resp3.t, Connection.Error.t) result
(** Raw command dispatch. If [target] is omitted, [Command_spec] picks
    the default route from the command name: single-key commands go
    [By_slot (slot_of key)]; writes force the [Read_from] to [Primary];
    fan-out commands (FLUSHALL, CLIENT LIST, …) fall back to a single
    random node — use {!exec_multi} for true fan-out. *)

val exec_multi :
  ?timeout:float ->
  ?fan:Router.Fan_target.t ->
  t ->
  string array ->
  (string * (Resp3.t, Connection.Error.t) result) list
(** Fan a command out to every node in the selected set. If [fan] is
    omitted, [Command_spec] picks the default: [All_nodes] for
    aggregation commands (CLIENT LIST, CLUSTER NODES), [All_primaries]
    for write-style fan-outs (FLUSHALL, SCRIPT LOAD, WAIT). *)

val custom :
  ?timeout:float ->
  ?target:Target.t ->
  ?read_from:Read_from.t ->
  t ->
  string array ->
  (Resp3.t, Connection.Error.t) result
(** Dispatch a command this library does not expose as a typed helper
    — e.g. [BITCOUNT], [PFADD], [GEORADIUS], an ACL command, a module
    command.

    Routing follows [Command_spec]: single-key commands go
    [By_slot (slot_of key)]; writes are forced to [Primary]; readonly
    commands respect [read_from]; keyless / unknown commands go to a
    random node. Pass an explicit [target] to override.

    [custom] is an alias for [exec] with a name that documents
    intent at call sites. Behavior is identical. *)

val custom_multi :
  ?timeout:float ->
  ?fan:Router.Fan_target.t ->
  t ->
  string array ->
  (string * (Resp3.t, Connection.Error.t) result) list
(** Fan a custom command to every node in the target set. Alias for
    [exec_multi]. *)

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

(** ZADD modifier mode.

    valkey.io: [ZADD key [NX | XX] [GT | LT] [CH] [INCR] score
    member [score member ...]]. The server enforces:

    - [NX | XX] are mutually exclusive.
    - [GT | LT | NX] are mutually exclusive — i.e. NX cannot
      combine with GT or LT.
    - [GT] and [LT] are mutually exclusive with each other but
      both can combine with [XX], and both can be used without
      [XX] (in which case they still allow new elements to be
      added; only existing-element updates are conditional).

    The seven modes below cover every server-permitted
    combination; illegal combinations are not expressible. *)
type zadd_mode =
  | Z_only_add                 (** [NX] — add new, never update. *)
  | Z_only_update              (** [XX] — update existing, never add. *)
  | Z_only_update_if_greater   (** [XX GT] *)
  | Z_only_update_if_less      (** [XX LT] *)
  | Z_add_or_update_if_greater (** [GT] — adds new, updates existing only if new > current. *)
  | Z_add_or_update_if_less    (** [LT] — adds new, updates existing only if new < current. *)

val zadd :
  ?timeout:float ->
  ?mode:zadd_mode ->
  ?ch:bool ->
  t -> string -> (float * string) list ->
  (int, Connection.Error.t) result
(** valkey.io: [ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]].

    Pairs are [(score, member)]. Server replies (RESP3):

    - Integer: count of newly inserted elements.
    - Integer (with [~ch:true], i.e. CH modifier): count of
      elements whose score was added OR changed.
    - Null when [~mode] aborts the operation (e.g. NX on existing
      key / XX on missing key). Surfaces as [Ok 0] here since the
      typed return is [int]; if you need to distinguish "0 added,
      no condition" from "aborted by mode", use [zadd_incr] or
      [Client.custom]. *)

val zadd_incr :
  ?timeout:float ->
  ?mode:zadd_mode ->
  t -> string -> score:float -> member:string ->
  (float option, Connection.Error.t) result
(** valkey.io: [ZADD key [NX|XX] [GT|LT] INCR score member].
    Atomic increment-or-create. The server only permits one
    score-member pair under [INCR]; this signature enforces it.

    Returns the new score on success; [None] when [~mode]
    prevented the write (server returns Null in that case). *)

val zincrby :
  ?timeout:float ->
  t -> string -> by:float -> member:string ->
  (float, Connection.Error.t) result
(** valkey.io: [ZINCRBY key increment member]. Returns the new
    score after the atomic increment. If the member doesn't
    exist, it is added with [by] as its score. If the key
    doesn't exist, a new sorted set is created. *)

val zrem :
  ?timeout:float ->
  t -> string -> string list ->
  (int, Connection.Error.t) result
(** valkey.io: [ZREM key member [member ...]]. Returns the count
    of members actually removed; non-existing members are
    silently ignored. Errors only when [key] holds a non-zset. *)

val zrank :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string ->
  (int option, Connection.Error.t) result
(** valkey.io: [ZRANK key member]. Zero-based rank from low to
    high score. Server returns Null when [member] is absent or
    [key] doesn't exist; mapped to [None]. *)

val zrank_with_score :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string ->
  ((int * float) option, Connection.Error.t) result
(** valkey.io: [ZRANK key member WITHSCORE]. Available since
    Valkey 7.2.0; older servers return an "ERR syntax" error.
    Reply: Array of [Integer rank, Double score], or Null if
    member is absent. *)

val zrevrank :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string ->
  (int option, Connection.Error.t) result
(** valkey.io: [ZREVRANK key member]. Zero-based rank from high
    to low score. Null if member is missing. *)

val zrevrank_with_score :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string ->
  ((int * float) option, Connection.Error.t) result
(** valkey.io: [ZREVRANK key member WITHSCORE] (Valkey 7.2+). *)

val zscore :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string ->
  (float option, Connection.Error.t) result
(** valkey.io: [ZSCORE key member]. RESP3 returns Double or Null.
    [None] is returned when [member] is missing OR when [key]
    doesn't exist (server returns nil in both cases). *)

val zmscore :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> string list ->
  (float option list, Connection.Error.t) result
(** valkey.io: [ZMSCORE key member [member ...]] (Valkey 6.2+).
    Returns one [Some/None] per requested member, in input
    order. If the whole [key] is missing, every member surfaces
    as [None]. *)

val zcount :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> min:score_bound -> max:score_bound ->
  (int, Connection.Error.t) result
(** valkey.io: [ZCOUNT key min max]. Bounds use the
    [score_bound] type ([Score], [Score_excl], [Score_neg_inf],
    [Score_pos_inf]). Returns the count of members whose score
    falls in the range. *)

val zrange_with_scores :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?rev:bool ->
  t -> string -> start:int -> stop:int ->
  ((string * float) list, Connection.Error.t) result
(** valkey.io: [ZRANGE key start stop [REV] WITHSCORES] (Valkey
    6.2+ unified ZRANGE). RESP3 reply is an array of
    [[member, score]] pairs; this wrapper returns
    [(member, score)] tuples in result order. *)

val zrangebyscore_with_scores :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?limit:(int * int) ->
  t -> string -> min:score_bound -> max:score_bound ->
  ((string * float) list, Connection.Error.t) result
(** valkey.io: [ZRANGEBYSCORE key min max WITHSCORES [LIMIT offset count]].
    The unified [ZRANGE ... BYSCORE] form is recommended for new
    code; this wrapper exists for parity with the older command
    that callers may already be using. *)

val zpopmin :
  ?timeout:float ->
  ?count:int ->
  t -> string ->
  ((string * float) list, Connection.Error.t) result
(** valkey.io: [ZPOPMIN key [count]] (Valkey 5.0+). Removes and
    returns the lowest-scored member(s).

    Reply shape (RESP3):
    - No [~count]: flat 2-elem array [member, score], or empty
      array when the key is missing.
    - With [~count]: array of [[member, score]] pairs. *)

val zpopmax :
  ?timeout:float ->
  ?count:int ->
  t -> string ->
  ((string * float) list, Connection.Error.t) result
(** valkey.io: [ZPOPMAX key [count]]. Same semantics as
    [zpopmin], but pops highest-scored members. *)

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
(** Returns the SHA1 of the loaded script.

    In cluster mode this fans the LOAD to every primary so any future
    [EVALSHA] — whichever shard its key routes to — finds the SHA.
    All primaries must return the same SHA (deterministic digest); a
    divergence is surfaced as [Terminal]. In standalone, this is a
    single-node LOAD. *)

val script_exists :
  ?timeout:float ->
  t -> string list -> (bool list, Connection.Error.t) result
(** A SHA is reported present only if {i every} primary has it. Fans
    to [All_primaries] and AND-reduces the per-shard rows. *)

type script_flush_mode = Flush_sync | Flush_async

val script_flush :
  ?timeout:float ->
  ?mode:script_flush_mode ->
  t -> (unit, Connection.Error.t) result
(** Fans to every primary. Clears the client's local
    known-loaded-SHA cache on success so [eval_script] falls back to
    [EVAL] as expected. *)

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
  t -> string -> (string list, Connection.Error.t) result
(** [KEYS pattern]. Fans to every primary in cluster mode (each shard
    only knows its own slots) and flattens the per-shard lists.

    [KEYS] is O(N) on each node it visits and blocks that node's
    command loop for the duration; in production, use [scan] instead. *)

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
  ?read_from:Read_from.t ->
  ?count:int ->
  t -> streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result
(** Non-blocking XREAD. In cluster mode, all stream keys must hash
    to the same slot (colocate with hashtags) or the server
    returns CROSSSLOT. The slot is computed from the first key. *)

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
(** [WAIT] against a single primary: on standalone, the one node;
    on cluster, a random primary. Returns the replica count that
    acknowledged on that one primary. [block_ms = 0] blocks
    indefinitely.

    In cluster mode this only tells you about one shard. Use the
    [_all] / [_min] / [_sum] variants below to reason about the
    cluster as a whole. *)

val wait_replicas_all :
  ?timeout:float ->
  t -> num_replicas:int -> block_ms:int ->
  ((string * int) list, Connection.Error.t) result
(** Fan [WAIT] to every primary. Returns one [(primary_id, ack_count)]
    entry per shard, preserving the raw view so the caller picks the
    aggregation policy. On standalone, a one-entry list. *)

val wait_replicas_min :
  ?timeout:float ->
  t -> num_replicas:int -> block_ms:int ->
  (int, Connection.Error.t) result
(** Fan [WAIT] and return the minimum ack count across all primaries
    — "at least this many replicas everywhere". The natural
    global-durability aggregation. Returns [0] on a cluster with no
    primaries (which cannot happen outside a pathological refresh). *)

val wait_replicas_sum :
  ?timeout:float ->
  t -> num_replicas:int -> block_ms:int ->
  (int, Connection.Error.t) result
(** Fan [WAIT] and return the sum of ack counts across primaries.
    Rarely what you want — replicas of different primaries are not
    interchangeable. Provided for completeness. *)

(** {1 Pub/sub (publish side)} *)

val publish :
  ?timeout:float ->
  t -> channel:string -> message:string ->
  (int, Connection.Error.t) result
(** [PUBLISH channel message]. Returns the number of subscribers the
    message was delivered to. Any node accepts PUBLISH; in cluster
    mode the message is propagated across shards by the server. *)

val spublish :
  ?timeout:float ->
  t -> channel:string -> message:string ->
  (int, Connection.Error.t) result
(** Sharded [SPUBLISH channel message]. Routed to the primary that
    owns [slot(channel)]; the message is only delivered to
    [SSUBSCRIBE]rs on that slot's primary and replicas — no
    cluster-wide propagation. Subscribers must connect to the same
    primary (see {!Pubsub} and {!Cluster_pubsub}). *)

val xread_block :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?count:int ->
  t -> block_ms:int ->
  streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result
(** Blocking XREAD. On server-side timeout returns an empty list.
    Same cluster rule as [xread]: all stream keys must hash to the
    same slot. *)

val xreadgroup_block :
  ?timeout:float ->
  ?count:int ->
  ?noack:bool ->
  t -> block_ms:int -> group:string -> consumer:string ->
  streams:(string * string) list ->
  ((string * stream_entry list) list, Connection.Error.t) result

(** {1 Bitmaps} *)

(** Range unit for [BITCOUNT] / [BITPOS]: whether [start] and [end]
    are measured in bytes or in individual bits. BYTE | BIT modifier
    is a Valkey 7.0+ feature; omit when talking to older servers. *)
type bit_range_unit = Byte | Bit

(** Range modifier for [BITCOUNT] / [BITPOS].

    - [From start] — Valkey 8.0+. Defaults [end] to the last byte.
    - [From_to { start; end_ }] — the classic form; unit defaults to
      [Byte] on the server.
    - [From_to_unit { start; end_; unit }] — explicit unit (Valkey 7.0+).

    Negative indices work like [GETRANGE]: [-1] is the last byte/bit. *)
type bit_range =
  | From of int
  | From_to of { start : int; end_ : int }
  | From_to_unit of { start : int; end_ : int; unit : bit_range_unit }

val bitcount :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?range:bit_range ->
  t -> string -> (int, Connection.Error.t) result
(** [BITCOUNT key [start [end [BYTE | BIT]]]]. Integer reply: the
    number of set bits. Returns [0] for a non-existent key. *)

(** Target bit value for [BITPOS]. *)
type bit = B0 | B1

val bitpos :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?range:bit_range ->
  t -> string -> bit:bit -> (int, Connection.Error.t) result
(** [BITPOS key bit [start [end [BYTE | BIT]]]]. Returns the position
    of the first matching bit, or [-1] when not found.

    Range-elision detail per Valkey docs: when searching for [B0] and
    the range is unbounded on the right, the server treats the string
    as zero-padded beyond its actual length — which can yield a
    position past end-of-string. Specifying an explicit [end] forces
    strict "no clear bit in range" semantics that return [-1]. *)

(** [BITOP] operator + sources. Each constructor enforces the source
    arity documented by Valkey: AND/OR/XOR take one or more source
    keys; NOT (bitwise inversion) takes exactly one. *)
type bitop =
  | Bitop_and of string list
  | Bitop_or of string list
  | Bitop_xor of string list
  | Bitop_not of string

val bitop :
  ?timeout:float ->
  t -> bitop -> destination:string ->
  (int, Connection.Error.t) result
(** [BITOP op destination source...]. Integer reply: the size in
    bytes of the destination string (equal to the longest source).
    In cluster mode, [destination] and every source must hash to
    the same slot (hashtags); otherwise the server returns
    [CROSSSLOT]. *)

val setbit :
  ?timeout:float ->
  t -> string -> offset:int -> value:bit ->
  (bit, Connection.Error.t) result
(** [SETBIT key offset value]. Returns the previous bit value. *)

val getbit :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> offset:int ->
  (bit, Connection.Error.t) result
(** [GETBIT key offset]. *)

(** Integer type descriptor for [BITFIELD]. [Signed n] means
    [iN]; [Unsigned n] means [uN]. Valkey supports signed widths
    1..64 and unsigned widths 1..63. *)
type bitfield_type = Signed of int | Unsigned of int

(** Offset into the bit-string. [Bit n] is an absolute bit offset
    (no prefix on the wire); [Scaled n] is [n] type-widths from
    the start (wire prefix [#]). *)
type bitfield_offset = Bit_offset of int | Scaled_offset of int

type bitfield_overflow = Wrap | Sat | Fail

(** One BITFIELD sub-operation. [Overflow] has no reply; every
    other variant contributes one element to the response. *)
type bitfield_op =
  | Get of { ty : bitfield_type; at : bitfield_offset }
  | Set of { ty : bitfield_type; at : bitfield_offset; value : int64 }
  | Incrby of
      { ty : bitfield_type; at : bitfield_offset; increment : int64 }
  | Overflow of bitfield_overflow

val bitfield :
  ?timeout:float ->
  t -> string -> bitfield_op list ->
  (int64 option list, Connection.Error.t) result
(** [BITFIELD key ops...]. Returns one reply per [Get] / [Set] /
    [Incrby] op, in order — [Overflow] modifiers produce no reply.
    An [Incrby] under [Overflow Fail] returns [None] on overflow;
    other replies are [Some _]. *)

val bitfield_ro :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> gets:(bitfield_type * bitfield_offset) list ->
  (int64 list, Connection.Error.t) result
(** [BITFIELD_RO key GET ...]. Read-only subset of [BITFIELD];
    only [GET] operations are accepted. *)

(** {1 HyperLogLog} *)

val pfadd :
  ?timeout:float ->
  t -> string -> elements:string list ->
  (bool, Connection.Error.t) result
(** [PFADD key [element ...]]. Returns [true] when at least one
    internal register was altered, [false] otherwise. An empty
    [elements] list is valid — it just materialises the key as an
    empty HLL if it did not already exist. *)

val pfcount :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string list ->
  (int, Connection.Error.t) result
(** [PFCOUNT key [key ...]]. Approximate cardinality for a single
    HLL, or the approximate cardinality of the union when multiple
    keys are given. Multi-key form in cluster mode requires all
    keys hash to the same slot. *)

val pfmerge :
  ?timeout:float ->
  t -> destination:string -> sources:string list ->
  (unit, Connection.Error.t) result
(** [PFMERGE destkey sourcekey ...]. Empty [sources] is a valid
    no-op that materialises [destination] as an empty HLL. *)

(** {1 Generic keyspace} *)

val copy :
  ?timeout:float ->
  ?db:int ->
  ?replace:bool ->
  t -> source:string -> destination:string ->
  (bool, Connection.Error.t) result
(** [COPY source destination [DB db] [REPLACE]]. Returns [true]
    when the copy happened, [false] when [destination] already
    existed without [replace:true] or [source] was missing.
    Available since Valkey 6.2. In cluster mode, [source] and
    [destination] must hash to the same slot. *)

val dump :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string ->
  (string option, Connection.Error.t) result
(** [DUMP key]. Returns the RDB-encoded serialized value (valid
    input for {!restore}) or [None] if [key] does not exist. The
    serialized bytes include an RDB version and a 64-bit checksum;
    TTL is NOT included — use [PTTL] separately. *)

val restore :
  ?timeout:float ->
  ?replace:bool ->
  ?abs_ttl:bool ->
  ?idle_time_seconds:int ->
  ?freq:int ->
  t -> string ->
  ttl_ms:int ->
  serialized:string ->
  (unit, Connection.Error.t) result
(** [RESTORE key ttl serialized [REPLACE] [ABSTTL]
    [IDLETIME seconds] [FREQ frequency]]. [ttl_ms = 0] means no
    expiration. With [abs_ttl:true] the TTL is interpreted as an
    absolute Unix-milliseconds timestamp. Returns a
    [Server_error { code = "BUSYKEY" ; _ }] if the key already
    exists and [replace:true] was not passed. *)

val touch :
  ?timeout:float ->
  t -> string list ->
  (int, Connection.Error.t) result
(** [TOUCH key ...]. Updates last-access time. Returns the number
    of keys that existed. Missing keys are silently ignored. *)

val randomkey :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> (string option, Connection.Error.t) result
(** [RANDOMKEY]. Returns a random existing key, or [None] when the
    keyspace is empty. In cluster mode, hits a random primary and
    samples from that node's slots — not a uniform cluster-wide
    sample. *)

(** {2 OBJECT sub-commands}

    All four return [None] when the key does not exist. [idletime]
    requires the server's [maxmemory-policy] to {i not} be an LFU
    variant; [freq] requires it {i to be} an LFU variant. Using
    the wrong one surfaces as a [Server_error]. *)

val object_encoding :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string ->
  (string option, Connection.Error.t) result

val object_refcount :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string ->
  (int option, Connection.Error.t) result

val object_idletime :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string ->
  (int option, Connection.Error.t) result

val object_freq :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string ->
  (int option, Connection.Error.t) result

(** {1 Geo}

    Valkey stores geo data in a sorted set under the hood; this
    module exposes only the geo-shaped typed API. Longitude range
    is [[-180, 180]], latitude range is [[-85.05112878, 85.05112878]]. *)

type geo_unit = Meters | Kilometers | Miles | Feet

type geo_coord = { longitude : float; latitude : float }

type geo_point = {
  longitude : float;
  latitude : float;
  member : string;
}

(** Mutually exclusive existence condition on [GEOADD]; [NX | XX]. *)
type geoadd_cond = Geoadd_nx | Geoadd_xx

val geoadd :
  ?timeout:float ->
  ?cond:geoadd_cond ->
  ?ch:bool ->
  t -> string -> points:geo_point list ->
  (int, Connection.Error.t) result
(** [GEOADD key [NX | XX] [CH] lon lat member ...]. Returns the
    count: number of new elements added, or (with [ch:true]) the
    number of elements that were added {i or} updated in place. *)

val geodist :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?unit:geo_unit ->
  t -> string -> member1:string -> member2:string ->
  (float option, Connection.Error.t) result
(** [GEODIST key m1 m2 [unit]]. [None] when either member is
    missing. Distance is always positive. *)

val geopos :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> members:string list ->
  (geo_coord option list, Connection.Error.t) result
(** [GEOPOS key member ...]. One entry per input member, in input
    order. [None] for members that don't exist. *)

val geohash :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> members:string list ->
  (string option list, Connection.Error.t) result
(** [GEOHASH key member ...]. One Geohash-encoded string per
    input member, in input order; [None] for missing members. *)

(** Search origin — either an existing member's position or a
    concrete longitude/latitude pair. *)
type geo_from =
  | From_member of string
  | From_lonlat of { longitude : float; latitude : float }

(** Search area. [BYPOLYGON] is not exposed in this release — open
    an issue if you need it. *)
type geo_shape =
  | By_radius of { radius : float; unit : geo_unit }
  | By_box of { width : float; height : float; unit : geo_unit }

type geo_order = Geo_asc | Geo_desc

type geo_count = {
  count : int;
  any : bool;
  (** [ANY] returns the first [count] matches without the
      nearest-first guarantee — cheaper when you just need "some". *)
}

(** A single GEOSEARCH result. Every optional field corresponds to
    a [WITH*] flag on the call:
    - [distance] filled when [?with_dist:true]
    - [hash] filled when [?with_hash:true]
    - [coord] filled when [?with_coord:true]
    Otherwise [None]. *)
type geo_search_result = {
  member : string;
  distance : float option;
  hash : int64 option;
  coord : geo_coord option;
}

val geosearch :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?order:geo_order ->
  ?count:geo_count ->
  ?with_coord:bool ->
  ?with_dist:bool ->
  ?with_hash:bool ->
  t -> string ->
  from:geo_from ->
  shape:geo_shape ->
  (geo_search_result list, Connection.Error.t) result
(** [GEOSEARCH key <FROM...> <BY...> [ASC|DESC] [COUNT n [ANY]]
      [WITHCOORD] [WITHDIST] [WITHHASH]]. *)

val geosearchstore :
  ?timeout:float ->
  ?order:geo_order ->
  ?count:geo_count ->
  ?store_dist:bool ->
  t -> destination:string -> source:string ->
  from:geo_from ->
  shape:geo_shape ->
  (int, Connection.Error.t) result
(** [GEOSEARCHSTORE destination source <FROM...> <BY...>
      [ASC|DESC] [COUNT n [ANY]] [STOREDIST]]. Returns the number
    of elements stored. In cluster mode, [source] and
    [destination] must hash to the same slot. [STOREDIST] stores
    distances instead of the geo positions. *)

(** {1 CLIENT admin}

    Per-connection and fleet-wide admin. Every typed wrapper here
    goes through [Client.exec] on a single connection — including
    [client_list], which fans to all nodes internally in cluster
    mode and concatenates. *)

val client_id :
  ?timeout:float -> t -> (int, Connection.Error.t) result
(** [CLIENT ID]. Monotonic unique identifier for this connection
    on the server it is currently attached to. Resets after a
    reconnect. *)

val client_getname :
  ?timeout:float -> t -> (string option, Connection.Error.t) result
(** [CLIENT GETNAME]. [None] when no name has been set. *)

val client_setname :
  ?timeout:float -> t -> name:string ->
  (unit, Connection.Error.t) result
(** [CLIENT SETNAME name]. [name] must not contain spaces (Valkey
    would reject). Empty string is also rejected by the server. *)

val client_info :
  ?timeout:float -> t -> (string, Connection.Error.t) result
(** [CLIENT INFO]. One-line [key=value ...] dump describing the
    current connection. Format is documented on the CLIENT LIST
    page. *)

(** Filter value for [client_list] and [client_kill]. *)
type client_type = Client_normal | Client_master | Client_replica
                 | Client_pubsub

val client_list :
  ?timeout:float ->
  ?type_:client_type ->
  ?ids:int list ->
  t -> (string, Connection.Error.t) result
(** [CLIENT LIST [TYPE ...] [ID id [id ...]]]. In cluster mode
    fans to every node (one call per primary + replica) and
    concatenates the output. Returns a multiline string; each
    line is one connection in [key=value ...] form. *)

type client_pause_mode = Pause_all | Pause_write

val client_pause :
  ?timeout:float ->
  ?mode:client_pause_mode ->
  t -> timeout_ms:int ->
  (unit, Connection.Error.t) result
(** [CLIENT PAUSE timeout [WRITE | ALL]]. Suspends other
    connections on the same server. Default mode is [Pause_all].
    Fans to every primary in cluster mode. *)

val client_unpause :
  ?timeout:float -> t -> (unit, Connection.Error.t) result
(** [CLIENT UNPAUSE]. Fans to every primary in cluster mode. *)

val client_no_evict :
  ?timeout:float -> t -> bool -> (unit, Connection.Error.t) result
(** [CLIENT NO-EVICT ON|OFF]. Pass [true] to protect the current
    connection from eviction under memory pressure. *)

val client_no_touch :
  ?timeout:float -> t -> bool -> (unit, Connection.Error.t) result
(** [CLIENT NO-TOUCH ON|OFF]. Pass [true] to stop tracking
    last-access time on keys this connection reads. *)

(** Filter for [client_kill]. Filters AND together at the server;
    the [Not_*] variants negate. Covers the commonly-used filters;
    uncommon ones (LIB-NAME / DB / CAPA / IP / IDLE variants)
    can be added by user request. *)
type client_kill_filter =
  | Kill_id of int
  | Kill_not_id of int
  | Kill_type of client_type
  | Kill_not_type of client_type
  | Kill_addr of string
  | Kill_not_addr of string
  | Kill_laddr of string
  | Kill_not_laddr of string
  | Kill_user of string
  | Kill_not_user of string
  | Kill_skipme of bool
  | Kill_maxage of int

val client_kill :
  ?timeout:float ->
  t -> filters:client_kill_filter list ->
  (int, Connection.Error.t) result
(** [CLIENT KILL <filter> <value> ...]. Returns the number of
    connections killed. In cluster mode, dispatched to a single
    random node — use [exec_multi ~fan:All_nodes] if you want to
    sweep the whole fleet. *)

val client_tracking_on :
  ?timeout:float ->
  ?redirect:int ->
  ?prefixes:string list ->
  ?bcast:bool ->
  ?optin:bool ->
  ?optout:bool ->
  ?noloop:bool ->
  t -> unit -> (unit, Connection.Error.t) result
(** [CLIENT TRACKING ON [REDIRECT id] [PREFIX p ...] [BCAST]
       [OPTIN] [OPTOUT] [NOLOOP]].

    [prefixes] require [bcast:true]; [optin] and [optout] are
    mutually exclusive (the server enforces). When [redirect] is
    set, invalidations are forwarded to that client-id instead of
    to the current connection. *)

val client_tracking_off :
  ?timeout:float -> t -> (unit, Connection.Error.t) result
(** [CLIENT TRACKING OFF]. *)

(** {1 Functions (Valkey 7+)}

    Functions are server-side Lua libraries. Load once with
    [function_load], call repeatedly with [fcall] / [fcall_ro].
    Unlike scripts, functions persist across restarts and
    replicate to replicas.

    All function-lifecycle commands ({!function_load},
    {!function_delete}, {!function_flush}) fan to every primary
    so the library is present on every shard — required for
    [FCALL] to succeed regardless of which slot's primary the
    call routes to. *)

val function_load :
  ?timeout:float ->
  ?replace:bool ->
  t -> source:string ->
  (string, Connection.Error.t) result
(** [FUNCTION LOAD [REPLACE] source]. The [source] must start
    with a shebang line [#!<engine> name=<library>]. Returns the
    loaded library's name. Fans to every primary. *)

val function_delete :
  ?timeout:float ->
  t -> library_name:string ->
  (unit, Connection.Error.t) result
(** [FUNCTION DELETE library]. Fans to every primary; server
    errors if the library is unknown on any of them. *)

val function_flush :
  ?timeout:float ->
  ?mode:script_flush_mode ->
  t -> (unit, Connection.Error.t) result
(** [FUNCTION FLUSH [ASYNC | SYNC]]. Fans to every primary.
    Removes every loaded library. *)

val function_list :
  ?timeout:float ->
  ?library_name_pattern:string ->
  ?with_code:bool ->
  t -> (Resp3.t list, Connection.Error.t) result
(** [FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]]. Returns
    one [Resp3.t] per library — each is typically a map with
    [library_name], [engine], [functions], and (if
    [with_code:true]) [library_code] fields. Decoding is left to
    the caller because the field set is version-dependent. *)

val fcall :
  ?timeout:float ->
  t -> function_:string -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result
(** [FCALL function numkeys key... arg...]. Routed by the first
    key's slot. In cluster mode all keys must hash to the same
    slot. *)

val fcall_ro :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> function_:string -> keys:string list -> args:string list ->
  (Resp3.t, Connection.Error.t) result
(** [FCALL_RO function numkeys key... arg...]. Read-only variant;
    honours [?read_from] so it can land on a replica. *)

(** {1 CLUSTER introspection} *)

val cluster_keyslot :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> key:string ->
  (int, Connection.Error.t) result
(** [CLUSTER KEYSLOT key]. Useful to cross-check {!Valkey.Slot.of_key}
    against the server's own CRC16 implementation. *)

val cluster_info :
  ?timeout:float ->
  t -> (string, Connection.Error.t) result
(** [CLUSTER INFO]. Returns a CRLF-separated [key:value] string
    covering cluster_state, cluster_slots_ok, cluster_known_nodes,
    cluster_size, and friends. *)

(** {1 LATENCY} *)

val latency_doctor :
  ?timeout:float -> t -> (string, Connection.Error.t) result
(** [LATENCY DOCTOR]. Server-generated human-readable latency
    analysis report. *)

val latency_reset :
  ?timeout:float ->
  ?events:string list ->
  t -> (int, Connection.Error.t) result
(** [LATENCY RESET [event ...]]. Without [events], resets every
    tracked event. Returns the number of event series cleared. *)

(** {1 MEMORY} *)

val memory_usage :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  ?samples:int ->
  t -> string ->
  (int option, Connection.Error.t) result
(** [MEMORY USAGE key [SAMPLES count]]. Bytes the key + value
    occupy in RAM, or [None] if the key does not exist. [samples]
    controls nested-container sampling (default 5 on the server;
    pass 0 to sample all children). *)

val memory_purge :
  ?timeout:float -> t -> (unit, Connection.Error.t) result
(** [MEMORY PURGE]. Asks the allocator to release dirty pages.
    No-op on non-jemalloc builds. *)


(** {1 Internals exposed for testing} *)

module For_testing : sig
  val map_optin_pair_reply :
    ((Resp3.t, Connection.Error.t) result
     * (Resp3.t, Connection.Error.t) result,
     Connection.Error.t) result ->
    (Resp3.t, Connection.Error.t) result
  (** Pure mapping from a {!Connection.request_pair} result to
      the OPTIN read's effective reply. Lifted out of
      [exec_optin_pair] so every arm — including the
      [Protocol_violation] case (server replies non-OK to
      [CLIENT CACHING YES]) and the frame-1 transport-error case
      — is covered by pure unit tests. *)

  val blocking_pool : t -> Blocking_pool.t option
  (** Returns the client's blocking pool if one was constructed
      at [connect] / [from_router] time, or [None] if the
      feature is disabled. Used by integration tests to drive
      [Blocking_pool.drain_node] directly and assert the
      resulting [Node_gone] path. *)
end
