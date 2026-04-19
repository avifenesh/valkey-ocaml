type t =
  | Single_key of { key_index : int; readonly : bool }
  | Multi_key of { first_key_index : int; readonly : bool }
  | Keyless_random of { readonly : bool }
  | Fan_primaries
  | Fan_all_nodes

let readonly = function
  | Single_key { readonly; _ }
  | Multi_key { readonly; _ }
  | Keyless_random { readonly } -> readonly
  | Fan_primaries | Fan_all_nodes -> false

(* Helper constructors — keep the tables compact and the intent
   obvious at a glance. Every Valkey command routes via one of these.
   "wk<i>" = write,  key at position i.
   "rk<i>" = read,   key at position i.
   "mw"    = multi-key write; caller co-locates via hashtag.
   "mr"    = multi-key read.  *)
let wk i = Single_key { key_index = i; readonly = false }
let rk i = Single_key { key_index = i; readonly = true }
let mw i = Multi_key { first_key_index = i; readonly = false }
let mr i = Multi_key { first_key_index = i; readonly = true }
let rand_r = Keyless_random { readonly = true }
let rand_w = Keyless_random { readonly = false }

(* Two-word commands (subcommand in args.(1)). Looked up with names
   already upper-cased. *)
let two_word : (string * string * t) list =
  [
    (* SCRIPT / FUNCTION — scripts live on every primary *)
    "SCRIPT", "LOAD", Fan_primaries;
    "SCRIPT", "FLUSH", Fan_primaries;
    "SCRIPT", "EXISTS", Fan_primaries;
    "FUNCTION", "LOAD", Fan_primaries;
    "FUNCTION", "DELETE", Fan_primaries;
    "FUNCTION", "FLUSH", Fan_primaries;
    "FUNCTION", "LIST", Fan_primaries;
    "FUNCTION", "DUMP", Fan_primaries;
    "FUNCTION", "STATS", Fan_primaries;
    "FUNCTION", "RESTORE", Fan_primaries;

    (* CONFIG — global server configuration is per-node *)
    "CONFIG", "SET", Fan_primaries;
    "CONFIG", "RESETSTAT", Fan_primaries;
    "CONFIG", "REWRITE", Fan_primaries;
    "CONFIG", "GET", rand_r;

    (* CLIENT — most are per-connection, so any one node answers *)
    "CLIENT", "LIST", Fan_all_nodes;
    "CLIENT", "ID", rand_r;
    "CLIENT", "GETNAME", rand_r;
    "CLIENT", "INFO", rand_r;
    "CLIENT", "GETREDIR", rand_r;
    "CLIENT", "SETNAME", rand_w;
    "CLIENT", "SETINFO", rand_w;
    "CLIENT", "NO-EVICT", rand_w;
    "CLIENT", "NO-TOUCH", rand_w;
    "CLIENT", "TRACKING", rand_w;
    "CLIENT", "TRACKINGINFO", rand_r;
    "CLIENT", "CACHING", rand_w;
    "CLIENT", "REPLY", rand_w;
    "CLIENT", "PAUSE", Fan_primaries;
    "CLIENT", "UNPAUSE", Fan_primaries;
    "CLIENT", "KILL", rand_w;

    (* CLUSTER — cluster membership queries *)
    "CLUSTER", "NODES", Fan_all_nodes;
    "CLUSTER", "SHARDS", rand_r;
    "CLUSTER", "INFO", rand_r;
    "CLUSTER", "MYID", rand_r;
    "CLUSTER", "MYSHARDID", rand_r;
    "CLUSTER", "COUNTKEYSINSLOT", rand_r;
    "CLUSTER", "KEYSLOT", rand_r;
    "CLUSTER", "GETKEYSINSLOT", rand_r;
    "CLUSTER", "COUNT-FAILURE-REPORTS", rand_r;
    "CLUSTER", "LINKS", rand_r;
    "CLUSTER", "SLAVES", rand_r;
    "CLUSTER", "REPLICAS", rand_r;
    "CLUSTER", "SLOTS", rand_r;
    "CLUSTER", "MEET", rand_w;
    "CLUSTER", "FORGET", rand_w;
    "CLUSTER", "RESET", rand_w;
    "CLUSTER", "ADDSLOTS", rand_w;
    "CLUSTER", "DELSLOTS", rand_w;
    "CLUSTER", "ADDSLOTSRANGE", rand_w;
    "CLUSTER", "DELSLOTSRANGE", rand_w;
    "CLUSTER", "FLUSHSLOTS", rand_w;
    "CLUSTER", "SETSLOT", rand_w;
    "CLUSTER", "BUMPEPOCH", rand_w;
    "CLUSTER", "REPLICATE", rand_w;
    "CLUSTER", "FAILOVER", rand_w;
    "CLUSTER", "HELP", rand_r;

    (* XINFO / stream introspection *)
    "XINFO", "STREAM", rk 2;
    "XINFO", "GROUPS", rk 2;
    "XINFO", "CONSUMERS", rk 2;
    "XINFO", "HELP", rand_r;

    (* OBJECT — each variant takes a key *)
    "OBJECT", "ENCODING", rk 2;
    "OBJECT", "REFCOUNT", rk 2;
    "OBJECT", "IDLETIME", rk 2;
    "OBJECT", "FREQ", rk 2;
    "OBJECT", "HELP", rand_r;

    (* DEBUG (mostly disabled on production Valkey 8+) *)
    "DEBUG", "OBJECT", rk 2;
    "DEBUG", "SLEEP", rand_w;
    "DEBUG", "JMAP", rand_r;
    "DEBUG", "SET-ACTIVE-EXPIRE", rand_w;

    (* MEMORY — USAGE takes a key; others are server-local *)
    "MEMORY", "USAGE", rk 2;
    "MEMORY", "STATS", rand_r;
    "MEMORY", "DOCTOR", rand_r;
    "MEMORY", "HELP", rand_r;
    "MEMORY", "PURGE", rand_w;
    "MEMORY", "MALLOC-STATS", rand_r;

    (* LATENCY *)
    "LATENCY", "LATEST", rand_r;
    "LATENCY", "HISTORY", rand_r;
    "LATENCY", "RESET", rand_w;
    "LATENCY", "GRAPH", rand_r;
    "LATENCY", "DOCTOR", rand_r;
    "LATENCY", "HELP", rand_r;
    "LATENCY", "HISTOGRAM", rand_r;

    (* SLOWLOG *)
    "SLOWLOG", "GET", rand_r;
    "SLOWLOG", "LEN", rand_r;
    "SLOWLOG", "RESET", rand_w;
    "SLOWLOG", "HELP", rand_r;

    (* ACL — global user management, propagate everywhere *)
    "ACL", "LIST", Fan_primaries;
    "ACL", "USERS", Fan_primaries;
    "ACL", "WHOAMI", rand_r;
    "ACL", "GETUSER", Fan_primaries;
    "ACL", "SETUSER", Fan_primaries;
    "ACL", "DELUSER", Fan_primaries;
    "ACL", "CAT", rand_r;
    "ACL", "GENPASS", rand_r;
    "ACL", "LOG", rand_r;
    "ACL", "LOAD", Fan_primaries;
    "ACL", "SAVE", Fan_primaries;
    "ACL", "HELP", rand_r;

    (* PUBSUB introspection (not SUBSCRIBE — that is connection-bound) *)
    "PUBSUB", "CHANNELS", Fan_all_nodes;
    "PUBSUB", "NUMSUB", Fan_all_nodes;
    "PUBSUB", "NUMPAT", Fan_all_nodes;
    "PUBSUB", "SHARDCHANNELS", Fan_all_nodes;
    "PUBSUB", "SHARDNUMSUB", Fan_all_nodes;
    "PUBSUB", "HELP", rand_r;

    (* MODULE *)
    "MODULE", "LOAD", Fan_primaries;
    "MODULE", "LOADEX", Fan_primaries;
    "MODULE", "UNLOAD", Fan_primaries;
    "MODULE", "LIST", Fan_all_nodes;

    (* COMMAND introspection *)
    "COMMAND", "COUNT", rand_r;
    "COMMAND", "DOCS", rand_r;
    "COMMAND", "INFO", rand_r;
    "COMMAND", "LIST", rand_r;
    "COMMAND", "GETKEYS", rand_r;
    "COMMAND", "GETKEYSANDFLAGS", rand_r;
  ]

(* Single-word commands. Alphabetical within groups. *)
let one_word : (string * t) list =
  [
    (* ---------- strings / counters ---------- *)
    "APPEND", wk 1;
    "DECR", wk 1;
    "DECRBY", wk 1;
    "GET", rk 1;
    "GETDEL", wk 1;
    "GETEX", wk 1;
    "GETRANGE", rk 1;
    "GETSET", wk 1;
    "INCR", wk 1;
    "INCRBY", wk 1;
    "INCRBYFLOAT", wk 1;
    "PSETEX", wk 1;
    "SET", wk 1;
    "SETEX", wk 1;
    "SETNX", wk 1;
    "SETRANGE", wk 1;
    "STRLEN", rk 1;
    "SUBSTR", rk 1;

    (* ---------- generic keyspace ---------- *)
    "COPY", mw 1;
    "DEL", mw 1;
    "DELIFEQ", wk 1;
    "DUMP", rk 1;
    "EXISTS", mr 1;
    "EXPIRE", wk 1;
    "EXPIREAT", wk 1;
    "EXPIRETIME", rk 1;
    "LCS", mr 1;
    "MGET", mr 1;
    "MSET", mw 1;
    "MSETNX", mw 1;
    "PERSIST", wk 1;
    "PEXPIRE", wk 1;
    "PEXPIREAT", wk 1;
    "PEXPIRETIME", rk 1;
    "PTTL", rk 1;
    "RANDOMKEY", rand_r;
    "RENAME", mw 1;
    "RENAMENX", mw 1;
    "RESTORE", wk 1;
    "SORT", wk 1;                    (* SORT has STORE form - writes *)
    "SORT_RO", rk 1;
    "TOUCH", mr 1;
    "TTL", rk 1;
    "TYPE", rk 1;
    "UNLINK", mw 1;

    (* ---------- hashes ---------- *)
    "HDEL", wk 1;
    "HEXISTS", rk 1;
    "HEXPIRE", wk 1;
    "HEXPIREAT", wk 1;
    "HEXPIRETIME", rk 1;
    "HGET", rk 1;
    "HGETALL", rk 1;
    "HGETDEL", wk 1;
    "HGETEX", wk 1;
    "HINCRBY", wk 1;
    "HINCRBYFLOAT", wk 1;
    "HKEYS", rk 1;
    "HLEN", rk 1;
    "HMGET", rk 1;
    "HMSET", wk 1;
    "HPERSIST", wk 1;
    "HPEXPIRE", wk 1;
    "HPEXPIREAT", wk 1;
    "HPEXPIRETIME", rk 1;
    "HPTTL", rk 1;
    "HRANDFIELD", rk 1;
    "HSCAN", rk 1;
    "HSET", wk 1;
    "HSETEX", wk 1;
    "HSETNX", wk 1;
    "HSTRLEN", rk 1;
    "HTTL", rk 1;
    "HVALS", rk 1;

    (* ---------- lists ---------- *)
    "BLMOVE", mw 1;
    "BLMPOP", Keyless_random { readonly = false };  (* keys after count *)
    "BLPOP", mw 1;
    "BRPOP", mw 1;
    "BRPOPLPUSH", mw 1;
    "LINDEX", rk 1;
    "LINSERT", wk 1;
    "LLEN", rk 1;
    "LMOVE", mw 1;
    "LMPOP", Keyless_random { readonly = false };
    "LPOP", wk 1;
    "LPOS", rk 1;
    "LPUSH", wk 1;
    "LPUSHX", wk 1;
    "LRANGE", rk 1;
    "LREM", wk 1;
    "LSET", wk 1;
    "LTRIM", wk 1;
    "RPOP", wk 1;
    "RPOPLPUSH", mw 1;
    "RPUSH", wk 1;
    "RPUSHX", wk 1;

    (* ---------- sets ---------- *)
    "SADD", wk 1;
    "SCARD", rk 1;
    "SDIFF", mr 1;
    "SDIFFSTORE", mw 1;
    "SINTER", mr 1;
    "SINTERCARD", Keyless_random { readonly = true };  (* keys after count *)
    "SINTERSTORE", mw 1;
    "SISMEMBER", rk 1;
    "SMEMBERS", rk 1;
    "SMISMEMBER", rk 1;
    "SMOVE", mw 1;
    "SPOP", wk 1;
    "SRANDMEMBER", rk 1;
    "SREM", wk 1;
    "SSCAN", rk 1;
    "SUNION", mr 1;
    "SUNIONSTORE", mw 1;

    (* ---------- sorted sets ---------- *)
    "BZMPOP", Keyless_random { readonly = false };
    "BZPOPMAX", mw 1;
    "BZPOPMIN", mw 1;
    "ZADD", wk 1;
    "ZCARD", rk 1;
    "ZCOUNT", rk 1;
    "ZDIFF", Keyless_random { readonly = true };  (* keys after count *)
    "ZDIFFSTORE", mw 1;
    "ZINCRBY", wk 1;
    "ZINTER", Keyless_random { readonly = true };
    "ZINTERCARD", Keyless_random { readonly = true };
    "ZINTERSTORE", mw 1;
    "ZLEXCOUNT", rk 1;
    "ZMPOP", Keyless_random { readonly = false };
    "ZMSCORE", rk 1;
    "ZPOPMAX", wk 1;
    "ZPOPMIN", wk 1;
    "ZRANDMEMBER", rk 1;
    "ZRANGE", rk 1;
    "ZRANGEBYLEX", rk 1;
    "ZRANGEBYSCORE", rk 1;
    "ZRANGESTORE", mw 1;
    "ZRANK", rk 1;
    "ZREM", wk 1;
    "ZREMRANGEBYLEX", wk 1;
    "ZREMRANGEBYRANK", wk 1;
    "ZREMRANGEBYSCORE", wk 1;
    "ZREVRANGE", rk 1;
    "ZREVRANGEBYLEX", rk 1;
    "ZREVRANGEBYSCORE", rk 1;
    "ZREVRANK", rk 1;
    "ZSCAN", rk 1;
    "ZSCORE", rk 1;
    "ZUNION", Keyless_random { readonly = true };
    "ZUNIONSTORE", mw 1;

    (* ---------- streams ---------- *)
    "XACK", wk 1;
    "XADD", wk 1;
    "XAUTOCLAIM", wk 1;
    "XCLAIM", wk 1;
    "XDEL", wk 1;
    "XGROUP", wk 2;
    "XLEN", rk 1;
    "XPENDING", rk 1;
    "XRANGE", rk 1;
    (* XREAD / XREADGROUP: keys live after STREAMS keyword — the
       typed wrapper computes the slot from the first stream key and
       passes target explicitly. Default Keyless_random is safe here
       because a raw caller should also pass target. *)
    "XREAD", rand_r;
    "XREADGROUP", rand_w;
    "XREVRANGE", rk 1;
    "XSETID", wk 1;
    "XTRIM", wk 1;

    (* ---------- bitmaps ---------- *)
    "BITCOUNT", rk 1;
    "BITFIELD", wk 1;
    "BITFIELD_RO", rk 1;
    "BITOP", wk 2;           (* BITOP OP dest src1 src2 … — dest at 2 *)
    "BITPOS", rk 1;
    "GETBIT", rk 1;
    "SETBIT", wk 1;

    (* ---------- HyperLogLog ---------- *)
    "PFADD", wk 1;
    "PFCOUNT", mr 1;
    "PFMERGE", mw 1;

    (* ---------- geo ---------- *)
    "GEOADD", wk 1;
    "GEODIST", rk 1;
    "GEOHASH", rk 1;
    "GEOPOS", rk 1;
    "GEORADIUS", wk 1;
    "GEORADIUS_RO", rk 1;
    "GEORADIUSBYMEMBER", wk 1;
    "GEORADIUSBYMEMBER_RO", rk 1;
    "GEOSEARCH", rk 1;
    "GEOSEARCHSTORE", mw 1;  (* dest src … *)

    (* ---------- scripting ---------- *)
    "EVAL", Single_key { key_index = 3; readonly = false };
    "EVALSHA", Single_key { key_index = 3; readonly = false };
    "EVAL_RO", Single_key { key_index = 3; readonly = true };
    "EVALSHA_RO", Single_key { key_index = 3; readonly = true };
    "FCALL", Single_key { key_index = 3; readonly = false };
    "FCALL_RO", Single_key { key_index = 3; readonly = true };

    (* ---------- transactions (connection-scoped) ---------- *)
    "MULTI", rand_w;
    "EXEC", rand_w;
    "DISCARD", rand_w;
    "WATCH", mr 1;          (* slot of first key; all keys same slot *)
    "UNWATCH", rand_w;

    (* ---------- pub/sub (connection-scoped subscribe channels) ---------- *)
    "SUBSCRIBE", rand_w;
    "UNSUBSCRIBE", rand_w;
    "PSUBSCRIBE", rand_w;
    "PUNSUBSCRIBE", rand_w;
    "PUBLISH", rand_w;      (* cluster-wide broadcast on any node *)
    "SPUBLISH", rand_w;     (* router will compute slot from channel *)
    "SSUBSCRIBE", rand_w;
    "SUNSUBSCRIBE", rand_w;

    (* ---------- iteration / admin ---------- *)
    "SCAN", rand_r;
    "KEYS", Fan_all_nodes;

    (* ---------- server / info ---------- *)
    "BGREWRITEAOF", Fan_primaries;
    "BGSAVE", Fan_primaries;
    "COMMAND", rand_r;
    "DBSIZE", Fan_primaries;
    "DEBUG", rand_w;
    "ECHO", rand_r;
    "FAILOVER", rand_w;
    "FLUSHALL", Fan_primaries;
    "FLUSHDB", Fan_primaries;
    "INFO", rand_r;
    "LASTSAVE", rand_r;
    "LATENCY", rand_r;
    "LOLWUT", rand_r;
    "MEMORY", rand_r;
    "PING", rand_r;
    "REPLICAOF", rand_w;
    "RESET", rand_w;
    "ROLE", rand_r;
    "SAVE", Fan_primaries;
    "SHUTDOWN", Fan_primaries;
    "SLAVEOF", rand_w;
    "SLOWLOG", rand_r;
    "SWAPDB", rand_w;
    "TIME", rand_r;
    "WAIT", Fan_primaries;
    "WAITAOF", Fan_primaries;

    (* ---------- connection ---------- *)
    "AUTH", rand_w;
    "HELLO", rand_w;
    "QUIT", rand_w;
    "SELECT", rand_w;
  ]

let upper s = String.uppercase_ascii s

let one_word_tbl =
  let h = Hashtbl.create (List.length one_word) in
  List.iter (fun (k, v) -> Hashtbl.replace h k v) one_word;
  h

let two_word_tbl =
  let h = Hashtbl.create (List.length two_word) in
  List.iter (fun (a, b, v) -> Hashtbl.replace h (a, b) v) two_word;
  h

let default = Keyless_random { readonly = false }

let lookup args =
  if Array.length args = 0 then default
  else
    let cmd = upper args.(0) in
    if Array.length args >= 2 then
      let sub = upper args.(1) in
      match Hashtbl.find_opt two_word_tbl (cmd, sub) with
      | Some v -> v
      | None ->
          (match Hashtbl.find_opt one_word_tbl cmd with
           | Some v -> v
           | None -> default)
    else
      match Hashtbl.find_opt one_word_tbl cmd with
      | Some v -> v
      | None -> default

let target_and_rf rf args =
  match lookup args with
  | Single_key { key_index; readonly } ->
      if key_index >= Array.length args then
        Some (Router.Target.Random, rf)
      else
        let effective_rf =
          if readonly then rf else Router.Read_from.Primary
        in
        Some (Router.Target.By_slot (Slot.of_key args.(key_index)),
              effective_rf)
  | Multi_key { first_key_index; readonly } ->
      if first_key_index >= Array.length args then
        Some (Router.Target.Random, rf)
      else
        let effective_rf =
          if readonly then rf else Router.Read_from.Primary
        in
        Some (Router.Target.By_slot
                (Slot.of_key args.(first_key_index)),
              effective_rf)
  | Keyless_random { readonly } ->
      let effective_rf =
        if readonly then rf else Router.Read_from.Primary
      in
      Some (Router.Target.Random, effective_rf)
  | Fan_primaries | Fan_all_nodes -> None

(* Table size exposed for visibility/debug. *)
let command_count () =
  Hashtbl.length one_word_tbl + Hashtbl.length two_word_tbl
