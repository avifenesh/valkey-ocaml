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

(* Commands whose routing depends on a subcommand (args[1]). The
   caller uppercases the name before looking it up. *)
let two_word =
  [
    "SCRIPT", "LOAD", Fan_primaries;
    "SCRIPT", "FLUSH", Fan_primaries;
    "SCRIPT", "EXISTS", Fan_primaries;
    "FUNCTION", "LOAD", Fan_primaries;
    "FUNCTION", "DELETE", Fan_primaries;
    "FUNCTION", "FLUSH", Fan_primaries;
    "CONFIG", "SET", Fan_primaries;
    "CONFIG", "RESETSTAT", Fan_primaries;
    "CONFIG", "REWRITE", Fan_primaries;
    "CONFIG", "GET", Keyless_random { readonly = true };
    "CLIENT", "LIST", Fan_all_nodes;
    "CLIENT", "ID", Keyless_random { readonly = true };
    "CLIENT", "GETNAME", Keyless_random { readonly = true };
    "CLIENT", "SETNAME", Keyless_random { readonly = false };
    "CLIENT", "NO-EVICT", Keyless_random { readonly = false };
    "CLIENT", "NO-TOUCH", Keyless_random { readonly = false };
    "CLIENT", "TRACKING", Keyless_random { readonly = false };
    "CLUSTER", "NODES", Fan_all_nodes;
    "CLUSTER", "SHARDS", Keyless_random { readonly = true };
    "CLUSTER", "INFO", Keyless_random { readonly = true };
    "CLUSTER", "MYID", Keyless_random { readonly = true };
    "CLUSTER", "COUNTKEYSINSLOT", Keyless_random { readonly = true };
    "XINFO", "STREAM", Single_key { key_index = 2; readonly = true };
    "XINFO", "GROUPS", Single_key { key_index = 2; readonly = true };
    "XINFO", "CONSUMERS", Single_key { key_index = 2; readonly = true };
    "OBJECT", "ENCODING", Single_key { key_index = 2; readonly = true };
    "OBJECT", "REFCOUNT", Single_key { key_index = 2; readonly = true };
    "OBJECT", "IDLETIME", Single_key { key_index = 2; readonly = true };
    "OBJECT", "FREQ", Single_key { key_index = 2; readonly = true };
    "DEBUG", "OBJECT", Single_key { key_index = 2; readonly = true };
  ]

(* Single-word commands. Alphabetical within groups. *)
let one_word : (string * t) list =
  [
    (* strings / generic keys *)
    "GET", Single_key { key_index = 1; readonly = true };
    "GETDEL", Single_key { key_index = 1; readonly = false };
    "GETEX", Single_key { key_index = 1; readonly = false };
    "GETRANGE", Single_key { key_index = 1; readonly = true };
    "GETSET", Single_key { key_index = 1; readonly = false };
    "SET", Single_key { key_index = 1; readonly = false };
    "SETEX", Single_key { key_index = 1; readonly = false };
    "SETNX", Single_key { key_index = 1; readonly = false };
    "PSETEX", Single_key { key_index = 1; readonly = false };
    "APPEND", Single_key { key_index = 1; readonly = false };
    "STRLEN", Single_key { key_index = 1; readonly = true };
    "INCR", Single_key { key_index = 1; readonly = false };
    "INCRBY", Single_key { key_index = 1; readonly = false };
    "INCRBYFLOAT", Single_key { key_index = 1; readonly = false };
    "DECR", Single_key { key_index = 1; readonly = false };
    "DECRBY", Single_key { key_index = 1; readonly = false };
    "DELIFEQ", Single_key { key_index = 1; readonly = false };
    "EXPIRE", Single_key { key_index = 1; readonly = false };
    "PEXPIRE", Single_key { key_index = 1; readonly = false };
    "EXPIREAT", Single_key { key_index = 1; readonly = false };
    "PEXPIREAT", Single_key { key_index = 1; readonly = false };
    "EXPIRETIME", Single_key { key_index = 1; readonly = true };
    "PEXPIRETIME", Single_key { key_index = 1; readonly = true };
    "PERSIST", Single_key { key_index = 1; readonly = false };
    "TTL", Single_key { key_index = 1; readonly = true };
    "PTTL", Single_key { key_index = 1; readonly = true };
    "TYPE", Single_key { key_index = 1; readonly = true };
    "DUMP", Single_key { key_index = 1; readonly = true };
    "RESTORE", Single_key { key_index = 1; readonly = false };
    "DEL", Multi_key { first_key_index = 1; readonly = false };
    "UNLINK", Multi_key { first_key_index = 1; readonly = false };
    "EXISTS", Multi_key { first_key_index = 1; readonly = true };
    "MGET", Multi_key { first_key_index = 1; readonly = true };
    "MSET", Multi_key { first_key_index = 1; readonly = false };
    "MSETNX", Multi_key { first_key_index = 1; readonly = false };
    "RENAME", Multi_key { first_key_index = 1; readonly = false };
    "RENAMENX", Multi_key { first_key_index = 1; readonly = false };
    "COPY", Multi_key { first_key_index = 1; readonly = false };

    (* hashes *)
    "HGET", Single_key { key_index = 1; readonly = true };
    "HMGET", Single_key { key_index = 1; readonly = true };
    "HSET", Single_key { key_index = 1; readonly = false };
    "HSETNX", Single_key { key_index = 1; readonly = false };
    "HSETEX", Single_key { key_index = 1; readonly = false };
    "HGETEX", Single_key { key_index = 1; readonly = false };
    "HGETDEL", Single_key { key_index = 1; readonly = false };
    "HDEL", Single_key { key_index = 1; readonly = false };
    "HEXISTS", Single_key { key_index = 1; readonly = true };
    "HGETALL", Single_key { key_index = 1; readonly = true };
    "HKEYS", Single_key { key_index = 1; readonly = true };
    "HVALS", Single_key { key_index = 1; readonly = true };
    "HLEN", Single_key { key_index = 1; readonly = true };
    "HSTRLEN", Single_key { key_index = 1; readonly = true };
    "HINCRBY", Single_key { key_index = 1; readonly = false };
    "HINCRBYFLOAT", Single_key { key_index = 1; readonly = false };
    "HEXPIRE", Single_key { key_index = 1; readonly = false };
    "HPEXPIRE", Single_key { key_index = 1; readonly = false };
    "HEXPIREAT", Single_key { key_index = 1; readonly = false };
    "HPEXPIREAT", Single_key { key_index = 1; readonly = false };
    "HPERSIST", Single_key { key_index = 1; readonly = false };
    "HTTL", Single_key { key_index = 1; readonly = true };
    "HPTTL", Single_key { key_index = 1; readonly = true };
    "HEXPIRETIME", Single_key { key_index = 1; readonly = true };
    "HPEXPIRETIME", Single_key { key_index = 1; readonly = true };

    (* sets *)
    "SADD", Single_key { key_index = 1; readonly = false };
    "SREM", Single_key { key_index = 1; readonly = false };
    "SMEMBERS", Single_key { key_index = 1; readonly = true };
    "SISMEMBER", Single_key { key_index = 1; readonly = true };
    "SMISMEMBER", Single_key { key_index = 1; readonly = true };
    "SCARD", Single_key { key_index = 1; readonly = true };
    "SPOP", Single_key { key_index = 1; readonly = false };
    "SRANDMEMBER", Single_key { key_index = 1; readonly = true };

    (* lists *)
    "LPUSH", Single_key { key_index = 1; readonly = false };
    "RPUSH", Single_key { key_index = 1; readonly = false };
    "LPUSHX", Single_key { key_index = 1; readonly = false };
    "RPUSHX", Single_key { key_index = 1; readonly = false };
    "LPOP", Single_key { key_index = 1; readonly = false };
    "RPOP", Single_key { key_index = 1; readonly = false };
    "LRANGE", Single_key { key_index = 1; readonly = true };
    "LLEN", Single_key { key_index = 1; readonly = true };
    "LINDEX", Single_key { key_index = 1; readonly = true };
    "LPOS", Single_key { key_index = 1; readonly = true };
    "LSET", Single_key { key_index = 1; readonly = false };
    "LREM", Single_key { key_index = 1; readonly = false };
    "LTRIM", Single_key { key_index = 1; readonly = false };
    "LINSERT", Single_key { key_index = 1; readonly = false };
    "LMOVE", Multi_key { first_key_index = 1; readonly = false };
    "BLPOP", Multi_key { first_key_index = 1; readonly = false };
    "BRPOP", Multi_key { first_key_index = 1; readonly = false };
    "BLMOVE", Multi_key { first_key_index = 1; readonly = false };

    (* sorted sets *)
    "ZADD", Single_key { key_index = 1; readonly = false };
    "ZREM", Single_key { key_index = 1; readonly = false };
    "ZRANGE", Single_key { key_index = 1; readonly = true };
    "ZRANGEBYSCORE", Single_key { key_index = 1; readonly = true };
    "ZRANGEBYLEX", Single_key { key_index = 1; readonly = true };
    "ZREVRANGE", Single_key { key_index = 1; readonly = true };
    "ZREVRANGEBYSCORE", Single_key { key_index = 1; readonly = true };
    "ZCARD", Single_key { key_index = 1; readonly = true };
    "ZSCORE", Single_key { key_index = 1; readonly = true };
    "ZMSCORE", Single_key { key_index = 1; readonly = true };
    "ZRANK", Single_key { key_index = 1; readonly = true };
    "ZREVRANK", Single_key { key_index = 1; readonly = true };
    "ZCOUNT", Single_key { key_index = 1; readonly = true };
    "ZLEXCOUNT", Single_key { key_index = 1; readonly = true };
    "ZINCRBY", Single_key { key_index = 1; readonly = false };
    "ZPOPMIN", Single_key { key_index = 1; readonly = false };
    "ZPOPMAX", Single_key { key_index = 1; readonly = false };
    "ZREMRANGEBYRANK", Single_key { key_index = 1; readonly = false };
    "ZREMRANGEBYSCORE", Single_key { key_index = 1; readonly = false };
    "ZREMRANGEBYLEX", Single_key { key_index = 1; readonly = false };

    (* streams *)
    "XADD", Single_key { key_index = 1; readonly = false };
    "XDEL", Single_key { key_index = 1; readonly = false };
    "XLEN", Single_key { key_index = 1; readonly = true };
    "XRANGE", Single_key { key_index = 1; readonly = true };
    "XREVRANGE", Single_key { key_index = 1; readonly = true };
    "XTRIM", Single_key { key_index = 1; readonly = false };
    "XACK", Single_key { key_index = 1; readonly = false };
    "XCLAIM", Single_key { key_index = 1; readonly = false };
    "XAUTOCLAIM", Single_key { key_index = 1; readonly = false };
    "XPENDING", Single_key { key_index = 1; readonly = true };
    "XGROUP", Single_key { key_index = 2; readonly = false };
    (* XREAD / XREADGROUP have STREAMS keyword then alternating
       keys/ids; leaving default Keyless_random and letting typed
       wrappers override is simpler than parsing args here. *)

    (* scripting *)
    "EVAL", Single_key { key_index = 3; readonly = false };
    "EVALSHA", Single_key { key_index = 3; readonly = false };
    "EVAL_RO", Single_key { key_index = 3; readonly = true };
    "EVALSHA_RO", Single_key { key_index = 3; readonly = true };
    "FCALL", Single_key { key_index = 3; readonly = false };
    "FCALL_RO", Single_key { key_index = 3; readonly = true };

    (* iteration / admin *)
    "SCAN", Keyless_random { readonly = true };
    "HSCAN", Single_key { key_index = 1; readonly = true };
    "SSCAN", Single_key { key_index = 1; readonly = true };
    "ZSCAN", Single_key { key_index = 1; readonly = true };
    "KEYS", Fan_all_nodes;
    "DBSIZE", Fan_primaries;
    "FLUSHALL", Fan_primaries;
    "FLUSHDB", Fan_primaries;
    "RANDOMKEY", Keyless_random { readonly = true };
    "PING", Keyless_random { readonly = true };
    "ECHO", Keyless_random { readonly = true };
    "TIME", Keyless_random { readonly = true };
    "WAIT", Fan_primaries;
    "INFO", Keyless_random { readonly = true };
    "ROLE", Keyless_random { readonly = true };
    "COMMAND", Keyless_random { readonly = true };
    "MEMORY", Keyless_random { readonly = true };
    "LATENCY", Keyless_random { readonly = true };
    "SLOWLOG", Keyless_random { readonly = true };
  ]

let upper s = String.uppercase_ascii s

(* Build hashtables once; lookups are O(1). *)
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
