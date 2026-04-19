module Read_from = Router.Read_from
module Target = Router.Target

module Config = struct
  type t = {
    connection : Connection.Config.t;
    client_az : string option;
    read_from : Read_from.t;
  }
  let default = {
    connection = Connection.Config.default;
    client_az = None;
    read_from = Read_from.default;
  }
end

type t = {
  router : Router.t;
  config : Config.t;
  loaded_shas : (string, unit) Hashtbl.t;
  loaded_shas_mutex : Eio.Mutex.t;
}
[@@warning "-69"]

let connect ~sw ~net ~clock ?domain_mgr ?(config = Config.default) ~host ~port () =
  (* Standalone is a degenerate one-shard cluster: synthesise a topology,
     stash the one Connection in a Node_pool, and dispatch through the
     same Cluster_router code path that handles real clusters. *)
  let connection =
    Connection.connect ~sw ~net ~clock ?domain_mgr
      ~config:config.connection ~host ~port ()
  in
  let tls_port =
    if config.connection.tls <> None then Some port else None
  in
  let topology = Topology.single_primary ~host ~port ?tls_port () in
  let pool = Node_pool.create () in
  Node_pool.add pool Topology.standalone_node_id connection;
  let router =
    Cluster_router.from_pool_and_topology ~clock ~pool ~topology ()
  in
  { router; config;
    loaded_shas = Hashtbl.create 16;
    loaded_shas_mutex = Eio.Mutex.create () }

let from_router ~config router =
  { router; config;
    loaded_shas = Hashtbl.create 16;
    loaded_shas_mutex = Eio.Mutex.create () }

let close t = Router.close t.router

let raw_connection t =
  match Router.primary_connection t.router with
  | Some c -> c
  | None ->
      invalid_arg
        "Client.raw_connection: no primary connection available \
         (multi-node router)"

let exec ?timeout ?target ?read_from t args =
  let user_rf = Option.value read_from ~default:t.config.read_from in
  match target with
  | Some target ->
      (* Explicit override — trust the caller; do not second-guess their
         target or their Read_from. *)
      Router.exec ?timeout t.router target user_rf args
  | None ->
      (match Command_spec.target_and_rf user_rf args with
       | Some (target, effective_rf) ->
           Router.exec ?timeout t.router target effective_rf args
       | None ->
           (* Fan-out command routed through single-reply exec: send to
              a random node. This preserves pre-spec behavior; callers
              who want true fan-out should use [exec_multi]. *)
           Router.exec ?timeout t.router Target.Random user_rf args)

let exec_multi ?timeout ?fan t args =
  let fan =
    match fan with
    | Some f -> f
    | None ->
        (match Command_spec.lookup args with
         | Fan_all_nodes -> Router.Fan_target.All_nodes
         | Fan_primaries -> Router.Fan_target.All_primaries
         | _ -> Router.Fan_target.All_primaries)
  in
  Router.exec_multi ?timeout t.router fan args

let protocol_violation cmd v =
  Connection.Error.Protocol_violation
    (Format.asprintf "%s: unexpected reply %a" cmd Resp3.pp v)

(* ---------- helpers ---------- *)

type set_cond = Set_nx | Set_xx

type set_ttl =
  | Set_ex_seconds of int
  | Set_px_millis of int
  | Set_exat_unix_seconds of int
  | Set_pxat_unix_millis of int
  | Set_keepttl

let set_cond_args = function
  | Some Set_nx -> [ "NX" ]
  | Some Set_xx -> [ "XX" ]
  | None -> []

let set_ttl_args = function
  | None -> []
  | Some (Set_ex_seconds n) -> [ "EX"; string_of_int n ]
  | Some (Set_px_millis n) -> [ "PX"; string_of_int n ]
  | Some (Set_exat_unix_seconds n) -> [ "EXAT"; string_of_int n ]
  | Some (Set_pxat_unix_millis n) -> [ "PXAT"; string_of_int n ]
  | Some Set_keepttl -> [ "KEEPTTL" ]

let build_set_args ?cond ?ttl ?ifeq ~get key value =
  let base = [ "SET"; key; value ] in
  let cond_a = set_cond_args cond in
  let ttl_a = set_ttl_args ttl in
  let ifeq_a = match ifeq with None -> [] | Some v -> [ "IFEQ"; v ] in
  let get_a = if get then [ "GET" ] else [] in
  Array.of_list (base @ cond_a @ ttl_a @ ifeq_a @ get_a)

let bool_from_integer cmd = function
  | Ok (Resp3.Integer 0L) -> Ok false
  | Ok (Resp3.Integer 1L) -> Ok true
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let string_option_of_reply cmd = function
  | Resp3.Null -> Ok None
  | Resp3.Bulk_string s -> Ok (Some s)
  | Resp3.Simple_string s -> Ok (Some s)
  | v -> Error (protocol_violation cmd v)

(* ---------- strings / keys ---------- *)

let get ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "GET"; key |] with
  | Error e -> Error e
  | Ok v -> string_option_of_reply "GET" v

let set ?timeout ?cond ?ttl ?ifeq t key value =
  let args = build_set_args ?cond ?ttl ?ifeq ~get:false key value in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok true
  | Ok Resp3.Null -> Ok false
  | Ok v -> Error (protocol_violation "SET" v)

let set_and_get ?timeout ?cond ?ttl ?ifeq t key value =
  let args = build_set_args ?cond ?ttl ?ifeq ~get:true key value in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> string_option_of_reply "SET ... GET" v

let del ?timeout t keys =
  let args = Array.of_list ("DEL" :: keys) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "DEL" v)

let unlink ?timeout t keys =
  let args = Array.of_list ("UNLINK" :: keys) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "UNLINK" v)

let delifeq ?timeout t key ~value =
  bool_from_integer "DELIFEQ"
    (exec ?timeout t [| "DELIFEQ"; key; value |])

let setex ?timeout t key ~seconds value =
  match
    exec ?timeout t [| "SETEX"; key; string_of_int seconds; value |]
  with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok ()
  | Ok v -> Error (protocol_violation "SETEX" v)

let setnx ?timeout t key value =
  bool_from_integer "SETNX"
    (exec ?timeout t [| "SETNX"; key; value |])

let mget ?timeout ?read_from t keys =
  let args = Array.of_list ("MGET" :: keys) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec convert acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Null :: rest -> convert (None :: acc) rest
        | Resp3.Bulk_string s :: rest -> convert (Some s :: acc) rest
        | other :: _ -> Error (protocol_violation "MGET element" other)
      in
      convert [] items
  | Ok v -> Error (protocol_violation "MGET" v)

type value_type =
  | T_none
  | T_string
  | T_list
  | T_hash
  | T_set
  | T_zset
  | T_stream
  | T_other of string

let value_type_of_string = function
  | "none" -> T_none
  | "string" -> T_string
  | "list" -> T_list
  | "hash" -> T_hash
  | "set" -> T_set
  | "zset" -> T_zset
  | "stream" -> T_stream
  | other -> T_other other

let type_of ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "TYPE"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Simple_string s) -> Ok (value_type_of_string s)
  | Ok (Resp3.Bulk_string s) -> Ok (value_type_of_string s)
  | Ok v -> Error (protocol_violation "TYPE" v)

(* ---------- TTL family ---------- *)

type expiry_state =
  | Absent
  | Persistent
  | Expires_in of int

let expiry_from_integer = function
  | -2L -> Absent
  | -1L -> Persistent
  | n when n >= 0L -> Expires_in (Int64.to_int n)
  | n -> Expires_in (Int64.to_int n)   (* future-proof fallback *)

let ttl ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "TTL"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (expiry_from_integer n)
  | Ok v -> Error (protocol_violation "TTL" v)

let pttl ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "PTTL"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (expiry_from_integer n)
  | Ok v -> Error (protocol_violation "PTTL" v)

let expire ?timeout t key ~seconds =
  bool_from_integer "EXPIRE"
    (exec ?timeout t [| "EXPIRE"; key; string_of_int seconds |])

let pexpire ?timeout t key ~millis =
  bool_from_integer "PEXPIRE"
    (exec ?timeout t [| "PEXPIRE"; key; string_of_int millis |])

let pexpireat ?timeout t key ~at_unix_millis =
  bool_from_integer "PEXPIREAT"
    (exec ?timeout t [| "PEXPIREAT"; key; string_of_int at_unix_millis |])

(* ---------- counters ---------- *)

let int64_of_reply cmd = function
  | Ok (Resp3.Integer n) -> Ok n
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let incr ?timeout t key =
  int64_of_reply "INCR" (exec ?timeout t [| "INCR"; key |])

let incrby ?timeout t key delta =
  int64_of_reply "INCRBY"
    (exec ?timeout t [| "INCRBY"; key; string_of_int delta |])

(* ---------- hashes ---------- *)

let hget ?timeout ?read_from t key field =
  match exec ?timeout ?read_from t [| "HGET"; key; field |] with
  | Error e -> Error e
  | Ok v -> string_option_of_reply "HGET" v

let hset ?timeout t key fvs =
  let pairs = List.concat_map (fun (f, v) -> [ f; v ]) fvs in
  let args = Array.of_list ("HSET" :: key :: pairs) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "HSET" v)

let hmget ?timeout ?read_from t key fields =
  let args = Array.of_list ("HMGET" :: key :: fields) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec convert acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Null :: rest -> convert (None :: acc) rest
        | Resp3.Bulk_string s :: rest -> convert (Some s :: acc) rest
        | other :: _ -> Error (protocol_violation "HMGET element" other)
      in
      convert [] items
  | Ok v -> Error (protocol_violation "HMGET" v)

let extract_string cmd = function
  | Resp3.Bulk_string s -> Ok s
  | Resp3.Simple_string s -> Ok s
  | v -> Error (protocol_violation cmd v)

let hgetall ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "HGETALL"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Map kvs) ->
      let rec convert acc = function
        | [] -> Ok (List.rev acc)
        | (k, v) :: rest ->
            (match extract_string "HGETALL key" k with
             | Error e -> Error e
             | Ok k' ->
                 (match extract_string "HGETALL value" v with
                  | Error e -> Error e
                  | Ok v' -> convert ((k', v') :: acc) rest))
      in
      convert [] kvs
  | Ok (Resp3.Array items) ->
      (* Defensive: some proxies translate RESP3 Map into flat Array. *)
      let rec convert acc = function
        | [] -> Ok (List.rev acc)
        | [ _ ] -> Error (protocol_violation "HGETALL" (Resp3.Array items))
        | k :: v :: rest ->
            (match extract_string "HGETALL key" k with
             | Error e -> Error e
             | Ok k' ->
                 (match extract_string "HGETALL value" v with
                  | Error e -> Error e
                  | Ok v' -> convert ((k', v') :: acc) rest))
      in
      convert [] items
  | Ok v -> Error (protocol_violation "HGETALL" v)

let hincrby ?timeout t key field delta =
  int64_of_reply "HINCRBY"
    (exec ?timeout t [| "HINCRBY"; key; field; string_of_int delta |])

(* ---------- hash field TTL (Valkey 9+) ---------- *)

type hexpire_cond = H_nx | H_xx | H_gt | H_lt

let string_of_hexpire_cond = function
  | H_nx -> "NX"
  | H_xx -> "XX"
  | H_gt -> "GT"
  | H_lt -> "LT"

type field_ttl_set =
  | Hfield_missing
  | Hfield_condition_failed
  | Hfield_ttl_set
  | Hfield_expired_now

let decode_field_ttl_set = function
  | -2L -> Hfield_missing
  | 0L -> Hfield_condition_failed
  | 1L -> Hfield_ttl_set
  | 2L -> Hfield_expired_now
  | n ->
      (* Future-proof: unknown code → report as condition_failed is wrong;
         raise as protocol violation at the caller layer. *)
      ignore n; Hfield_condition_failed

let ints_per_field cmd decoder = function
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Integer n :: rest -> loop (decoder n :: acc) rest
        | other :: _ -> Error (protocol_violation cmd other)
      in
      loop [] items
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let hexpire_args cmd ?cond key unit_arg fields =
  let cond_args = match cond with
    | None -> []
    | Some c -> [ string_of_hexpire_cond c ]
  in
  let nfields = string_of_int (List.length fields) in
  Array.of_list
    ([ cmd; key; unit_arg ] @ cond_args @ [ "FIELDS"; nfields ] @ fields)

let hexpire ?timeout ?cond t key ~seconds fields =
  let args = hexpire_args "HEXPIRE" ?cond key (string_of_int seconds) fields in
  ints_per_field "HEXPIRE" decode_field_ttl_set (exec ?timeout t args)

let hpexpire ?timeout ?cond t key ~millis fields =
  let args = hexpire_args "HPEXPIRE" ?cond key (string_of_int millis) fields in
  ints_per_field "HPEXPIRE" decode_field_ttl_set (exec ?timeout t args)

let hexpireat ?timeout ?cond t key ~at_unix_seconds fields =
  let args = hexpire_args "HEXPIREAT" ?cond key (string_of_int at_unix_seconds) fields in
  ints_per_field "HEXPIREAT" decode_field_ttl_set (exec ?timeout t args)

let hpexpireat ?timeout ?cond t key ~at_unix_millis fields =
  let args = hexpire_args "HPEXPIREAT" ?cond key (string_of_int at_unix_millis) fields in
  ints_per_field "HPEXPIREAT" decode_field_ttl_set (exec ?timeout t args)

let decode_expiry = function
  | -2L -> Absent
  | -1L -> Persistent
  | n -> Expires_in (Int64.to_int n)

let httl_like cmd ?timeout ?read_from t key fields =
  let nfields = string_of_int (List.length fields) in
  let args = Array.of_list ([ cmd; key; "FIELDS"; nfields ] @ fields) in
  ints_per_field cmd decode_expiry (exec ?timeout ?read_from t args)

let httl ?timeout ?read_from t key fields =
  httl_like "HTTL" ?timeout ?read_from t key fields

let hpttl ?timeout ?read_from t key fields =
  httl_like "HPTTL" ?timeout ?read_from t key fields

let hexpiretime ?timeout ?read_from t key fields =
  httl_like "HEXPIRETIME" ?timeout ?read_from t key fields

let hpexpiretime ?timeout ?read_from t key fields =
  httl_like "HPEXPIRETIME" ?timeout ?read_from t key fields

type field_persist =
  | Persist_field_missing
  | Persist_had_no_ttl
  | Persist_ttl_removed

let decode_persist = function
  | -2L -> Persist_field_missing
  | -1L -> Persist_had_no_ttl
  | 1L -> Persist_ttl_removed
  | _ -> Persist_had_no_ttl   (* future-proof fallback *)

let hpersist ?timeout t key fields =
  let nfields = string_of_int (List.length fields) in
  let args =
    Array.of_list ([ "HPERSIST"; key; "FIELDS"; nfields ] @ fields)
  in
  ints_per_field "HPERSIST" decode_persist (exec ?timeout t args)

type hgetex_ttl =
  | Hge_no_change
  | Hge_ex_seconds of int
  | Hge_px_millis of int
  | Hge_exat_unix_seconds of int
  | Hge_pxat_unix_millis of int
  | Hge_persist

let hgetex ?timeout t key ~ttl fields =
  let ttl_args = match ttl with
    | Hge_no_change -> []
    | Hge_ex_seconds n -> [ "EX"; string_of_int n ]
    | Hge_px_millis n -> [ "PX"; string_of_int n ]
    | Hge_exat_unix_seconds n -> [ "EXAT"; string_of_int n ]
    | Hge_pxat_unix_millis n -> [ "PXAT"; string_of_int n ]
    | Hge_persist -> [ "PERSIST" ]
  in
  let nfields = string_of_int (List.length fields) in
  let args =
    Array.of_list
      ([ "HGETEX"; key ] @ ttl_args @ [ "FIELDS"; nfields ] @ fields)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Null :: rest -> loop (None :: acc) rest
        | Resp3.Bulk_string s :: rest -> loop (Some s :: acc) rest
        | other :: _ -> Error (protocol_violation "HGETEX element" other)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "HGETEX" v)

type hsetex_ttl =
  | Hse_ex_seconds of int
  | Hse_px_millis of int
  | Hse_exat_unix_seconds of int
  | Hse_pxat_unix_millis of int
  | Hse_keepttl

let hsetex_ttl_args = function
  | None -> []
  | Some (Hse_ex_seconds n) -> [ "EX"; string_of_int n ]
  | Some (Hse_px_millis n) -> [ "PX"; string_of_int n ]
  | Some (Hse_exat_unix_seconds n) -> [ "EXAT"; string_of_int n ]
  | Some (Hse_pxat_unix_millis n) -> [ "PXAT"; string_of_int n ]
  | Some Hse_keepttl -> [ "KEEPTTL" ]

let hsetex ?timeout ?ttl t key fvs =
  let ttl_a = hsetex_ttl_args ttl in
  let nfields = string_of_int (List.length fvs) in
  let pairs = List.concat_map (fun (f, v) -> [ f; v ]) fvs in
  let args =
    Array.of_list
      ([ "HSETEX"; key ] @ ttl_a @ [ "FIELDS"; nfields ] @ pairs)
  in
  bool_from_integer "HSETEX" (exec ?timeout t args)

(* ---------- sets ---------- *)

let sadd ?timeout t key members =
  let args = Array.of_list ("SADD" :: key :: members) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "SADD" v)

let scard ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "SCARD"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "SCARD" v)

let strings_of_resp3_collection cmd items =
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | Resp3.Bulk_string s :: rest -> loop (s :: acc) rest
    | Resp3.Simple_string s :: rest -> loop (s :: acc) rest
    | other :: _ -> Error (protocol_violation cmd other)
  in
  loop [] items

let smembers ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "SMEMBERS"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Set xs) -> strings_of_resp3_collection "SMEMBERS" xs
  | Ok (Resp3.Array xs) -> strings_of_resp3_collection "SMEMBERS" xs
  | Ok v -> Error (protocol_violation "SMEMBERS" v)

let sismember ?timeout ?read_from t key member =
  bool_from_integer "SISMEMBER"
    (exec ?timeout ?read_from t [| "SISMEMBER"; key; member |])

(* ---------- lists ---------- *)

let int_of_push_reply cmd = function
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let lpush ?timeout t key elements =
  let args = Array.of_list ("LPUSH" :: key :: elements) in
  int_of_push_reply "LPUSH" (exec ?timeout t args)

let rpush ?timeout t key elements =
  let args = Array.of_list ("RPUSH" :: key :: elements) in
  int_of_push_reply "RPUSH" (exec ?timeout t args)

let pop_single cmd ?timeout t key =
  match exec ?timeout t [| cmd; key |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation cmd v)

let pop_many cmd ?timeout t key count =
  match exec ?timeout t [| cmd; key; string_of_int count |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok []
  | Ok (Resp3.Array items) -> strings_of_resp3_collection cmd items
  | Ok v -> Error (protocol_violation cmd v)

let lpop ?timeout t key = pop_single "LPOP" ?timeout t key
let rpop ?timeout t key = pop_single "RPOP" ?timeout t key
let lpop_n ?timeout t key n = pop_many "LPOP" ?timeout t key n
let rpop_n ?timeout t key n = pop_many "RPOP" ?timeout t key n

let lrange ?timeout ?read_from t key ~start ~stop =
  match
    exec ?timeout ?read_from t
      [| "LRANGE"; key; string_of_int start; string_of_int stop |]
  with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> strings_of_resp3_collection "LRANGE" items
  | Ok v -> Error (protocol_violation "LRANGE" v)

let llen ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "LLEN"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "LLEN" v)

(* ---------- sorted sets ---------- *)

type score_bound =
  | Score of float
  | Score_excl of float
  | Score_neg_inf
  | Score_pos_inf

let score_bound_to_string = function
  | Score f -> Printf.sprintf "%g" f
  | Score_excl f -> Printf.sprintf "(%g" f
  | Score_neg_inf -> "-inf"
  | Score_pos_inf -> "+inf"

let zrange ?timeout ?read_from ?(rev = false) t key ~start ~stop =
  let base =
    [ "ZRANGE"; key; string_of_int start; string_of_int stop ]
  in
  let args = Array.of_list (if rev then base @ [ "REV" ] else base) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> strings_of_resp3_collection "ZRANGE" items
  | Ok v -> Error (protocol_violation "ZRANGE" v)

let zrangebyscore ?timeout ?read_from ?limit t key ~min ~max =
  let base =
    [ "ZRANGEBYSCORE"; key;
      score_bound_to_string min; score_bound_to_string max ]
  in
  let limit_a = match limit with
    | None -> []
    | Some (off, count) ->
        [ "LIMIT"; string_of_int off; string_of_int count ]
  in
  let args = Array.of_list (base @ limit_a) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      strings_of_resp3_collection "ZRANGEBYSCORE" items
  | Ok v -> Error (protocol_violation "ZRANGEBYSCORE" v)

let zremrangebyscore ?timeout t key ~min ~max =
  match
    exec ?timeout t
      [| "ZREMRANGEBYSCORE"; key;
         score_bound_to_string min; score_bound_to_string max |]
  with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "ZREMRANGEBYSCORE" v)

let zcard ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "ZCARD"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "ZCARD" v)

(* ---------- scripting ---------- *)

let eval_like cmd ?timeout t ~code ~keys ~args =
  let nkeys = string_of_int (List.length keys) in
  let req =
    Array.of_list ([ cmd; code; nkeys ] @ keys @ args)
  in
  exec ?timeout t req

let eval ?timeout t ~script ~keys ~args =
  eval_like "EVAL" ?timeout t ~code:script ~keys ~args

let evalsha ?timeout t ~sha ~keys ~args =
  eval_like "EVALSHA" ?timeout t ~code:sha ~keys ~args

let script_load ?timeout t script =
  match exec ?timeout t [| "SCRIPT"; "LOAD"; script |] with
  | Error e -> Error e
  | Ok (Resp3.Bulk_string s) -> Ok s
  | Ok (Resp3.Simple_string s) -> Ok s
  | Ok v -> Error (protocol_violation "SCRIPT LOAD" v)

let script_exists ?timeout ?read_from t shas =
  let args = Array.of_list ("SCRIPT" :: "EXISTS" :: shas) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Integer 0L :: rest -> loop (false :: acc) rest
        | Resp3.Integer 1L :: rest -> loop (true :: acc) rest
        | other :: _ -> Error (protocol_violation "SCRIPT EXISTS" other)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "SCRIPT EXISTS" v)

type script_flush_mode = Flush_sync | Flush_async

let script_flush ?timeout ?mode t =
  let tail = match mode with
    | None -> []
    | Some Flush_sync -> [ "SYNC" ]
    | Some Flush_async -> [ "ASYNC" ]
  in
  let args = Array.of_list ([ "SCRIPT"; "FLUSH" ] @ tail) in
  let result =
    match exec ?timeout t args with
    | Error e -> Error e
    | Ok (Resp3.Simple_string "OK") -> Ok ()
    | Ok v -> Error (protocol_violation "SCRIPT FLUSH" v)
  in
  (* SCRIPT FLUSH invalidates everything the server has loaded. Clear our
     view so future eval_script calls fall back to EVAL correctly. *)
  (match result with
   | Ok () ->
       Eio.Mutex.use_rw ~protect:true t.loaded_shas_mutex (fun () ->
           Hashtbl.clear t.loaded_shas)
   | Error _ -> ());
  result

module Script = struct
  type t = { source : string; sha : string }

  let create source =
    let sha =
      Digestif.SHA1.digest_string source |> Digestif.SHA1.to_hex
    in
    { source; sha }

  let source t = t.source
  let sha t = t.sha
end

let eval_script ?timeout t s ~keys ~args =
  let sha = Script.sha s in
  let known_loaded =
    Eio.Mutex.use_rw ~protect:true t.loaded_shas_mutex (fun () ->
        Hashtbl.mem t.loaded_shas sha)
  in
  let remember () =
    Eio.Mutex.use_rw ~protect:true t.loaded_shas_mutex (fun () ->
        Hashtbl.replace t.loaded_shas sha ())
  in
  let forget () =
    Eio.Mutex.use_rw ~protect:true t.loaded_shas_mutex (fun () ->
        Hashtbl.remove t.loaded_shas sha)
  in
  let do_eval () =
    match eval ?timeout t ~script:(Script.source s) ~keys ~args with
    | Ok v -> remember (); Ok v
    | Error e -> Error e
  in
  if not known_loaded then do_eval ()
  else
    match evalsha ?timeout t ~sha ~keys ~args with
    | Ok v -> Ok v
    | Error (Connection.Error.Server_error ve)
      when ve.code = "NOSCRIPT" ->
        forget ();
        do_eval ()
    | Error e -> Error e

let eval_cached ?timeout t ~script ~keys ~args =
  eval_script ?timeout t (Script.create script) ~keys ~args

(* ---------- iteration ---------- *)

type scan_page = { cursor : string; keys : string list }

let scan ?timeout ?read_from ?match_ ?count ?type_ t ~cursor =
  let match_a = match match_ with
    | None -> []
    | Some p -> [ "MATCH"; p ]
  in
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let type_a = match type_ with
    | None -> []
    | Some t -> [ "TYPE"; t ]
  in
  let args =
    Array.of_list ([ "SCAN"; cursor ] @ match_a @ count_a @ type_a)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array [ cursor_v; Resp3.Array items ]) ->
      (match extract_string "SCAN cursor" cursor_v with
       | Error e -> Error e
       | Ok cursor ->
           (match strings_of_resp3_collection "SCAN keys" items with
            | Error e -> Error e
            | Ok keys -> Ok { cursor; keys }))
  | Ok v -> Error (protocol_violation "SCAN" v)

let keys ?timeout ?read_from t pattern =
  match exec ?timeout ?read_from t [| "KEYS"; pattern |] with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> strings_of_resp3_collection "KEYS" items
  | Ok v -> Error (protocol_violation "KEYS" v)

(* ---------- streams ---------- *)

type stream_entry = { id : string; fields : (string * string) list }

let parse_fields cmd items =
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | [ _ ] -> Error (protocol_violation cmd (Resp3.Array items))
    | k :: v :: rest ->
        (match extract_string cmd k, extract_string cmd v with
         | Ok f, Ok vv -> loop ((f, vv) :: acc) rest
         | Error e, _ | _, Error e -> Error e)
  in
  loop [] items

let parse_entry cmd = function
  | Resp3.Array [ id_v; Resp3.Array field_items ] ->
      (match extract_string cmd id_v with
       | Error e -> Error e
       | Ok id ->
           (match parse_fields cmd field_items with
            | Ok fields -> Ok { id; fields }
            | Error e -> Error e))
  | Resp3.Array [ id_v; Resp3.Map kvs ] ->
      (match extract_string cmd id_v with
       | Error e -> Error e
       | Ok id ->
           let rec loop acc = function
             | [] -> Ok (List.rev acc)
             | (k, v) :: rest ->
                 (match extract_string cmd k, extract_string cmd v with
                  | Ok f, Ok vv -> loop ((f, vv) :: acc) rest
                  | Error e, _ | _, Error e -> Error e)
           in
           (match loop [] kvs with
            | Ok fields -> Ok { id; fields }
            | Error e -> Error e))
  | Resp3.Null -> Ok { id = ""; fields = [] }
  | v -> Error (protocol_violation cmd v)

let parse_entries cmd items =
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | e :: rest ->
        (match parse_entry cmd e with
         | Ok entry -> loop (entry :: acc) rest
         | Error err -> Error err)
  in
  loop [] items

type xadd_trim =
  | Xadd_maxlen of { approx : bool; threshold : int }
  | Xadd_minid of { approx : bool; threshold : string }

let xadd_trim_args = function
  | None -> []
  | Some (Xadd_maxlen { approx; threshold }) ->
      [ "MAXLEN"; (if approx then "~" else "="); string_of_int threshold ]
  | Some (Xadd_minid { approx; threshold }) ->
      [ "MINID"; (if approx then "~" else "="); threshold ]

let xadd ?timeout ?(id = "*") ?(nomkstream = false) ?trim t key fields =
  let nomk = if nomkstream then [ "NOMKSTREAM" ] else [] in
  let trim_a = xadd_trim_args trim in
  let pairs = List.concat_map (fun (f, v) -> [ f; v ]) fields in
  let args =
    Array.of_list
      ([ "XADD"; key ] @ nomk @ trim_a @ [ id ] @ pairs)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Bulk_string s) -> Ok s
  | Ok Resp3.Null ->
      (* NOMKSTREAM with a non-existing key returns nil *)
      Error
        (Connection.Error.Protocol_violation
           "XADD returned Null (NOMKSTREAM on missing stream)")
  | Ok v -> Error (protocol_violation "XADD" v)

let xlen ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "XLEN"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "XLEN" v)

let xrange_like cmd ?timeout ?read_from ?count t key a b =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let args = Array.of_list ([ cmd; key; a; b ] @ count_a) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> parse_entries cmd items
  | Ok v -> Error (protocol_violation cmd v)

let xrange ?timeout ?read_from ?count t key ~start ~end_ =
  xrange_like "XRANGE" ?timeout ?read_from ?count t key start end_

let xrevrange ?timeout ?read_from ?count t key ~end_ ~start =
  xrange_like "XREVRANGE" ?timeout ?read_from ?count t key end_ start

let xdel ?timeout t key ids =
  let args = Array.of_list ("XDEL" :: key :: ids) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "XDEL" v)

type xtrim_strategy =
  | Xtrim_maxlen of { approx : bool; threshold : int }
  | Xtrim_minid of { approx : bool; threshold : string }

let xtrim ?timeout t key strategy =
  let strat_args = match strategy with
    | Xtrim_maxlen { approx; threshold } ->
        [ "MAXLEN"; (if approx then "~" else "="); string_of_int threshold ]
    | Xtrim_minid { approx; threshold } ->
        [ "MINID"; (if approx then "~" else "="); threshold ]
  in
  let args = Array.of_list ([ "XTRIM"; key ] @ strat_args) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "XTRIM" v)

let parse_streams_reply cmd = function
  | Resp3.Null -> Ok []
  | Resp3.Array items ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Array [ name_v; Resp3.Array entries ] :: rest ->
            (match extract_string cmd name_v with
             | Error e -> Error e
             | Ok name ->
                 (match parse_entries cmd entries with
                  | Ok es -> loop ((name, es) :: acc) rest
                  | Error e -> Error e))
        | other :: _ -> Error (protocol_violation cmd other)
      in
      loop [] items
  | Resp3.Map kvs ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | (name_v, Resp3.Array entries) :: rest ->
            (match extract_string cmd name_v with
             | Error e -> Error e
             | Ok name ->
                 (match parse_entries cmd entries with
                  | Ok es -> loop ((name, es) :: acc) rest
                  | Error e -> Error e))
        | (_, other) :: _ -> Error (protocol_violation cmd other)
      in
      loop [] kvs
  | v -> Error (protocol_violation cmd v)

let xread ?timeout ?count t ~streams =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let keys = List.map fst streams in
  let ids = List.map snd streams in
  let args =
    Array.of_list (
      [ "XREAD" ] @ count_a @ [ "STREAMS" ] @ keys @ ids
    )
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> parse_streams_reply "XREAD" v

type xgroup_create_option =
  | Xgroup_mkstream
  | Xgroup_entries_read of int

let xgroup_create ?timeout ?(opts = []) t key ~group ~id =
  let opt_args =
    List.concat_map
      (function
        | Xgroup_mkstream -> [ "MKSTREAM" ]
        | Xgroup_entries_read n -> [ "ENTRIESREAD"; string_of_int n ])
      opts
  in
  let args =
    Array.of_list (
      [ "XGROUP"; "CREATE"; key; group; id ] @ opt_args
    )
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok ()
  | Ok v -> Error (protocol_violation "XGROUP CREATE" v)

let xgroup_destroy ?timeout t key ~group =
  bool_from_integer "XGROUP DESTROY"
    (exec ?timeout t [| "XGROUP"; "DESTROY"; key; group |])

let xreadgroup ?timeout ?count ?(noack = false) t ~group ~consumer ~streams =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let noack_a = if noack then [ "NOACK" ] else [] in
  let keys = List.map fst streams in
  let ids = List.map snd streams in
  let args =
    Array.of_list (
      [ "XREADGROUP"; "GROUP"; group; consumer ]
      @ count_a @ noack_a
      @ [ "STREAMS" ] @ keys @ ids
    )
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> parse_streams_reply "XREADGROUP" v

let xack ?timeout t key ~group ids =
  let args = Array.of_list ("XACK" :: key :: group :: ids) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "XACK" v)

(* ---------- stream admin ---------- *)

type xpending_summary = {
  count : int;
  min_id : string option;
  max_id : string option;
  consumers : (string * int) list;
}

let extract_string_opt cmd = function
  | Resp3.Null -> Ok None
  | v -> (match extract_string cmd v with Ok s -> Ok (Some s) | Error e -> Error e)

let parse_xpending_consumers cmd = function
  | Resp3.Null -> Ok []
  | Resp3.Array pairs ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Array [ name_v; count_v ] :: rest ->
            (match
               extract_string cmd name_v,
               match count_v with
               | Resp3.Integer n -> Ok (Int64.to_int n)
               | Resp3.Bulk_string s ->
                   (try Ok (int_of_string s) with _ -> Error (protocol_violation cmd count_v))
               | _ -> Error (protocol_violation cmd count_v)
             with
             | Ok name, Ok n -> loop ((name, n) :: acc) rest
             | Error e, _ | _, Error e -> Error e)
        | other :: _ -> Error (protocol_violation cmd other)
      in
      loop [] pairs
  | v -> Error (protocol_violation cmd v)

let xpending ?timeout ?read_from t key ~group =
  let cmd = "XPENDING" in
  match exec ?timeout ?read_from t [| cmd; key; group |] with
  | Error e -> Error e
  | Ok (Resp3.Array [ count_v; min_v; max_v; consumers_v ]) ->
      (match count_v with
       | Resp3.Integer count ->
           (match
              extract_string_opt cmd min_v,
              extract_string_opt cmd max_v,
              parse_xpending_consumers cmd consumers_v
            with
            | Ok min_id, Ok max_id, Ok consumers ->
                Ok
                  { count = Int64.to_int count; min_id; max_id; consumers }
            | Error e, _, _ | _, Error e, _ | _, _, Error e -> Error e)
       | _ -> Error (protocol_violation cmd count_v))
  | Ok v -> Error (protocol_violation cmd v)

type xpending_entry = {
  id : string;
  consumer : string;
  idle_ms : int;
  delivery_count : int;
}

let xpending_range ?timeout ?read_from ?idle_ms ?consumer t key ~group
    ~start ~end_ ~count =
  let idle_a = match idle_ms with
    | None -> []
    | Some n -> [ "IDLE"; string_of_int n ]
  in
  let consumer_a = match consumer with
    | None -> []
    | Some c -> [ c ]
  in
  let args =
    Array.of_list
      ([ "XPENDING"; key; group ]
       @ idle_a
       @ [ start; end_; string_of_int count ]
       @ consumer_a)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Array [ id_v; consumer_v; idle_v; delc_v ] :: rest ->
            (match
               extract_string "XPENDING id" id_v,
               extract_string "XPENDING consumer" consumer_v,
               (match idle_v with
                | Resp3.Integer n -> Ok (Int64.to_int n)
                | _ -> Error (protocol_violation "XPENDING idle" idle_v)),
               (match delc_v with
                | Resp3.Integer n -> Ok (Int64.to_int n)
                | _ -> Error (protocol_violation "XPENDING delivery_count" delc_v))
             with
             | Ok id, Ok consumer, Ok idle_ms, Ok delivery_count ->
                 loop
                   ({ id; consumer; idle_ms; delivery_count } :: acc) rest
             | Error e, _, _, _
             | _, Error e, _, _
             | _, _, Error e, _
             | _, _, _, Error e -> Error e)
        | other :: _ -> Error (protocol_violation "XPENDING entry" other)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "XPENDING range" v)

let xclaim_args ?idle_ms ?time_unix_ms ?retry_count ?(force = false)
    ?(justid = false) ~group ~consumer ~min_idle_ms ids =
  let idle_a = match idle_ms with
    | None -> []
    | Some n -> [ "IDLE"; string_of_int n ]
  in
  let time_a = match time_unix_ms with
    | None -> []
    | Some n -> [ "TIME"; string_of_int n ]
  in
  let retry_a = match retry_count with
    | None -> []
    | Some n -> [ "RETRYCOUNT"; string_of_int n ]
  in
  let force_a = if force then [ "FORCE" ] else [] in
  let justid_a = if justid then [ "JUSTID" ] else [] in
  [ group; consumer; string_of_int min_idle_ms ] @ ids
  @ idle_a @ time_a @ retry_a @ force_a @ justid_a

let xclaim ?timeout ?idle_ms ?time_unix_ms ?retry_count ?force
    t key ~group ~consumer ~min_idle_ms ~ids =
  let tail =
    xclaim_args ?idle_ms ?time_unix_ms ?retry_count ?force
      ~group ~consumer ~min_idle_ms ids
  in
  let args = Array.of_list ([ "XCLAIM"; key ] @ tail) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> parse_entries "XCLAIM" items
  | Ok v -> Error (protocol_violation "XCLAIM" v)

let xclaim_ids ?timeout ?idle_ms ?time_unix_ms ?retry_count ?force
    t key ~group ~consumer ~min_idle_ms ~ids =
  let tail =
    xclaim_args ?idle_ms ?time_unix_ms ?retry_count ?force ~justid:true
      ~group ~consumer ~min_idle_ms ids
  in
  let args = Array.of_list ([ "XCLAIM"; key ] @ tail) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      strings_of_resp3_collection "XCLAIM JUSTID" items
  | Ok v -> Error (protocol_violation "XCLAIM JUSTID" v)

type xautoclaim_result = {
  next_cursor : string;
  claimed : stream_entry list;
  deleted_ids : string list;
}

type xautoclaim_ids_result = {
  next_cursor : string;
  claimed_ids : string list;
  deleted_ids : string list;
}

let xautoclaim_args ?count ?(justid = false) ~group ~consumer ~min_idle_ms
    ~cursor () =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let justid_a = if justid then [ "JUSTID" ] else [] in
  [ group; consumer; string_of_int min_idle_ms; cursor ] @ count_a @ justid_a

let xautoclaim ?timeout ?count t key ~group ~consumer ~min_idle_ms ~cursor =
  let tail = xautoclaim_args ?count ~group ~consumer ~min_idle_ms ~cursor () in
  let args = Array.of_list ([ "XAUTOCLAIM"; key ] @ tail) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array [ cursor_v; claimed_v; deleted_v ]) ->
      (match extract_string "XAUTOCLAIM cursor" cursor_v with
       | Error e -> Error e
       | Ok next_cursor ->
           let claimed_r =
             match claimed_v with
             | Resp3.Array items -> parse_entries "XAUTOCLAIM claimed" items
             | _ -> Error (protocol_violation "XAUTOCLAIM claimed" claimed_v)
           in
           let deleted_r =
             match deleted_v with
             | Resp3.Array items ->
                 strings_of_resp3_collection "XAUTOCLAIM deleted" items
             | _ -> Error (protocol_violation "XAUTOCLAIM deleted" deleted_v)
           in
           (match claimed_r, deleted_r with
            | Ok claimed, Ok deleted_ids ->
                Ok { next_cursor; claimed; deleted_ids }
            | Error e, _ | _, Error e -> Error e))
  | Ok v -> Error (protocol_violation "XAUTOCLAIM" v)

let xautoclaim_ids ?timeout ?count t key ~group ~consumer ~min_idle_ms ~cursor =
  let tail =
    xautoclaim_args ?count ~justid:true ~group ~consumer ~min_idle_ms ~cursor ()
  in
  let args = Array.of_list ([ "XAUTOCLAIM"; key ] @ tail) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array [ cursor_v; claimed_v; deleted_v ]) ->
      (match extract_string "XAUTOCLAIM cursor" cursor_v with
       | Error e -> Error e
       | Ok next_cursor ->
           let claimed_r =
             match claimed_v with
             | Resp3.Array items ->
                 strings_of_resp3_collection "XAUTOCLAIM JUSTID claimed" items
             | _ ->
                 Error (protocol_violation "XAUTOCLAIM JUSTID claimed" claimed_v)
           in
           let deleted_r =
             match deleted_v with
             | Resp3.Array items ->
                 strings_of_resp3_collection "XAUTOCLAIM JUSTID deleted" items
             | _ ->
                 Error (protocol_violation "XAUTOCLAIM JUSTID deleted" deleted_v)
           in
           (match claimed_r, deleted_r with
            | Ok claimed_ids, Ok deleted_ids ->
                Ok { next_cursor; claimed_ids; deleted_ids }
            | Error e, _ | _, Error e -> Error e))
  | Ok v -> Error (protocol_violation "XAUTOCLAIM JUSTID" v)

let xinfo_stream ?timeout ?read_from t key =
  exec ?timeout ?read_from t [| "XINFO"; "STREAM"; key |]

let xinfo_groups ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "XINFO"; "GROUPS"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> Ok items
  | Ok v -> Error (protocol_violation "XINFO GROUPS" v)

let xinfo_consumers ?timeout ?read_from t key ~group =
  match exec ?timeout ?read_from t [| "XINFO"; "CONSUMERS"; key; group |] with
  | Error e -> Error e
  | Ok (Resp3.Array items) -> Ok items
  | Ok v -> Error (protocol_violation "XINFO CONSUMERS" v)

(* ---------- blocking commands ----------
   Caller is responsible for using a dedicated Client. Typed here for
   ergonomics; typing does not make multiplexed use safe. *)

type list_side = Left | Right
let string_of_side = function Left -> "LEFT" | Right -> "RIGHT"

let lmove ?timeout t ~source ~destination ~from ~to_ =
  let args =
    [| "LMOVE"; source; destination;
       string_of_side from; string_of_side to_ |]
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "LMOVE" v)

let parse_blpop_reply cmd = function
  | Resp3.Null -> Ok None
  | Resp3.Array [ k_v; v_v ] ->
      (match extract_string cmd k_v, extract_string cmd v_v with
       | Ok k, Ok v -> Ok (Some (k, v))
       | Error e, _ | _, Error e -> Error e)
  | v -> Error (protocol_violation cmd v)

let blpop_like cmd ?timeout t ~keys ~block_seconds =
  let args =
    Array.of_list
      (cmd :: keys @ [ Printf.sprintf "%g" block_seconds ])
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> parse_blpop_reply cmd v

let blpop ?timeout t ~keys ~block_seconds =
  blpop_like "BLPOP" ?timeout t ~keys ~block_seconds

let brpop ?timeout t ~keys ~block_seconds =
  blpop_like "BRPOP" ?timeout t ~keys ~block_seconds

let blmove ?timeout t ~source ~destination ~from ~to_ ~block_seconds =
  let args =
    [| "BLMOVE"; source; destination;
       string_of_side from; string_of_side to_;
       Printf.sprintf "%g" block_seconds |]
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "BLMOVE" v)

let wait_replicas ?timeout t ~num_replicas ~block_ms =
  match
    exec ?timeout t
      [| "WAIT"; string_of_int num_replicas; string_of_int block_ms |]
  with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "WAIT" v)

let xread_block ?timeout ?count t ~block_ms ~streams =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let keys = List.map fst streams in
  let ids = List.map snd streams in
  let args =
    Array.of_list (
      [ "XREAD" ] @ count_a
      @ [ "BLOCK"; string_of_int block_ms ]
      @ [ "STREAMS" ] @ keys @ ids
    )
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> parse_streams_reply "XREAD BLOCK" v

let xreadgroup_block ?timeout ?count ?(noack = false) t
    ~block_ms ~group ~consumer ~streams =
  let count_a = match count with
    | None -> []
    | Some n -> [ "COUNT"; string_of_int n ]
  in
  let noack_a = if noack then [ "NOACK" ] else [] in
  let keys = List.map fst streams in
  let ids = List.map snd streams in
  let args =
    Array.of_list (
      [ "XREADGROUP"; "GROUP"; group; consumer ]
      @ count_a
      @ [ "BLOCK"; string_of_int block_ms ]
      @ noack_a
      @ [ "STREAMS" ] @ keys @ ids
    )
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok v -> parse_streams_reply "XREADGROUP BLOCK" v
