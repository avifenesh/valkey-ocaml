module Read_from = struct
  type t =
    | Primary
    | Prefer_replica
    | Az_affinity of { az : string }
    | Az_affinity_replicas_and_primary of { az : string }

  let default = Primary
end

module Target = struct
  type t =
    | Random
    | All_nodes
    | All_primaries
    | By_slot of int
    | By_node of string
    | By_channel of string
end

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
  connection : Connection.t;
  config : Config.t;
}
[@@warning "-69"]

let connect ~sw ~net ~clock ?domain_mgr ?(config = Config.default) ~host ~port () =
  let connection =
    Connection.connect ~sw ~net ~clock ?domain_mgr
      ~config:config.connection ~host ~port ()
  in
  { connection; config }

let close t = Connection.close t.connection

let raw_connection t = t.connection

let exec ?timeout ?target:_ ?read_from:_ t args =
  Connection.request ?timeout t.connection args

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
