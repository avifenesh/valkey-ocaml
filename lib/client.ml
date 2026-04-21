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

let connection_for_slot t slot = Router.connection_for_slot t.router slot

let is_standalone t = Router.is_standalone t.router

let atomic_lock_for_slot t slot =
  Router.atomic_lock_for_slot t.router slot

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

(* custom / custom_multi are named aliases for exec / exec_multi. Use
   them at call sites to make the intent clear: "I am calling a
   command this library has not wrapped with a typed helper, but I
   still want cluster-correct routing." Pure naming; no behavior
   change. *)
let custom = exec
let custom_multi = exec_multi

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

(* Score in RESP3 may arrive as Double or as Bulk_string,
   depending on server version + command. Handle both. *)
let score_of_resp3 cmd = function
  | Resp3.Double f -> Ok f
  | Resp3.Bulk_string s ->
      (try Ok (float_of_string s)
       with _ -> Error (protocol_violation cmd (Resp3.Bulk_string s)))
  | Resp3.Integer n -> Ok (Int64.to_float n)
  | v -> Error (protocol_violation cmd v)

let score_opt_of_resp3 cmd = function
  | Resp3.Null -> Ok None
  | v ->
      match score_of_resp3 cmd v with
      | Ok f -> Ok (Some f)
      | Error e -> Error e

(* ZRANGE WITHSCORES / ZRANGEBYSCORE WITHSCORES / ZPOPMIN / ZPOPMAX
   all return Array of [Array [member; score]] in RESP3. *)
let member_score_pairs cmd items =
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | Resp3.Array [ Resp3.Bulk_string m; score_v ] :: rest ->
        (match score_of_resp3 cmd score_v with
         | Ok s -> loop ((m, s) :: acc) rest
         | Error e -> Error e)
    | other :: _ -> Error (protocol_violation cmd other)
  in
  loop [] items

type zadd_mode =
  | Z_only_add
  | Z_only_update
  | Z_only_update_if_greater
  | Z_only_update_if_less
  | Z_add_or_update_if_greater
  | Z_add_or_update_if_less

let zadd_mode_args = function
  | None -> []
  | Some Z_only_add -> [ "NX" ]
  | Some Z_only_update -> [ "XX" ]
  | Some Z_only_update_if_greater -> [ "XX"; "GT" ]
  | Some Z_only_update_if_less -> [ "XX"; "LT" ]
  | Some Z_add_or_update_if_greater -> [ "GT" ]
  | Some Z_add_or_update_if_less -> [ "LT" ]

(* Score formatting: %.17g preserves full IEEE-754 double round-trip
   precision. Plain %g truncates to 6 significant digits, which
   silently loses precision on real-world scores. Matches the
   encoder in bin/fuzz_parser/. *)
let fmt_score f = Printf.sprintf "%.17g" f

let zadd ?timeout ?mode ?(ch = false) t key pairs =
  let pair_args =
    List.concat_map
      (fun (score, member) -> [ fmt_score score; member ])
      pairs
  in
  let args =
    Array.of_list
      ([ "ZADD"; key ]
       @ zadd_mode_args mode
       @ (if ch then [ "CH" ] else [])
       @ pair_args)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  (* Server returns Null when ~mode aborts the operation entirely
     (e.g. NX on a key whose every member already exists). Treat
     as zero added so the typed return stays [int]. *)
  | Ok Resp3.Null -> Ok 0
  | Ok v -> Error (protocol_violation "ZADD" v)

let zadd_incr ?timeout ?mode t key ~score ~member =
  let args =
    Array.of_list
      ([ "ZADD"; key ]
       @ zadd_mode_args mode
       @ [ "INCR"; fmt_score score; member ])
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok v ->
      (match score_of_resp3 "ZADD INCR" v with
       | Ok f -> Ok (Some f)
       | Error e -> Error e)

let zincrby ?timeout t key ~by ~member =
  match
    exec ?timeout t
      [| "ZINCRBY"; key; fmt_score by; member |]
  with
  | Error e -> Error e
  | Ok v -> score_of_resp3 "ZINCRBY" v

let zrem ?timeout t key members =
  let args = Array.of_list ([ "ZREM"; key ] @ members) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "ZREM" v)

let zrank_base cmd ?timeout ?read_from t key member =
  match exec ?timeout ?read_from t [| cmd; key; member |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Integer n) -> Ok (Some (Int64.to_int n))
  | Ok v -> Error (protocol_violation cmd v)

let zrank_with_score_base cmd ?timeout ?read_from t key member =
  match
    exec ?timeout ?read_from t [| cmd; key; member; "WITHSCORE" |]
  with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Array [ Resp3.Integer rank; score_v ]) ->
      (match score_of_resp3 cmd score_v with
       | Ok s -> Ok (Some (Int64.to_int rank, s))
       | Error e -> Error e)
  | Ok v -> Error (protocol_violation cmd v)

let zrank ?timeout ?read_from t key member =
  zrank_base "ZRANK" ?timeout ?read_from t key member

let zrank_with_score ?timeout ?read_from t key member =
  zrank_with_score_base "ZRANK" ?timeout ?read_from t key member

let zrevrank ?timeout ?read_from t key member =
  zrank_base "ZREVRANK" ?timeout ?read_from t key member

let zrevrank_with_score ?timeout ?read_from t key member =
  zrank_with_score_base "ZREVRANK" ?timeout ?read_from t key member

let zscore ?timeout ?read_from t key member =
  match exec ?timeout ?read_from t [| "ZSCORE"; key; member |] with
  | Error e -> Error e
  | Ok v -> score_opt_of_resp3 "ZSCORE" v

let zmscore ?timeout ?read_from t key members =
  let args = Array.of_list ([ "ZMSCORE"; key ] @ members) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | v :: rest ->
            (match score_opt_of_resp3 "ZMSCORE" v with
             | Ok x -> loop (x :: acc) rest
             | Error e -> Error e)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "ZMSCORE" v)

let zcount ?timeout ?read_from t key ~min ~max =
  match
    exec ?timeout ?read_from t
      [| "ZCOUNT"; key;
         score_bound_to_string min; score_bound_to_string max |]
  with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "ZCOUNT" v)

let zrange_with_scores ?timeout ?read_from ?(rev = false) t key
    ~start ~stop =
  let base =
    [ "ZRANGE"; key; string_of_int start; string_of_int stop ]
  in
  let args =
    Array.of_list
      (base
       @ (if rev then [ "REV" ] else [])
       @ [ "WITHSCORES" ])
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      member_score_pairs "ZRANGE WITHSCORES" items
  | Ok v -> Error (protocol_violation "ZRANGE WITHSCORES" v)

let zrangebyscore_with_scores ?timeout ?read_from ?limit t key
    ~min ~max =
  let base =
    [ "ZRANGEBYSCORE"; key;
      score_bound_to_string min; score_bound_to_string max;
      "WITHSCORES" ]
  in
  let limit_a =
    match limit with
    | None -> []
    | Some (off, count) ->
        [ "LIMIT"; string_of_int off; string_of_int count ]
  in
  match exec ?timeout ?read_from t (Array.of_list (base @ limit_a)) with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      member_score_pairs "ZRANGEBYSCORE WITHSCORES" items
  | Ok v ->
      Error (protocol_violation "ZRANGEBYSCORE WITHSCORES" v)

let zpop_base cmd ?timeout ?count t key =
  let args =
    match count with
    | None -> [| cmd; key |]
    | Some n -> [| cmd; key; string_of_int n |]
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array []) -> Ok []
  | Ok (Resp3.Array items) ->
      (* Two reply shapes:
         - ZPOPMIN key (no count) on a non-empty key returns
           [Bulk_string member; score] (a flat 2-elem array).
         - ZPOPMIN key COUNT returns Array of [member; score] pairs. *)
      (match items with
       | [ Resp3.Bulk_string m; score_v ] ->
           (match score_of_resp3 cmd score_v with
            | Ok s -> Ok [ (m, s) ]
            | Error e -> Error e)
       | _ -> member_score_pairs cmd items)
  | Ok v -> Error (protocol_violation cmd v)

let zpopmin ?timeout ?count t key =
  zpop_base "ZPOPMIN" ?timeout ?count t key

let zpopmax ?timeout ?count t key =
  zpop_base "ZPOPMAX" ?timeout ?count t key

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

(* Aggregate a fan_primaries result: return Ok if every shard succeeded
   with the same payload; surface the first server error otherwise.
   Used by SCRIPT LOAD / SCRIPT FLUSH / SCRIPT EXISTS so a cluster
   behaves like a standalone from the caller's perspective. *)
let fan_primaries_unanimous cmd ?timeout ~decode t args =
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t args
  in
  let rec loop first = function
    | [] ->
        (match first with
         | Some v -> Ok v
         | None ->
             Error (Connection.Error.Terminal
                      (Printf.sprintf "%s: no primaries to dispatch to" cmd)))
    | (_, Error e) :: _ -> Error e
    | (_, Ok reply) :: rest ->
        (match decode reply with
         | Error e -> Error e
         | Ok v ->
             (match first with
              | None -> loop (Some v) rest
              | Some v0 when v0 = v -> loop first rest
              | Some _ ->
                  Error (Connection.Error.Terminal
                           (Printf.sprintf
                              "%s: shards disagreed on reply" cmd))))
  in
  loop None per_node

let script_load ?timeout t script =
  (* Load on every primary so any subsequent EVALSHA finds the SHA
     regardless of which shard the key routes to. All primaries
     must produce the same SHA (it is a deterministic digest). *)
  let decode = function
    | Resp3.Bulk_string s -> Ok s
    | Resp3.Simple_string s -> Ok s
    | v -> Error (protocol_violation "SCRIPT LOAD" v)
  in
  fan_primaries_unanimous "SCRIPT LOAD" ?timeout ~decode t
    [| "SCRIPT"; "LOAD"; script |]

let script_exists ?timeout t shas =
  (* A SHA is "present" only if every primary has it — otherwise an
     EVALSHA routing through an unloaded primary will NOSCRIPT. *)
  let args = Array.of_list ("SCRIPT" :: "EXISTS" :: shas) in
  let decode = function
    | Resp3.Array items ->
        let rec loop acc = function
          | [] -> Ok (List.rev acc)
          | Resp3.Integer 0L :: rest -> loop (false :: acc) rest
          | Resp3.Integer 1L :: rest -> loop (true :: acc) rest
          | other :: _ ->
              Error (protocol_violation "SCRIPT EXISTS" other)
        in
        loop [] items
    | v -> Error (protocol_violation "SCRIPT EXISTS" v)
  in
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t args
  in
  (* AND-reduce bit-by-bit across primaries. *)
  let combine a b =
    try Ok (List.map2 ( && ) a b)
    with Invalid_argument _ ->
      Error (Connection.Error.Terminal
               "SCRIPT EXISTS: shards returned different row counts")
  in
  let rec loop acc = function
    | [] ->
        (match acc with
         | Some v -> Ok v
         | None ->
             Error (Connection.Error.Terminal
                      "SCRIPT EXISTS: no primaries to dispatch to"))
    | (_, Error e) :: _ -> Error e
    | (_, Ok reply) :: rest ->
        (match decode reply with
         | Error e -> Error e
         | Ok row ->
             (match acc with
              | None -> loop (Some row) rest
              | Some prev ->
                  (match combine prev row with
                   | Ok merged -> loop (Some merged) rest
                   | Error e -> Error e)))
  in
  loop None per_node

type script_flush_mode = Flush_sync | Flush_async

let script_flush ?timeout ?mode t =
  let tail = match mode with
    | None -> []
    | Some Flush_sync -> [ "SYNC" ]
    | Some Flush_async -> [ "ASYNC" ]
  in
  let args = Array.of_list ([ "SCRIPT"; "FLUSH" ] @ tail) in
  let decode = function
    | Resp3.Simple_string "OK" -> Ok ()
    | v -> Error (protocol_violation "SCRIPT FLUSH" v)
  in
  let result =
    fan_primaries_unanimous "SCRIPT FLUSH" ?timeout ~decode t args
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
    (* EVAL loads the script on whichever shard the key routes to.
       Remember the SHA only if it is now on every primary — fall
       through to a full SCRIPT LOAD fan-out so subsequent EVALSHA
       calls on any shard succeed. *)
    match eval ?timeout t ~script:(Script.source s) ~keys ~args with
    | Error e -> Error e
    | Ok v ->
        (match script_load ?timeout t (Script.source s) with
         | Ok _ -> remember (); Ok v
         | Error _ -> Ok v)
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

let keys ?timeout t pattern =
  (* KEYS on a cluster is per-node: each primary (and replica, if you
     ask one) only knows its own slots. Fan to every node and flatten.
     Large keyspaces: this is a heavy command — it scans every node's
     whole keyspace. SCAN iteration is usually the right choice. *)
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t
      [| "KEYS"; pattern |]
  in
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | (_, Error e) :: _ -> Error e
    | (_, Ok (Resp3.Array items)) :: rest ->
        (match strings_of_resp3_collection "KEYS" items with
         | Error e -> Error e
         | Ok ks -> loop (List.rev_append ks acc) rest)
    | (_, Ok v) :: _ -> Error (protocol_violation "KEYS" v)
  in
  loop [] per_node

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

(* XREAD / XREADGROUP place their key list after a STREAMS keyword,
   so a static key-index spec cannot find them. These wrappers
   compute the slot from the first stream key and pass the target
   explicitly. Multi-stream calls in cluster require all stream keys
   to hash to the same slot (hashtags); the server rejects otherwise
   as CROSSSLOT. *)
let target_of_streams streams =
  match streams with
  | [] -> Target.Random
  | (first_key, _) :: _ -> Target.By_slot (Slot.of_key first_key)

let xread ?timeout ?read_from ?count t ~streams =
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
  let target = target_of_streams streams in
  match exec ?timeout ~target ?read_from t args with
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
  let target = target_of_streams streams in
  match exec ?timeout ~target t args with
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

(* WAIT semantics diverge between standalone and cluster:

   - Standalone: a single primary acks; [wait_replicas] returns the
     count of its replicas that replied.
   - Cluster: each primary has its own replicas. A WAIT on one
     connection only says something about one shard's durability.

   We expose the full per-primary view ([wait_replicas_all]) and
   two standard aggregations ([wait_replicas_min],
   [wait_replicas_sum]) so the API makes the choice explicit
   instead of hiding a policy inside one function.

   "Wait for the first primary to reach [num_replicas] and cancel
   the rest" is a separate shape — it needs fiber cancellation in
   exec_multi. Deferred. *)

let wait_replicas ?timeout t ~num_replicas ~block_ms =
  match
    exec ?timeout t
      [| "WAIT"; string_of_int num_replicas; string_of_int block_ms |]
  with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "WAIT" v)

let wait_replicas_all ?timeout t ~num_replicas ~block_ms =
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t
      [| "WAIT"; string_of_int num_replicas; string_of_int block_ms |]
  in
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | (_, Error e) :: _ -> Error e
    | (node_id, Ok (Resp3.Integer n)) :: rest ->
        loop ((node_id, Int64.to_int n) :: acc) rest
    | (_, Ok v) :: _ -> Error (protocol_violation "WAIT" v)
  in
  loop [] per_node

let wait_replicas_min ?timeout t ~num_replicas ~block_ms =
  match wait_replicas_all ?timeout t ~num_replicas ~block_ms with
  | Error e -> Error e
  | Ok [] -> Ok 0
  | Ok ((_, n0) :: rest) ->
      Ok (List.fold_left (fun acc (_, n) -> min acc n) n0 rest)

let wait_replicas_sum ?timeout t ~num_replicas ~block_ms =
  match wait_replicas_all ?timeout t ~num_replicas ~block_ms with
  | Error e -> Error e
  | Ok xs -> Ok (List.fold_left (fun acc (_, n) -> acc + n) 0 xs)

(* ---------- pub/sub produce side ---------- *)

let publish ?timeout t ~channel ~message =
  match exec ?timeout t [| "PUBLISH"; channel; message |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "PUBLISH" v)

let spublish ?timeout t ~channel ~message =
  (* SPUBLISH is sharded: route by slot of [channel] so the primary
     that owns the channel's slot publishes it. Override the default
     (Command_spec marks SPUBLISH as rand_w for lack of a better
     By_channel dispatcher) with an explicit By_slot target. *)
  let target = Target.By_slot (Slot.of_key channel) in
  match exec ?timeout ~target t [| "SPUBLISH"; channel; message |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "SPUBLISH" v)

let xread_block ?timeout ?read_from ?count t ~block_ms ~streams =
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
  let target = target_of_streams streams in
  match exec ?timeout ~target ?read_from t args with
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
  let target = target_of_streams streams in
  match exec ?timeout ~target t args with
  | Error e -> Error e
  | Ok v -> parse_streams_reply "XREADGROUP BLOCK" v

(* ---------- bitmaps ---------- *)

type bit_range_unit = Byte | Bit

let bit_unit_arg = function Byte -> "BYTE" | Bit -> "BIT"

type bit_range =
  | From of int
  | From_to of { start : int; end_ : int }
  | From_to_unit of { start : int; end_ : int; unit : bit_range_unit }

let bit_range_args = function
  | None -> []
  | Some (From n) -> [ string_of_int n ]
  | Some (From_to { start; end_ }) ->
      [ string_of_int start; string_of_int end_ ]
  | Some (From_to_unit { start; end_; unit }) ->
      [ string_of_int start; string_of_int end_; bit_unit_arg unit ]

type bit = B0 | B1

let bit_arg = function B0 -> "0" | B1 -> "1"

let bit_of_integer cmd = function
  | Ok (Resp3.Integer 0L) -> Ok B0
  | Ok (Resp3.Integer 1L) -> Ok B1
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let bitcount ?timeout ?read_from ?range t key =
  let args =
    Array.of_list ("BITCOUNT" :: key :: bit_range_args range)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "BITCOUNT" v)

let bitpos ?timeout ?read_from ?range t key ~bit =
  let args =
    Array.of_list
      ("BITPOS" :: key :: bit_arg bit :: bit_range_args range)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "BITPOS" v)

type bitop =
  | Bitop_and of string list
  | Bitop_or of string list
  | Bitop_xor of string list
  | Bitop_not of string

let bitop_args = function
  | Bitop_and sources -> "AND", sources
  | Bitop_or sources -> "OR", sources
  | Bitop_xor sources -> "XOR", sources
  | Bitop_not source -> "NOT", [ source ]

let bitop ?timeout t op ~destination =
  let op_arg, sources = bitop_args op in
  let args =
    Array.of_list ("BITOP" :: op_arg :: destination :: sources)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "BITOP" v)

let setbit ?timeout t key ~offset ~value =
  let args = [| "SETBIT"; key; string_of_int offset; bit_arg value |] in
  bit_of_integer "SETBIT" (exec ?timeout t args)

let getbit ?timeout ?read_from t key ~offset =
  let args = [| "GETBIT"; key; string_of_int offset |] in
  bit_of_integer "GETBIT" (exec ?timeout ?read_from t args)

type bitfield_type = Signed of int | Unsigned of int

let bitfield_type_arg = function
  | Signed n -> Printf.sprintf "i%d" n
  | Unsigned n -> Printf.sprintf "u%d" n

type bitfield_offset = Bit_offset of int | Scaled_offset of int

let bitfield_offset_arg = function
  | Bit_offset n -> string_of_int n
  | Scaled_offset n -> Printf.sprintf "#%d" n

type bitfield_overflow = Wrap | Sat | Fail

let bitfield_overflow_arg = function
  | Wrap -> "WRAP"
  | Sat -> "SAT"
  | Fail -> "FAIL"

type bitfield_op =
  | Get of { ty : bitfield_type; at : bitfield_offset }
  | Set of { ty : bitfield_type; at : bitfield_offset; value : int64 }
  | Incrby of
      { ty : bitfield_type; at : bitfield_offset; increment : int64 }
  | Overflow of bitfield_overflow

(* Whether this op contributes an element to the reply array.
   Every server-side reply maps 1:1 with a non-[Overflow] op. *)
let op_produces_reply = function
  | Overflow _ -> false
  | Get _ | Set _ | Incrby _ -> true

let bitfield_op_args = function
  | Get { ty; at } ->
      [ "GET"; bitfield_type_arg ty; bitfield_offset_arg at ]
  | Set { ty; at; value } ->
      [ "SET"; bitfield_type_arg ty; bitfield_offset_arg at;
        Int64.to_string value ]
  | Incrby { ty; at; increment } ->
      [ "INCRBY"; bitfield_type_arg ty; bitfield_offset_arg at;
        Int64.to_string increment ]
  | Overflow o ->
      [ "OVERFLOW"; bitfield_overflow_arg o ]

let decode_bitfield_element = function
  | Resp3.Integer n -> Ok (Some n)
  | Resp3.Null -> Ok None
  | v -> Error (protocol_violation "BITFIELD" v)

let bitfield ?timeout t key ops =
  let arg_chunks = List.concat_map bitfield_op_args ops in
  let args = Array.of_list ("BITFIELD" :: key :: arg_chunks) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      (* Only ops that produce a reply are matched against [items];
         [Overflow] modifiers are pure prefix state on the wire. *)
      let expected = List.filter op_produces_reply ops in
      let rec zip ops items acc =
        match ops, items with
        | [], [] -> Ok (List.rev acc)
        | _ :: ops_rest, v :: items_rest ->
            (match decode_bitfield_element v with
             | Error e -> Error e
             | Ok x -> zip ops_rest items_rest (x :: acc))
        | _ ->
            Error
              (Connection.Error.Protocol_violation
                 (Printf.sprintf
                    "BITFIELD: %d reply elements for %d ops"
                    (List.length items)
                    (List.length expected)))
      in
      zip expected items []
  | Ok v -> Error (protocol_violation "BITFIELD" v)

let bitfield_ro ?timeout ?read_from t key ~gets =
  let arg_chunks =
    List.concat_map
      (fun (ty, at) ->
        [ "GET"; bitfield_type_arg ty; bitfield_offset_arg at ])
      gets
  in
  let args = Array.of_list ("BITFIELD_RO" :: key :: arg_chunks) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Integer n :: rest -> loop (n :: acc) rest
        | v :: _ -> Error (protocol_violation "BITFIELD_RO" v)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "BITFIELD_RO" v)

(* ---------- HyperLogLog ---------- *)

let pfadd ?timeout t key ~elements =
  let args = Array.of_list ("PFADD" :: key :: elements) in
  bool_from_integer "PFADD" (exec ?timeout t args)

let pfcount ?timeout ?read_from t keys =
  let args = Array.of_list ("PFCOUNT" :: keys) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "PFCOUNT" v)

let pfmerge ?timeout t ~destination ~sources =
  let args =
    Array.of_list ("PFMERGE" :: destination :: sources)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok ()
  | Ok v -> Error (protocol_violation "PFMERGE" v)

(* ---------- generic keyspace ---------- *)

let copy ?timeout ?db ?(replace = false) t ~source ~destination =
  let db_args = match db with
    | None -> []
    | Some n -> [ "DB"; string_of_int n ]
  in
  let replace_args = if replace then [ "REPLACE" ] else [] in
  let args =
    Array.of_list
      ("COPY" :: source :: destination :: db_args @ replace_args)
  in
  bool_from_integer "COPY" (exec ?timeout t args)

let dump ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "DUMP"; key |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "DUMP" v)

let restore
    ?timeout ?(replace = false) ?(abs_ttl = false)
    ?idle_time_seconds ?freq t key ~ttl_ms ~serialized =
  let replace_args = if replace then [ "REPLACE" ] else [] in
  let absttl_args = if abs_ttl then [ "ABSTTL" ] else [] in
  let idle_args = match idle_time_seconds with
    | None -> []
    | Some n -> [ "IDLETIME"; string_of_int n ]
  in
  let freq_args = match freq with
    | None -> []
    | Some n -> [ "FREQ"; string_of_int n ]
  in
  let args =
    Array.of_list
      ([ "RESTORE"; key; string_of_int ttl_ms; serialized ]
       @ replace_args @ absttl_args @ idle_args @ freq_args)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok ()
  | Ok v -> Error (protocol_violation "RESTORE" v)

let touch ?timeout t keys =
  let args = Array.of_list ("TOUCH" :: keys) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "TOUCH" v)

let randomkey ?timeout ?read_from t =
  match exec ?timeout ?read_from t [| "RANDOMKEY" |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "RANDOMKEY" v)

(* OBJECT sub-commands: identical decode shape — Null for missing
   key, Integer for *count/time/freq*, Bulk_string for *encoding*. *)
let object_encoding ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "OBJECT"; "ENCODING"; key |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) | Ok (Resp3.Simple_string s) ->
      Ok (Some s)
  | Ok v -> Error (protocol_violation "OBJECT ENCODING" v)

let object_int_subcommand cmd sub ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| cmd; sub; key |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Integer n) -> Ok (Some (Int64.to_int n))
  | Ok v ->
      Error
        (protocol_violation (Printf.sprintf "%s %s" cmd sub) v)

let object_refcount ?timeout ?read_from t key =
  object_int_subcommand "OBJECT" "REFCOUNT" ?timeout ?read_from t key

let object_idletime ?timeout ?read_from t key =
  object_int_subcommand "OBJECT" "IDLETIME" ?timeout ?read_from t key

let object_freq ?timeout ?read_from t key =
  object_int_subcommand "OBJECT" "FREQ" ?timeout ?read_from t key

(* ---------- Geo ---------- *)

type geo_unit = Meters | Kilometers | Miles | Feet

let geo_unit_arg = function
  | Meters -> "M" | Kilometers -> "KM"
  | Miles -> "MI" | Feet -> "FT"

type geo_coord = { longitude : float; latitude : float }

type geo_point = {
  longitude : float;
  latitude : float;
  member : string;
}

type geoadd_cond = Geoadd_nx | Geoadd_xx

let geoadd_cond_arg = function
  | Geoadd_nx -> "NX"
  | Geoadd_xx -> "XX"

let geoadd ?timeout ?cond ?(ch = false) t key ~points =
  let cond_args = match cond with
    | None -> []
    | Some c -> [ geoadd_cond_arg c ]
  in
  let ch_args = if ch then [ "CH" ] else [] in
  let point_args =
    List.concat_map
      (fun { longitude; latitude; member } ->
        [ Printf.sprintf "%.17g" longitude;
          Printf.sprintf "%.17g" latitude;
          member ])
      points
  in
  let args =
    Array.of_list ("GEOADD" :: key :: cond_args @ ch_args @ point_args)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "GEOADD" v)

let parse_double_string cmd s =
  try Ok (float_of_string s)
  with _ ->
    Error
      (Connection.Error.Protocol_violation
         (Printf.sprintf "%s: non-numeric reply %S" cmd s))

let geodist ?timeout ?read_from ?unit t key ~member1 ~member2 =
  let unit_args = match unit with
    | None -> []
    | Some u -> [ geo_unit_arg u ]
  in
  let args =
    Array.of_list ("GEODIST" :: key :: member1 :: member2 :: unit_args)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) ->
      (match parse_double_string "GEODIST" s with
       | Ok f -> Ok (Some f)
       | Error e -> Error e)
  | Ok (Resp3.Double f) -> Ok (Some f)
  | Ok v -> Error (protocol_violation "GEODIST" v)

(* Parse a 2-element array as a coord, or Null as missing. *)
let parse_geo_coord_reply cmd = function
  | Resp3.Null -> Ok None
  | Resp3.Array [ lon_v; lat_v ] ->
      let to_float v =
        match v with
        | Resp3.Bulk_string s | Resp3.Simple_string s ->
            (try Ok (float_of_string s)
             with _ ->
               Error
                 (Connection.Error.Protocol_violation
                    (Printf.sprintf "%s: non-numeric coord %S" cmd s)))
        | Resp3.Double f -> Ok f
        | other -> Error (protocol_violation cmd other)
      in
      (match to_float lon_v, to_float lat_v with
       | Ok longitude, Ok latitude ->
           Ok (Some { longitude; latitude })
       | Error e, _ | _, Error e -> Error e)
  | v -> Error (protocol_violation cmd v)

let geopos ?timeout ?read_from t key ~members =
  let args = Array.of_list ("GEOPOS" :: key :: members) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | v :: rest ->
            (match parse_geo_coord_reply "GEOPOS" v with
             | Ok o -> loop (o :: acc) rest
             | Error e -> Error e)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "GEOPOS" v)

let geohash ?timeout ?read_from t key ~members =
  let args = Array.of_list ("GEOHASH" :: key :: members) in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | Resp3.Null :: rest -> loop (None :: acc) rest
        | Resp3.Bulk_string s :: rest
        | Resp3.Simple_string s :: rest ->
            loop (Some s :: acc) rest
        | v :: _ -> Error (protocol_violation "GEOHASH" v)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "GEOHASH" v)

type geo_from =
  | From_member of string
  | From_lonlat of { longitude : float; latitude : float }

type geo_shape =
  | By_radius of { radius : float; unit : geo_unit }
  | By_box of { width : float; height : float; unit : geo_unit }

type geo_order = Geo_asc | Geo_desc

type geo_count = {
  count : int;
  any : bool;
}

type geo_search_result = {
  member : string;
  distance : float option;
  hash : int64 option;
  coord : geo_coord option;
}

let geo_from_args = function
  | From_member m -> [ "FROMMEMBER"; m ]
  | From_lonlat { longitude; latitude } ->
      [ "FROMLONLAT";
        Printf.sprintf "%.17g" longitude;
        Printf.sprintf "%.17g" latitude ]

let geo_shape_args = function
  | By_radius { radius; unit } ->
      [ "BYRADIUS"; Printf.sprintf "%.17g" radius; geo_unit_arg unit ]
  | By_box { width; height; unit } ->
      [ "BYBOX";
        Printf.sprintf "%.17g" width;
        Printf.sprintf "%.17g" height;
        geo_unit_arg unit ]

let geo_order_args = function
  | None -> []
  | Some Geo_asc -> [ "ASC" ]
  | Some Geo_desc -> [ "DESC" ]

let geo_count_args = function
  | None -> []
  | Some { count; any = false } -> [ "COUNT"; string_of_int count ]
  | Some { count; any = true } ->
      [ "COUNT"; string_of_int count; "ANY" ]

(* Decode one search-result entry.

   With no WITH* flags, each entry is just a Bulk_string member
   name. With any WITH* flag, the entry is an Array whose first
   element is the member name and subsequent elements appear in
   the fixed order DIST, HASH, COORD when their flag is set. *)
let decode_search_result ~with_coord ~with_dist ~with_hash v =
  match v with
  | Resp3.Bulk_string m | Resp3.Simple_string m
    when not (with_coord || with_dist || with_hash) ->
      Ok { member = m; distance = None; hash = None; coord = None }
  | Resp3.Array (member_v :: rest) ->
      let member =
        match member_v with
        | Resp3.Bulk_string s | Resp3.Simple_string s -> Ok s
        | other -> Error (protocol_violation "GEOSEARCH member" other)
      in
      (match member with
       | Error e -> Error e
       | Ok member ->
           let rec consume ~with_dist ~with_hash ~with_coord
                   acc rest =
             match with_dist, with_hash, with_coord, rest with
             | true, _, _, (Resp3.Bulk_string s | Resp3.Simple_string s) :: rest' ->
                 (match parse_double_string "GEOSEARCH dist" s with
                  | Error e -> Error e
                  | Ok f ->
                      consume ~with_dist:false ~with_hash
                        ~with_coord
                        { acc with distance = Some f } rest')
             | true, _, _, Resp3.Double f :: rest' ->
                 consume ~with_dist:false ~with_hash ~with_coord
                   { acc with distance = Some f } rest'
             | false, true, _, Resp3.Integer n :: rest' ->
                 consume ~with_dist ~with_hash:false ~with_coord
                   { acc with hash = Some n } rest'
             | false, false, true, coord_v :: rest' ->
                 (match parse_geo_coord_reply "GEOSEARCH coord" coord_v with
                  | Error e -> Error e
                  | Ok c ->
                      consume ~with_dist ~with_hash
                        ~with_coord:false { acc with coord = c } rest')
             | false, false, false, [] -> Ok acc
             | _ ->
                 Error
                   (Connection.Error.Protocol_violation
                      "GEOSEARCH: reply shape does not match WITH* flags")
           in
           consume ~with_dist ~with_hash ~with_coord
             { member; distance = None; hash = None; coord = None }
             rest)
  | v -> Error (protocol_violation "GEOSEARCH" v)

let geosearch
    ?timeout ?read_from ?order ?count
    ?(with_coord = false) ?(with_dist = false) ?(with_hash = false)
    t key ~from ~shape =
  let with_args =
    (if with_coord then [ "WITHCOORD" ] else [])
    @ (if with_dist then [ "WITHDIST" ] else [])
    @ (if with_hash then [ "WITHHASH" ] else [])
  in
  let args =
    Array.of_list
      ("GEOSEARCH" :: key
       :: (geo_from_args from
           @ geo_shape_args shape
           @ geo_order_args order
           @ geo_count_args count
           @ with_args))
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok (Resp3.Array items) ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | v :: rest ->
            (match
               decode_search_result ~with_coord ~with_dist ~with_hash v
             with
             | Ok r -> loop (r :: acc) rest
             | Error e -> Error e)
      in
      loop [] items
  | Ok v -> Error (protocol_violation "GEOSEARCH" v)

let geosearchstore
    ?timeout ?order ?count ?(store_dist = false)
    t ~destination ~source ~from ~shape =
  let store_dist_args = if store_dist then [ "STOREDIST" ] else [] in
  let args =
    Array.of_list
      ("GEOSEARCHSTORE" :: destination :: source
       :: (geo_from_args from
           @ geo_shape_args shape
           @ geo_order_args order
           @ geo_count_args count
           @ store_dist_args))
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "GEOSEARCHSTORE" v)

(* ---------- CLIENT admin ---------- *)

let expect_ok cmd = function
  | Ok (Resp3.Simple_string "OK") -> Ok ()
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let string_of_reply cmd = function
  | Ok (Resp3.Bulk_string s) | Ok (Resp3.Simple_string s)
  | Ok (Resp3.Verbatim_string { data = s; _ }) -> Ok s
  | Ok v -> Error (protocol_violation cmd v)
  | Error e -> Error e

let client_id ?timeout t =
  match exec ?timeout t [| "CLIENT"; "ID" |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "CLIENT ID" v)

let client_getname ?timeout t =
  match exec ?timeout t [| "CLIENT"; "GETNAME" |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string "") -> Ok None   (* legacy RESP2 empty-string *)
  | Ok (Resp3.Bulk_string s)
  | Ok (Resp3.Simple_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "CLIENT GETNAME" v)

let client_setname ?timeout t ~name =
  expect_ok "CLIENT SETNAME"
    (exec ?timeout t [| "CLIENT"; "SETNAME"; name |])

let client_info ?timeout t =
  string_of_reply "CLIENT INFO"
    (exec ?timeout t [| "CLIENT"; "INFO" |])

type client_type = Client_normal | Client_master | Client_replica
                 | Client_pubsub

let client_type_arg = function
  | Client_normal -> "normal"
  | Client_master -> "master"
  | Client_replica -> "replica"
  | Client_pubsub -> "pubsub"

let client_list ?timeout ?type_ ?ids t =
  let type_args = match type_ with
    | None -> []
    | Some t -> [ "TYPE"; client_type_arg t ]
  in
  let id_args = match ids with
    | None | Some [] -> []
    | Some xs -> "ID" :: List.map string_of_int xs
  in
  let args = Array.of_list ("CLIENT" :: "LIST" :: type_args @ id_args) in
  (* Command_spec says CLIENT LIST is Fan_all_nodes, so exec_multi
     hits every node. Concatenate the per-node replies. *)
  let per_node = exec_multi ?timeout t args in
  let rec loop acc = function
    | [] -> Ok (String.concat "" (List.rev acc))
    | (_, Error e) :: _ -> Error e
    | (_, Ok r) :: rest ->
        (match string_of_reply "CLIENT LIST" (Ok r) with
         | Error e -> Error e
         | Ok s -> loop (s :: acc) rest)
  in
  loop [] per_node

type client_pause_mode = Pause_all | Pause_write

let client_pause_mode_arg = function
  | Pause_all -> "ALL"
  | Pause_write -> "WRITE"

let aggregate_ok_fan cmd per_node =
  let rec loop = function
    | [] -> Ok ()
    | (_, Error e) :: _ -> Error e
    | (_, Ok (Resp3.Simple_string "OK")) :: rest -> loop rest
    | (_, Ok v) :: _ -> Error (protocol_violation cmd v)
  in
  loop per_node

let client_pause ?timeout ?mode t ~timeout_ms =
  let mode_args = match mode with
    | None -> []
    | Some m -> [ client_pause_mode_arg m ]
  in
  let args =
    Array.of_list
      ("CLIENT" :: "PAUSE" :: string_of_int timeout_ms :: mode_args)
  in
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t args
  in
  aggregate_ok_fan "CLIENT PAUSE" per_node

let client_unpause ?timeout t =
  let per_node =
    exec_multi ?timeout ~fan:Router.Fan_target.All_primaries t
      [| "CLIENT"; "UNPAUSE" |]
  in
  aggregate_ok_fan "CLIENT UNPAUSE" per_node

let on_off = function true -> "ON" | false -> "OFF"

let client_no_evict ?timeout t enable =
  expect_ok "CLIENT NO-EVICT"
    (exec ?timeout t [| "CLIENT"; "NO-EVICT"; on_off enable |])

let client_no_touch ?timeout t enable =
  expect_ok "CLIENT NO-TOUCH"
    (exec ?timeout t [| "CLIENT"; "NO-TOUCH"; on_off enable |])

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

let kill_filter_args = function
  | Kill_id n -> [ "ID"; string_of_int n ]
  | Kill_not_id n -> [ "NOT-ID"; string_of_int n ]
  | Kill_type t -> [ "TYPE"; client_type_arg t ]
  | Kill_not_type t -> [ "NOT-TYPE"; client_type_arg t ]
  | Kill_addr s -> [ "ADDR"; s ]
  | Kill_not_addr s -> [ "NOT-ADDR"; s ]
  | Kill_laddr s -> [ "LADDR"; s ]
  | Kill_not_laddr s -> [ "NOT-LADDR"; s ]
  | Kill_user s -> [ "USER"; s ]
  | Kill_not_user s -> [ "NOT-USER"; s ]
  | Kill_skipme b -> [ "SKIPME"; if b then "yes" else "no" ]
  | Kill_maxage n -> [ "MAXAGE"; string_of_int n ]

let client_kill ?timeout t ~filters =
  let chunks = List.concat_map kill_filter_args filters in
  let args = Array.of_list ("CLIENT" :: "KILL" :: chunks) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "CLIENT KILL" v)

let client_tracking_on
    ?timeout ?redirect ?(prefixes = [])
    ?(bcast = false) ?(optin = false) ?(optout = false)
    ?(noloop = false) t () =
  let redir_args = match redirect with
    | None -> []
    | Some id -> [ "REDIRECT"; string_of_int id ]
  in
  let prefix_args =
    List.concat_map (fun p -> [ "PREFIX"; p ]) prefixes
  in
  let bcast_args = if bcast then [ "BCAST" ] else [] in
  let optin_args = if optin then [ "OPTIN" ] else [] in
  let optout_args = if optout then [ "OPTOUT" ] else [] in
  let noloop_args = if noloop then [ "NOLOOP" ] else [] in
  let args =
    Array.of_list
      ("CLIENT" :: "TRACKING" :: "ON"
       :: redir_args @ prefix_args
          @ bcast_args @ optin_args @ optout_args @ noloop_args)
  in
  expect_ok "CLIENT TRACKING ON" (exec ?timeout t args)

let client_tracking_off ?timeout t =
  expect_ok "CLIENT TRACKING OFF"
    (exec ?timeout t [| "CLIENT"; "TRACKING"; "OFF" |])

(* ---------- Functions ---------- *)

let function_load ?timeout ?(replace = false) t ~source =
  let replace_args = if replace then [ "REPLACE" ] else [] in
  let args =
    Array.of_list
      ("FUNCTION" :: "LOAD" :: replace_args @ [ source ])
  in
  let decode = function
    | Resp3.Bulk_string s | Resp3.Simple_string s -> Ok s
    | v -> Error (protocol_violation "FUNCTION LOAD" v)
  in
  fan_primaries_unanimous "FUNCTION LOAD" ?timeout ~decode t args

let function_delete ?timeout t ~library_name =
  let args = [| "FUNCTION"; "DELETE"; library_name |] in
  let decode = function
    | Resp3.Simple_string "OK" -> Ok ()
    | v -> Error (protocol_violation "FUNCTION DELETE" v)
  in
  fan_primaries_unanimous "FUNCTION DELETE" ?timeout ~decode t args

let function_flush ?timeout ?mode t =
  let tail = match mode with
    | None -> []
    | Some Flush_sync -> [ "SYNC" ]
    | Some Flush_async -> [ "ASYNC" ]
  in
  let args = Array.of_list ("FUNCTION" :: "FLUSH" :: tail) in
  let decode = function
    | Resp3.Simple_string "OK" -> Ok ()
    | v -> Error (protocol_violation "FUNCTION FLUSH" v)
  in
  fan_primaries_unanimous "FUNCTION FLUSH" ?timeout ~decode t args

let function_list ?timeout ?library_name_pattern ?(with_code = false) t =
  let lib_args = match library_name_pattern with
    | None -> []
    | Some p -> [ "LIBRARYNAME"; p ]
  in
  let code_args = if with_code then [ "WITHCODE" ] else [] in
  let args =
    Array.of_list
      ("FUNCTION" :: "LIST" :: lib_args @ code_args)
  in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Array xs) -> Ok xs
  | Ok v -> Error (protocol_violation "FUNCTION LIST" v)

let fcall ?timeout t ~function_ ~keys ~args =
  let cmd =
    Array.of_list
      ("FCALL" :: function_
       :: string_of_int (List.length keys)
       :: keys @ args)
  in
  exec ?timeout t cmd

let fcall_ro ?timeout ?read_from t ~function_ ~keys ~args =
  let cmd =
    Array.of_list
      ("FCALL_RO" :: function_
       :: string_of_int (List.length keys)
       :: keys @ args)
  in
  exec ?timeout ?read_from t cmd

(* ---------- CLUSTER introspection ---------- *)

let cluster_keyslot ?timeout ?read_from t ~key =
  match exec ?timeout ?read_from t [| "CLUSTER"; "KEYSLOT"; key |] with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "CLUSTER KEYSLOT" v)

let cluster_info ?timeout t =
  string_of_reply "CLUSTER INFO"
    (exec ?timeout t [| "CLUSTER"; "INFO" |])

(* ---------- LATENCY ---------- *)

let latency_doctor ?timeout t =
  string_of_reply "LATENCY DOCTOR"
    (exec ?timeout t [| "LATENCY"; "DOCTOR" |])

let latency_reset ?timeout ?events t =
  let ev_args = match events with
    | None | Some [] -> []
    | Some xs -> xs
  in
  let args = Array.of_list ("LATENCY" :: "RESET" :: ev_args) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "LATENCY RESET" v)

(* ---------- MEMORY ---------- *)

let memory_usage ?timeout ?read_from ?samples t key =
  let sample_args = match samples with
    | None -> []
    | Some n -> [ "SAMPLES"; string_of_int n ]
  in
  let args =
    Array.of_list ("MEMORY" :: "USAGE" :: key :: sample_args)
  in
  match exec ?timeout ?read_from t args with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Integer n) -> Ok (Some (Int64.to_int n))
  | Ok v -> Error (protocol_violation "MEMORY USAGE" v)

let memory_purge ?timeout t =
  expect_ok "MEMORY PURGE"
    (exec ?timeout t [| "MEMORY"; "PURGE" |])
