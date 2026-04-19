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

let build_set_args ?ex_seconds ?nx ?xx ?ifeq ~get key value =
  let args = ref [ "SET"; key; value ] in
  (match ex_seconds with
   | None -> ()
   | Some secs ->
       let ms = int_of_float (secs *. 1000.0) in
       args := !args @ [ "PX"; string_of_int ms ]);
  (match nx, xx with
   | Some true, _ -> args := !args @ [ "NX" ]
   | _, Some true -> args := !args @ [ "XX" ]
   | _ -> ());
  (match ifeq with
   | None -> ()
   | Some v -> args := !args @ [ "IFEQ"; v ]);
  if get then args := !args @ [ "GET" ];
  Array.of_list !args

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

let set ?timeout ?ex_seconds ?nx ?xx ?ifeq t key value =
  let args = build_set_args ?ex_seconds ?nx ?xx ?ifeq ~get:false key value in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok true
  | Ok Resp3.Null -> Ok false
  | Ok v -> Error (protocol_violation "SET" v)

let set_and_get ?timeout ?ex_seconds ?nx ?xx ?ifeq t key value =
  let args = build_set_args ?ex_seconds ?nx ?xx ?ifeq ~get:true key value in
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
