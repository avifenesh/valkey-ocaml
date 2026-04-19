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

let get ?timeout ?read_from t key =
  match exec ?timeout ?read_from t [| "GET"; key |] with
  | Error e -> Error e
  | Ok Resp3.Null -> Ok None
  | Ok (Resp3.Bulk_string s) -> Ok (Some s)
  | Ok v -> Error (protocol_violation "GET" v)

let set ?timeout ?ex_seconds ?nx ?xx t key value =
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
  match exec ?timeout t (Array.of_list !args) with
  | Error e -> Error e
  | Ok (Resp3.Simple_string "OK") -> Ok true
  | Ok Resp3.Null -> Ok false
  | Ok v -> Error (protocol_violation "SET" v)

let del ?timeout t keys =
  let args = Array.of_list ("DEL" :: keys) in
  match exec ?timeout t args with
  | Error e -> Error e
  | Ok (Resp3.Integer n) -> Ok (Int64.to_int n)
  | Ok v -> Error (protocol_violation "DEL" v)
