(* See batch.mli for the conceptual model. *)

type queue_error =
  | Fan_out_in_atomic_batch of string

type batch_entry_result =
  | One of (Resp3.t, Connection.Error.t) result
  | Many of (string * (Resp3.t, Connection.Error.t) result) list

type queued = {
  args : string array;
  spec : Command_spec.t;
  index : int;
}

type t = {
  atomic : bool;
  hint_key : string option;
  watch : string list;
  mutable queued : queued list;    (* reversed; reversed again on run *)
  mutable count : int;
}

let create ?(atomic = false) ?hint_key ?(watch = []) () =
  { atomic; hint_key; watch; queued = []; count = 0 }

let is_atomic t = t.atomic

let length t = t.count

let command_name_of args =
  if Array.length args = 0 then ""
  else String.uppercase_ascii args.(0)

let is_fan_out = function
  | Command_spec.Fan_primaries | Command_spec.Fan_all_nodes -> true
  | _ -> false

let queue t args =
  let spec = Command_spec.lookup args in
  if t.atomic && is_fan_out spec then
    Error (Fan_out_in_atomic_batch (command_name_of args))
  else begin
    let index = t.count in
    t.queued <- { args; spec; index } :: t.queued;
    t.count <- index + 1;
    Ok ()
  end

(* Wall-clock deadline, then ?timeout passed to each Client.exec /
   exec_multi call is the remaining window. Commands that start
   with zero window left will time out immediately. Fibers are
   all spawned in parallel via Fiber.List.iter so the nominal
   batch duration is [max over commands of their execution time]
   rather than [sum]. *)
let remaining_window ~deadline =
  match deadline with
  | None -> None
  | Some d ->
      let left = d -. Unix.gettimeofday () in
      (* Never pass a non-positive timeout; use a tiny positive
         value to signal "fire and time out immediately". *)
      Some (Float.max 0.001 left)

let dispatch_one client raw ~deadline q =
  let per_timeout = remaining_window ~deadline in
  let result : batch_entry_result =
    match q.spec with
    | Command_spec.Fan_primaries ->
        let rs =
          Client.exec_multi ?timeout:per_timeout
            ~fan:Router.Fan_target.All_primaries client q.args
        in
        Many rs
    | Command_spec.Fan_all_nodes ->
        let rs =
          Client.exec_multi ?timeout:per_timeout
            ~fan:Router.Fan_target.All_nodes client q.args
        in
        Many rs
    | _ ->
        One (Client.exec ?timeout:per_timeout client q.args)
  in
  raw.(q.index) <- Some result

let run_non_atomic ?timeout client t =
  let entries = List.rev t.queued in
  let raw = Array.make t.count None in
  let deadline =
    match timeout with
    | None -> None
    | Some s -> Some (Unix.gettimeofday () +. s)
  in
  Eio.Fiber.List.iter (dispatch_one client raw ~deadline) entries;
  let finalized =
    Array.map
      (function
        | Some r -> r
        | None -> One (Error Connection.Error.Timeout))
      raw
  in
  Ok (Some finalized)

(* ---------- atomic mode ---------- *)

(* Determine the slot an atomic batch will run against.
     1. Explicit [hint_key] wins.
     2. Else: first queued command whose spec has a key, use that key.
     3. Else: fail — a pure keyless atomic batch has no target. *)
let atomic_slot t =
  match t.hint_key with
  | Some k -> Ok (Slot.of_key k)
  | None ->
      let rec first_keyed = function
        | [] -> None
        | q :: rest ->
            match q.spec with
            | Command_spec.Single_key { key_index; _ }
            | Command_spec.Multi_key { first_key_index = key_index; _ } ->
                if Array.length q.args > key_index
                then Some (Slot.of_key q.args.(key_index))
                else first_keyed rest
            | _ -> first_keyed rest
      in
      (match first_keyed (List.rev t.queued) with
       | Some slot -> Ok slot
       | None ->
           Error
             (Connection.Error.Terminal
                "atomic batch: no hint_key and no queued command has a \
                 key — cannot determine target slot"))

(* Validate every queued command's key hashes to [slot]. Returns
   [Error] with a CROSSSLOT-style message on the first mismatch. *)
let validate_same_slot ~slot queued =
  let rec loop = function
    | [] -> Ok ()
    | q :: rest ->
        (match q.spec with
         | Command_spec.Single_key { key_index; _ }
         | Command_spec.Multi_key { first_key_index = key_index; _ } ->
             if Array.length q.args > key_index then begin
               let k = q.args.(key_index) in
               let s = Slot.of_key k in
               if s <> slot then
                 Error
                   (Connection.Error.Server_error
                      { code = "CROSSSLOT";
                        message =
                          Printf.sprintf
                            "atomic batch: key %S hashes to slot %d but \
                             the batch is pinned to slot %d"
                            k s slot })
               else loop rest
             end
             else loop rest
         | _ -> loop rest)
  in
  loop queued

let request_ok conn args =
  match Connection.request conn args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string _) -> Ok ()
  | Ok v ->
      Error
        (Connection.Error.Protocol_violation
           (Format.asprintf "%s: unexpected reply %a"
              (if Array.length args > 0 then args.(0) else "?")
              Resp3.pp v))

(* Send the queued commands back to back (each reply should be
   +QUEUED), collect replies without fast-failing on a single
   bad-arity error — the server will EXECABORT at EXEC time. *)
let queue_all conn queued =
  List.iter
    (fun q ->
      (* The request may come back as a Server_error for bad arity /
         unknown command — that's still a real RESP reply the server
         sent; dropping it here is fine because EXECABORT propagates
         through EXEC. *)
      let _ = Connection.request conn q.args in
      ())
    queued

let decode_exec_reply ~expected_n = function
  | Resp3.Null ->
      Ok None          (* WATCH abort *)
  | Resp3.Array items ->
      let arr = Array.make expected_n (One (Error Connection.Error.Closed)) in
      List.iteri
        (fun i reply ->
          if i < expected_n then
            arr.(i) <- One (Ok reply))
        items;
      Ok (Some arr)
  | v ->
      Error
        (Connection.Error.Protocol_violation
           (Format.asprintf "EXEC: unexpected reply %a" Resp3.pp v))

let run_atomic ?timeout:_ client t =
  let queued = List.rev t.queued in
  let expected_n = t.count in
  if expected_n = 0 then
    (* Empty atomic batch — nothing to run. Return an empty array. *)
    Ok (Some [||])
  else
    match atomic_slot t with
    | Error e -> Error e
    | Ok slot ->
        (match validate_same_slot ~slot queued with
         | Error e -> Error e
         | Ok () ->
             (match Client.connection_for_slot client slot with
              | None ->
                  Error
                    (Connection.Error.Terminal
                       (Printf.sprintf
                          "atomic batch: no live connection for slot %d"
                          slot))
              | Some conn ->
                  let send () =
                    (* WATCH first (if any), then MULTI, then all
                       queued commands (server replies +QUEUED or
                       error), then EXEC returning an array (or Null
                       on WATCH abort). Each step is serial on the
                       same connection to preserve MULTI ordering. *)
                    let watch_result =
                      match t.watch with
                      | [] -> Ok ()
                      | keys ->
                          request_ok conn
                            (Array.of_list ("WATCH" :: keys))
                    in
                    match watch_result with
                    | Error e -> Error e
                    | Ok () ->
                        (match request_ok conn [| "MULTI" |] with
                         | Error e -> Error e
                         | Ok () ->
                             queue_all conn queued;
                             match Connection.request conn [| "EXEC" |] with
                             | Error e -> Error e
                             | Ok v -> decode_exec_reply ~expected_n v)
                  in
                  send ()))

let run ?timeout client t =
  if t.atomic then run_atomic ?timeout client t
  else run_non_atomic ?timeout client t

(* ---- typed cluster helpers ---- *)

let protocol_violation cmd v =
  Connection.Error.Protocol_violation
    (Format.asprintf "%s: unexpected reply %a" cmd Resp3.pp v)

let mget_cluster ?timeout client keys =
  let b = create () in
  List.iter
    (fun k -> let _ = queue b [| "GET"; k |] in ())
    keys;
  match run ?timeout client b with
  | Error e -> Error e
  | Ok None -> Error (protocol_violation "mget_cluster" Resp3.Null)
  | Ok (Some results) ->
      let err = ref None in
      let acc = ref [] in
      List.iteri
        (fun i k ->
          match results.(i) with
          | One (Ok Resp3.Null) -> acc := (k, None) :: !acc
          | One (Ok (Resp3.Bulk_string s)) -> acc := (k, Some s) :: !acc
          | One (Ok v) ->
              if !err = None then
                err := Some (protocol_violation "GET" v)
          | One (Error e) -> if !err = None then err := Some e
          | Many _ ->
              if !err = None then
                err := Some (protocol_violation
                               "mget_cluster: unexpected fan-out" Resp3.Null))
        keys;
      (match !err with
       | Some e -> Error e
       | None -> Ok (List.rev !acc))

let mset_cluster ?timeout client kvs =
  let b = create () in
  List.iter
    (fun (k, v) -> let _ = queue b [| "SET"; k; v |] in ())
    kvs;
  match run ?timeout client b with
  | Error e -> Error e
  | Ok None -> Error (protocol_violation "mset_cluster" Resp3.Null)
  | Ok (Some results) ->
      let err = ref None in
      Array.iter
        (function
          | One (Ok _) -> ()
          | One (Error e) -> if !err = None then err := Some e
          | Many _ -> ())
        results;
      (match !err with Some e -> Error e | None -> Ok ())

(* Shared shape: issue [CMD k] per key, sum the integer replies,
   propagate the first error encountered. Used by DEL / UNLINK /
   EXISTS / TOUCH. *)
let per_key_sum ?timeout ~cmd client keys =
  let b = create () in
  List.iter
    (fun k -> let _ = queue b [| cmd; k |] in ())
    keys;
  match run ?timeout client b with
  | Error e -> Error e
  | Ok None -> Error (protocol_violation (cmd ^ "_cluster") Resp3.Null)
  | Ok (Some results) ->
      let total = ref 0 in
      let err = ref None in
      Array.iter
        (function
          | One (Ok (Resp3.Integer n)) ->
              total := !total + Int64.to_int n
          | One (Ok v) ->
              if !err = None then err := Some (protocol_violation cmd v)
          | One (Error e) -> if !err = None then err := Some e
          | Many _ -> ())
        results;
      (match !err with Some e -> Error e | None -> Ok !total)

let del_cluster ?timeout client keys =
  per_key_sum ?timeout ~cmd:"DEL" client keys

let unlink_cluster ?timeout client keys =
  per_key_sum ?timeout ~cmd:"UNLINK" client keys

let exists_cluster ?timeout client keys =
  per_key_sum ?timeout ~cmd:"EXISTS" client keys

let touch_cluster ?timeout client keys =
  per_key_sum ?timeout ~cmd:"TOUCH" client keys
