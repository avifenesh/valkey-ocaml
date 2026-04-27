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

let close_connection conn =
  try Connection.interrupt conn
  with Eio.Io _ | End_of_file | Invalid_argument _
     | Unix.Unix_error _ -> ()

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

(* In cluster mode every key in an atomic batch must hash to the
   exact same slot. Standalone keeps native Valkey semantics and
   allows any keys because there is only one server. *)
let validate_same_slot ~client ~slot queued =
  let standalone = Client.is_standalone client in
  let rec loop = function
    | [] -> Ok ()
    | q :: rest ->
        (match q.spec with
         | Command_spec.Single_key { key_index; _ }
         | Command_spec.Multi_key { first_key_index = key_index; _ } ->
             if Array.length q.args > key_index then begin
               let k = q.args.(key_index) in
               let s = Slot.of_key k in
               if not standalone && s <> slot then
                 Error
                   (Connection.Error.Server_error
                      { code = "CROSSSLOT";
                        message =
                          Printf.sprintf
                            "atomic batch: key %S hashes to slot %d \
                             but the batch is pinned to slot %d"
                            k s slot })
               else loop rest
             end
             else loop rest
         | _ -> loop rest)
  in
  loop queued

let request_ok ?timeout conn args =
  match Connection.request ?timeout conn args with
  | Error e -> Error e
  | Ok (Resp3.Simple_string _) -> Ok ()
  | Ok v ->
      Error
        (Connection.Error.Protocol_violation
           (Format.asprintf "%s: unexpected reply %a"
              (if Array.length args > 0 then args.(0) else "?")
              Resp3.pp v))

(* Outcome of [queue_all] inside a MULTI block.
   - [All_queued]: every command got a +QUEUED back; safe to EXEC.
   - [Topology_changed]: the server returned MOVED or ASK while
     we were filling the multi-queue. The slot moved underneath
     us between [WATCH]/[MULTI] and [queue_all]. The MULTI block
     is now in an unknown state on the server (Valkey will
     EXECABORT on EXEC). The caller should DISCARD and treat
     this as a WATCH abort so its retry loop re-routes against
     the fresh topology. Without this signal, the EXECABORT
     leaks to the user as [Server_error] and they can't tell
     "topology moved" from "I sent a malformed command."
   - [Transport_error]: timeout / Closed / Protocol_violation
     on the wire — bail out and let the caller reconnect. *)
type queue_outcome =
  | All_queued
  | Topology_changed
  | Transport_error of Connection.Error.t

let queue_all ~deadline conn queued =
  let rec loop = function
    | [] -> All_queued
    | q :: rest ->
        let per_timeout = remaining_window ~deadline in
        (match Connection.request ?timeout:per_timeout conn q.args with
         | Ok (Resp3.Simple_string "QUEUED") -> loop rest
         | Ok v ->
             Transport_error
               (Connection.Error.Protocol_violation
                  (Format.asprintf
                     "queue inside MULTI: unexpected reply %a"
                     Resp3.pp v))
         | Error
             (Connection.Error.Server_error
                { code = "MOVED" | "ASK"; _ }) ->
             Topology_changed
         | Error (Connection.Error.Server_error _) ->
             (* Bad-arity / unknown command etc. — leave it to the
                server's EXECABORT path. The transaction array's
                per-command slot will carry the error; if every
                queued command failed, EXEC returns EXECABORT and
                we surface it through [decode_exec_reply]. *)
             loop rest
         | Error e -> Transport_error e)
  in
  loop queued

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

let run_atomic ?timeout client t =
  let queued = List.rev t.queued in
  let expected_n = t.count in
  let deadline =
    match timeout with
    | None -> None
    | Some s -> Some (Unix.gettimeofday () +. s)
  in
  if expected_n = 0 then
    (* Empty atomic batch — nothing to run. Return an empty array. *)
    Ok (Some [||])
  else
    match atomic_slot t with
    | Error e -> Error e
    | Ok slot ->
        (match validate_same_slot ~client ~slot queued with
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
                  let fail e =
                    close_connection conn;
                    Error e
                  in
                  (* Serialise concurrent atomic ops on the same
                     primary connection so their MULTI/EXEC blocks
                     don't interleave. Non-atomic pipeline traffic
                     on the same connection is unaffected — it
                     doesn't acquire this mutex. *)
                  let mutex = Client.atomic_lock_for_slot client slot in
                  Eio.Mutex.use_rw ~protect:true mutex (fun () ->
                      let watch_result =
                        match t.watch with
                        | [] -> Ok ()
                        | keys ->
                            request_ok ?timeout:(remaining_window ~deadline) conn
                              (Array.of_list ("WATCH" :: keys))
                      in
                      match watch_result with
                      | Error e -> fail e
                      | Ok () ->
                          (match
                             request_ok
                               ?timeout:(remaining_window ~deadline)
                               conn [| "MULTI" |]
                           with
                           | Error e -> fail e
                           | Ok () ->
                               (match queue_all ~deadline conn queued with
                                | Transport_error e -> fail e
                                | Topology_changed ->
                                    (* Slot moved during queueing.
                                       Drop the connection so the
                                       supervisor reconnects and
                                       rediscovers the topology;
                                       surface as a WATCH-abort so
                                       the caller's retry loop runs
                                       on the fresh primary. *)
                                    close_connection conn;
                                    Ok None
                                | All_queued ->
                                    (match
                                       Connection.request
                                         ?timeout:(remaining_window ~deadline)
                                         conn [| "EXEC" |]
                                     with
                                     | Error
                                         (Connection.Error.Server_error
                                            { code = "MOVED" | "ASK"; _ }) ->
                                         close_connection conn;
                                         Ok None
                                     | Error e -> fail e
                                     | Ok v ->
                                         (match
                                            decode_exec_reply ~expected_n v
                                          with
                                          | Ok _ as ok -> ok
                                          | Error e -> fail e)))))))

let run ?timeout client t =
  if t.atomic then run_atomic ?timeout client t
  else run_non_atomic ?timeout client t

(* ---------- WATCH guards ---------- *)

type guard = {
  client : Client.t;
  conn : Connection.t;
  pinned_slot : int;
  atomic_lock : Eio.Mutex.t;
  mutable released : bool;
}

(* All watched keys must share a slot (cluster). Either resolve
   from [hint_key] and verify each watch key matches it, or pick
   the first watch key's slot and verify the rest. *)
let resolve_watch_slot ~hint_key keys =
  match hint_key, keys with
  | _, [] ->
      Error
        (Connection.Error.Terminal
           "Batch.watch: at least one key required")
  | Some hk, _ ->
      let s = Slot.of_key hk in
      (match List.find_opt (fun k -> Slot.of_key k <> s) keys with
       | None -> Ok s
       | Some k ->
           Error
             (Connection.Error.Server_error
                { code = "CROSSSLOT";
                  message =
                    Printf.sprintf
                      "Batch.watch: hint_key %S hashes to slot %d \
                       but watched key %S hashes to slot %d"
                      hk s k (Slot.of_key k) }))
  | None, k0 :: rest ->
      let s = Slot.of_key k0 in
      (match List.find_opt (fun k -> Slot.of_key k <> s) rest with
       | None -> Ok s
       | Some k ->
           Error
             (Connection.Error.Server_error
                { code = "CROSSSLOT";
                  message =
                    Printf.sprintf
                      "Batch.watch: keys %S and %S hash to \
                       different slots (%d vs %d) — all watched \
                       keys must share a slot"
                      k0 k s (Slot.of_key k) }))

let watch ?hint_key client keys =
  match resolve_watch_slot ~hint_key keys with
  | Error e -> Error e
  | Ok slot ->
      (match Client.connection_for_slot client slot with
       | None ->
           Error
             (Connection.Error.Terminal
                (Printf.sprintf
                   "Batch.watch: no live connection for slot %d"
                   slot))
       | Some conn ->
           let mutex = Client.atomic_lock_for_slot client slot in
           Eio.Mutex.lock mutex;
           let g = {
             client; conn; pinned_slot = slot;
             atomic_lock = mutex; released = false;
           } in
           let args = Array.of_list ("WATCH" :: keys) in
           (match request_ok conn args with
            | Error e ->
                g.released <- true;
                Eio.Mutex.unlock mutex;
                Error e
            | Ok () -> Ok g))

let release_guard g =
  if not g.released then begin
    g.released <- true;
    (* Best-effort UNWATCH; ignore errors — we're tearing down
       anyway and the next command on this connection won't have
       stale WATCH state once the server reset it on disconnect
       in the worst case. *)
    let _ = Connection.request g.conn [| "UNWATCH" |] in
    Eio.Mutex.unlock g.atomic_lock
  end

let run_with_guard ?timeout t g =
  if g.released then
    Error
      (Connection.Error.Terminal
         "Batch.run_with_guard: guard already released")
  else if not t.atomic then begin
    release_guard g;
    Error
      (Connection.Error.Terminal
         "Batch.run_with_guard: guard requires an atomic batch \
          (create with ~atomic:true)")
  end
  else begin
    let queued = List.rev t.queued in
    let expected_n = t.count in
    let deadline =
      match timeout with
      | None -> None
      | Some s -> Some (Unix.gettimeofday () +. s)
    in
    let slot_check =
      if expected_n = 0 then Ok g.pinned_slot
      else
        match atomic_slot t with
        | Error e -> Error e
        | Ok s ->
            if s <> g.pinned_slot then
              Error
                (Connection.Error.Server_error
                   { code = "CROSSSLOT";
                     message =
                       Printf.sprintf
                         "Batch.run_with_guard: batch pinned to \
                          slot %d but guard watches slot %d"
                         s g.pinned_slot })
            else Ok s
    in
    match slot_check with
    | Error e -> release_guard g; Error e
    | Ok slot ->
        match validate_same_slot ~client:g.client ~slot queued with
        | Error e -> release_guard g; Error e
        | Ok () ->
            let conn = g.conn in
            let fail e =
              close_connection conn;
              Error e
            in
            let result =
              if expected_n = 0 then begin
                (* Empty batch under guard: user looked, decided
                   no write needed. UNWATCH and return []. *)
                let _ =
                  Connection.request
                    ?timeout:(remaining_window ~deadline)
                    conn [| "UNWATCH" |]
                in
                Ok (Some [||])
              end else
                match
                  request_ok
                    ?timeout:(remaining_window ~deadline)
                    conn [| "MULTI" |]
                with
                | Error e -> fail e
                | Ok () ->
                    (match queue_all ~deadline conn queued with
                     | Transport_error e -> fail e
                     | Topology_changed ->
                         close_connection conn;
                         Ok None
                     | All_queued ->
                         (match
                            Connection.request
                              ?timeout:(remaining_window ~deadline)
                              conn [| "EXEC" |]
                          with
                          | Error
                              (Connection.Error.Server_error
                                 { code = "MOVED" | "ASK"; _ }) ->
                              close_connection conn;
                              Ok None
                          | Error e -> fail e
                          | Ok v ->
                              (match decode_exec_reply ~expected_n v with
                               | Ok _ as ok -> ok
                               | Error e -> fail e)))
            in
            (* EXEC implicitly clears WATCH on the server. The
               empty-batch path already sent UNWATCH. Either way
               there's nothing left to UNWATCH; just drop the
               mutex without re-sending. *)
            g.released <- true;
            Eio.Mutex.unlock g.atomic_lock;
            result
  end

let with_watch ?hint_key client keys f =
  match watch ?hint_key client keys with
  | Error e -> Error e
  | Ok g ->
      Fun.protect
        ~finally:(fun () -> release_guard g)
        (fun () -> Ok (f g))

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

(* PFCOUNT across cluster slots via PFMERGE. When every key
   already shares a slot we just hand off to the server's
   multi-key PFCOUNT. Otherwise we materialise every non-missing
   HLL as a temp in a hashtag-controlled slot, PFMERGE them into
   a destination, PFCOUNT that, then DEL everything. *)
let pfcount_cluster ?timeout client keys =
  match keys with
  | [] -> Ok 0
  | _ ->
      let slots = List.map (fun k -> Slot.of_key k) keys in
      let same_slot =
        match slots with
        | [] -> true
        | s :: rest -> List.for_all (fun s' -> s' = s) rest
      in
      if same_slot then
        Client.pfcount ?timeout client keys
      else begin
        (* Cross-slot: bring every input HLL into one hashtag-
           controlled slot via DUMP+RESTORE, PFMERGE, PFCOUNT,
           cleanup. The hashtag text determines the temp slot
           deterministically. *)
        let tag =
          Printf.sprintf "%d.%d"
            (Unix.getpid ())
            (Random.bits ())
        in
        let temp_of i =
          Printf.sprintf "{pfc:%s}:src:%d" tag i
        in
        let dest = Printf.sprintf "{pfc:%s}:dest" tag in
        let temps = ref [] in
        let err = ref None in
        List.iteri
          (fun i k ->
            if !err = None then
              match Client.dump ?timeout client k with
              | Error e -> err := Some e
              | Ok None -> () (* missing key = empty HLL, skip *)
              | Ok (Some bytes) ->
                  let t = temp_of i in
                  (match
                     Client.restore ?timeout client t
                       ~ttl_ms:0 ~serialized:bytes ~replace:true
                   with
                   | Error e -> err := Some e
                   | Ok () -> temps := t :: !temps))
          keys;
        let cleanup () =
          let all = dest :: !temps in
          let _ = Client.del ?timeout client all in
          ()
        in
        match !err with
        | Some e -> cleanup (); Error e
        | None ->
            let sources = List.rev !temps in
            (match sources with
             | [] ->
                 (* All inputs were missing — union is empty. *)
                 cleanup ();
                 Ok 0
             | _ ->
                 (match
                    Client.pfmerge ?timeout client
                      ~destination:dest ~sources
                  with
                  | Error e -> cleanup (); Error e
                  | Ok () ->
                      let r = Client.pfcount ?timeout client [ dest ] in
                      cleanup ();
                      r))
      end
