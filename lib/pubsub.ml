(* Tracked subscription sets. Three independent tables because
   SUBSCRIBE / PSUBSCRIBE / SSUBSCRIBE live in separate name spaces
   on the server. *)
type subs = {
  mutable channels : string list;
  mutable patterns : string list;
  mutable shards : string list;
}

type t = {
  conn : Connection.t;
  subs : subs;
  subs_mutex : Mutex.t;
  with_timeout :
    'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result;
}

type message =
  | Channel of { channel : string; payload : string }
  | Pattern of { pattern : string; channel : string; payload : string }
  | Shard of { channel : string; payload : string }

let send_prefixed t prefix channels =
  let args = Array.of_list (prefix :: channels) in
  Connection.send_fire_and_forget t.conn args

let with_subs_locked t f =
  Mutex.lock t.subs_mutex;
  let r =
    try Ok (f t.subs)
    with e -> Error e
  in
  Mutex.unlock t.subs_mutex;
  match r with Ok v -> v | Error e -> raise e

let add_unique lst xs =
  List.fold_left (fun acc x -> if List.mem x acc then acc else x :: acc)
    lst xs

let remove_all lst xs =
  List.filter (fun x -> not (List.mem x xs)) lst

(* Replay everything we believe we should be subscribed to on the
   current connection. Called after each reconnect (and after the
   initial handshake — idempotent there because the tables start
   empty). Snapshots the subscription sets under the mutex, then
   issues the subscribe calls outside of it. *)
let replay t =
  let channels, patterns, shards =
    Mutex.lock t.subs_mutex;
    let c = t.subs.channels
    and p = t.subs.patterns
    and s = t.subs.shards in
    Mutex.unlock t.subs_mutex;
    c, p, s
  in
  let fire cmd xs =
    match xs with
    | [] -> ()
    | _ ->
        (match send_prefixed t cmd xs with
         | Ok () -> ()
         | Error _ ->
             (* Connection's next state transition will trigger us
                again; nothing we can usefully do here. *)
             ())
  in
  fire "SUBSCRIBE" channels;
  fire "PSUBSCRIBE" patterns;
  fire "SSUBSCRIBE" shards

let connect ~sw ~net ~clock ?domain_mgr ?config ~host ~port () =
  (* We fill [self] in two passes because the on_connected hook
     needs to call [replay] which dereferences the [t] we are still
     constructing. *)
  let self : t option ref = ref None in
  let on_connected () =
    match !self with
    | None -> ()
    | Some t -> replay t
  in
  let conn =
    Connection.connect ~sw ~net ~clock ?domain_mgr ?config ~on_connected
      ~host ~port ()
  in
  let with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result =
    fun secs f ->
      match Eio.Time.with_timeout clock secs (fun () -> Ok (f ())) with
      | Ok v -> Ok v
      | Error `Timeout -> Error `Timeout
  in
  let t = {
    conn;
    subs = { channels = []; patterns = []; shards = [] };
    subs_mutex = Mutex.create ();
    with_timeout;
  } in
  self := Some t;
  t

let close t = Connection.close t.conn

let subscribe t cs =
  if cs = [] then Ok ()
  else begin
    with_subs_locked t (fun subs ->
        subs.channels <- add_unique subs.channels cs);
    send_prefixed t "SUBSCRIBE" cs
  end

let unsubscribe t cs =
  with_subs_locked t (fun subs ->
      subs.channels <-
        (if cs = [] then [] else remove_all subs.channels cs));
  send_prefixed t "UNSUBSCRIBE" cs

let psubscribe t ps =
  if ps = [] then Ok ()
  else begin
    with_subs_locked t (fun subs ->
        subs.patterns <- add_unique subs.patterns ps);
    send_prefixed t "PSUBSCRIBE" ps
  end

let punsubscribe t ps =
  with_subs_locked t (fun subs ->
      subs.patterns <-
        (if ps = [] then [] else remove_all subs.patterns ps));
  send_prefixed t "PUNSUBSCRIBE" ps

let ssubscribe t cs =
  if cs = [] then Ok ()
  else begin
    with_subs_locked t (fun subs ->
        subs.shards <- add_unique subs.shards cs);
    send_prefixed t "SSUBSCRIBE" cs
  end

let sunsubscribe t cs =
  with_subs_locked t (fun subs ->
      subs.shards <-
        (if cs = [] then [] else remove_all subs.shards cs));
  send_prefixed t "SUNSUBSCRIBE" cs

let string_of_resp = function
  | Resp3.Bulk_string s | Resp3.Simple_string s
  | Resp3.Verbatim_string { data = s; _ } -> Some s
  | _ -> None

let classify (items : Resp3.t list) : [ `Ack | `Deliver of message | `Other ] =
  match items with
  | tag :: _ when
      (match string_of_resp tag with
       | Some t ->
           (match String.lowercase_ascii t with
            | "subscribe" | "unsubscribe"
            | "psubscribe" | "punsubscribe"
            | "ssubscribe" | "sunsubscribe" -> true
            | _ -> false)
       | None -> false) -> `Ack
  | [ tag; channel; payload ] ->
      (match string_of_resp tag,
             string_of_resp channel,
             string_of_resp payload with
       | Some t, Some ch, Some pl ->
           (match String.lowercase_ascii t with
            | "message" -> `Deliver (Channel { channel = ch; payload = pl })
            | "smessage" -> `Deliver (Shard { channel = ch; payload = pl })
            | _ -> `Other)
       | _ -> `Other)
  | [ tag; pattern; channel; payload ] ->
      (match string_of_resp tag,
             string_of_resp pattern,
             string_of_resp channel,
             string_of_resp payload with
       | Some t, Some pt, Some ch, Some pl
         when String.lowercase_ascii t = "pmessage" ->
           `Deliver (Pattern { pattern = pt; channel = ch; payload = pl })
       | _ -> `Other)
  | _ -> `Other

let take_push ?timeout t =
  let stream = Connection.pushes t.conn in
  match timeout with
  | None -> Ok (Eio.Stream.take stream)
  | Some secs ->
      (match t.with_timeout secs (fun () -> Eio.Stream.take stream) with
       | Ok v -> Ok v
       | Error `Timeout -> Error `Timeout)

let rec next_message ?timeout t =
  match take_push ?timeout t with
  | Error `Timeout -> Error `Timeout
  | Ok (Resp3.Push items) ->
      (match classify items with
       | `Deliver m -> Ok m
       | `Ack -> next_message ?timeout t
       | `Other -> next_message ?timeout t)
  | Ok _ -> next_message ?timeout t
