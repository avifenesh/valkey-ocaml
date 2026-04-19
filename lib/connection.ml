module Byte_sem = struct
  type t = {
    mutable available : int;
    mutex : Eio.Mutex.t;
    cond : Eio.Condition.t;
  }

  let make n =
    { available = n;
      mutex = Eio.Mutex.create ();
      cond = Eio.Condition.create () }

  let acquire t n =
    Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
        while t.available < n do
          Eio.Condition.await t.cond t.mutex
        done;
        t.available <- t.available - n)

  let release t n =
    Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
        t.available <- t.available + n;
        Eio.Condition.broadcast t.cond)
end

(* Thin wrapper over Eio.Stream so the rest of the code can use a consistent
   interface. Eio.Stream.take is cancellable, which is what we need for clean
   shutdown when the supervisor races workers. *)
module Chan = struct
  type 'a t = {
    stream : 'a Eio.Stream.t;
    mutable closed : bool;
    closed_mutex : Eio.Mutex.t;
  }

  exception Closed

  (* Eio.Stream is bounded by count. We want byte-budget to be the real
     backpressure; pick a very large count cap so the stream never blocks
     on count in realistic workloads. *)
  let capacity = 1_000_000

  let create () =
    { stream = Eio.Stream.create capacity;
      closed = false;
      closed_mutex = Eio.Mutex.create () }

  let push t v =
    Eio.Mutex.use_rw ~protect:true t.closed_mutex (fun () ->
        if t.closed then raise Closed);
    Eio.Stream.add t.stream v

  let take t =
    if t.closed && Eio.Stream.length t.stream = 0 then raise Closed
    else Eio.Stream.take t.stream

  let close t =
    Eio.Mutex.use_rw ~protect:true t.closed_mutex (fun () ->
        t.closed <- true)

  let drain t =
    let drained = Queue.create () in
    let rec loop () =
      match Eio.Stream.take_nonblocking t.stream with
      | Some v -> Queue.push v drained; loop ()
      | None -> ()
    in
    loop ();
    drained
end

module Error = struct
  type t =
    | Tcp_refused of string
    | Dns_failed of string
    | Tls_failed of string
    | Handshake_rejected of Valkey_error.t
    | Auth_failed of Valkey_error.t
    | Protocol_violation of string
    | Timeout
    | Interrupted
    | Queue_full
    | Closed
    | Server_error of Valkey_error.t
    | Terminal of string

  let equal = ( = )

  let pp ppf = function
    | Tcp_refused s -> Format.fprintf ppf "Tcp_refused(%s)" s
    | Dns_failed s -> Format.fprintf ppf "Dns_failed(%s)" s
    | Tls_failed s -> Format.fprintf ppf "Tls_failed(%s)" s
    | Handshake_rejected e ->
        Format.fprintf ppf "Handshake_rejected(%a)" Valkey_error.pp e
    | Auth_failed e -> Format.fprintf ppf "Auth_failed(%a)" Valkey_error.pp e
    | Protocol_violation s -> Format.fprintf ppf "Protocol_violation(%s)" s
    | Timeout -> Format.pp_print_string ppf "Timeout"
    | Interrupted -> Format.pp_print_string ppf "Interrupted"
    | Queue_full -> Format.pp_print_string ppf "Queue_full"
    | Closed -> Format.pp_print_string ppf "Closed"
    | Server_error e -> Format.fprintf ppf "Server_error(%a)" Valkey_error.pp e
    | Terminal s -> Format.fprintf ppf "Terminal(%s)" s

  let is_terminal = function
    | Auth_failed _ | Protocol_violation _ | Closed | Terminal _ -> true
    | Handshake_rejected _ | Tls_failed _ -> true
    | Tcp_refused _ | Dns_failed _ | Timeout | Interrupted | Queue_full
    | Server_error _ -> false
end

module Handshake = struct
  type t = {
    protocol : int;
    auth : (string * string) option;
    client_name : string option;
    select_db : int option;
  }
  let default = {
    protocol = 3;
    auth = None;
    client_name = None;
    select_db = None;
  }
end

module Reconnect = struct
  type t = {
    initial_backoff : float;
    max_backoff : float;
    jitter : float;
    max_attempts : int option;
    max_total : float option;
    handshake_timeout : float;
  }
  let default = {
    initial_backoff = 0.1;
    max_backoff = 30.0;
    jitter = 0.3;
    max_attempts = None;
    max_total = None;
    handshake_timeout = 5.0;
  }
end

module Config = struct
  type t = {
    handshake : Handshake.t;
    reconnect : Reconnect.t;
    command_timeout : float option;
    keepalive_interval : float option;
    push_buffer_size : int;
    max_queued_bytes : int;
    tls : Tls_config.t option;
  }
  let default = {
    handshake = Handshake.default;
    reconnect = Reconnect.default;
    command_timeout = None;
    (* Some 30s by default: well under typical NAT / LB idle timeouts (60-300s),
       low enough overhead that it's noise on any real workload. Set to None
       to opt out when the application has constant traffic and keepalive is
       pure burden. *)
    keepalive_interval = Some 30.0;
    push_buffer_size = 1024;
    max_queued_bytes = 10 * 1024 * 1024;
    tls = None;
  }
end

type socket = {
  write : string -> unit;
  reader : Eio.Buf_read.t;
  close : unit -> unit;
}

type entry = {
  resolver : (Resp3.t, Error.t) result Eio.Promise.u;
  size : int;
  mutable abandoned : bool;
}

type queued = {
  entry : entry;
  bytes : string;
}

type state =
  | Connecting
  | Alive
  | Recovering
  | Dead of Error.t

type t = {
  mutable state : state;
  mutable current : socket option;
  mutable unsent : queued option;
  state_mutex : Eio.Mutex.t;
  state_changed : Eio.Condition.t;
  sent : entry Queue.t;
  cmd_queue : queued Chan.t;
  budget : Byte_sem.t;
  config : Config.t;
  pushes : Resp3.t Eio.Stream.t;
  connect_once : unit -> (socket, Error.t) result;
  sleep : float -> unit;
  now : unit -> float;
  with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result;
  mutable server_info : Resp3.t option;
  mutable availability_zone : string option;
  mutable closing : bool;
  mutable keepalive_count : int;
  cancel_signal : unit Eio.Promise.t;
  cancel_resolver : unit Eio.Promise.u;
}
[@@warning "-69"]

exception Handshake_failed of Error.t

let hello_args (hs : Handshake.t) =
  let base = [ "HELLO"; string_of_int hs.protocol ] in
  let with_auth =
    match hs.auth with
    | Some (u, p) -> base @ [ "AUTH"; u; p ]
    | None -> base
  in
  let with_name =
    match hs.client_name with
    | Some n -> with_auth @ [ "SETNAME"; n ]
    | None -> with_auth
  in
  Array.of_list with_name

let classify_handshake_error (ve : Valkey_error.t) : Error.t =
  match ve.code with
  | "WRONGPASS" | "NOAUTH" | "NOPERM" -> Auth_failed ve
  | _ -> Handshake_rejected ve

let extract_string_field key = function
  | Resp3.Map kvs ->
      List.find_map
        (fun (k, v) ->
          match k, v with
          | Resp3.Bulk_string k', Resp3.Bulk_string v' when k' = key ->
              Some v'
          | _ -> None)
        kvs
  | _ -> None

let run_hello (sock : socket) (hs : Handshake.t) : (Resp3.t, Error.t) result =
  sock.write (Resp3_writer.command_to_string (hello_args hs));
  match Resp3_parser.read sock.reader with
  | Map _ as m -> Ok m
  | Simple_error s | Bulk_error s ->
      Error (classify_handshake_error (Valkey_error.of_string s))
  | other ->
      Error
        (Protocol_violation
           (Format.asprintf "HELLO returned %a" Resp3.pp other))
  | exception Resp3_parser.Parse_error msg ->
      Error (Protocol_violation msg)

let run_select (sock : socket) db : (unit, Error.t) result =
  sock.write
    (Resp3_writer.command_to_string [| "SELECT"; string_of_int db |]);
  match Resp3_parser.read sock.reader with
  | Simple_string "OK" -> Ok ()
  | Simple_error s | Bulk_error s ->
      Error (Handshake_rejected (Valkey_error.of_string s))
  | other ->
      Error
        (Protocol_violation
           (Format.asprintf "SELECT returned %a" Resp3.pp other))
  | exception Resp3_parser.Parse_error msg ->
      Error (Protocol_violation msg)

let full_handshake (sock : socket) (hs : Handshake.t) :
    (Resp3.t * string option, Error.t) result =
  match run_hello sock hs with
  | Error _ as e -> e
  | Ok hello_map ->
      let az = extract_string_field "availability_zone" hello_map in
      (match hs.select_db with
       | None -> Ok (hello_map, az)
       | Some db ->
           (match run_select sock db with
            | Ok () -> Ok (hello_map, az)
            | Error e -> Error e))

let wrap_with_tls ~tls_cfg sock =
  match
    Tls.Config.client ~authenticator:(Tls_config.authenticator tls_cfg) ()
  with
  | Error (`Msg m) -> Error (Error.Tls_failed ("client config: " ^ m))
  | Ok client_cfg ->
      (try
         let flow =
           Tls_eio.client_of_flow client_cfg
             ?host:(Tls_config.server_name tls_cfg)
             sock
         in
         Ok flow
       with exn -> Error (Error.Tls_failed (Printexc.to_string exn)))

let make_tcp_connector ~sw ~net ~host ~port ~tls =
  fun () ->
    match
      try
        Ok (Eio.Net.getaddrinfo_stream ~service:(string_of_int port) net host)
      with exn -> Error (Error.Dns_failed (Printexc.to_string exn))
    with
    | Error e -> Error e
    | Ok [] -> Error (Error.Dns_failed host)
    | Ok (addr :: _) ->
        (try
           let tcp = Eio.Net.connect ~sw net addr in
           match tls with
           | None ->
               let reader =
                 Eio.Buf_read.of_flow ~max_size:(16 * 1024 * 1024) tcp
               in
               let write bytes = Eio.Flow.copy_string bytes tcp in
               let close () = try Eio.Resource.close tcp with _ -> () in
               Ok { write; reader; close }
           | Some tls_cfg ->
               (match wrap_with_tls ~tls_cfg tcp with
                | Error e ->
                    (try Eio.Resource.close tcp with _ -> ());
                    Error e
                | Ok flow ->
                    let reader =
                      Eio.Buf_read.of_flow ~max_size:(16 * 1024 * 1024) flow
                    in
                    let write bytes = Eio.Flow.copy_string bytes flow in
                    let close () =
                      try Eio.Resource.close flow with _ -> ()
                    in
                    Ok { write; reader; close })
         with exn -> Error (Error.Tcp_refused (Printexc.to_string exn)))

let connect_and_handshake t : (socket * Resp3.t * string option, Error.t) result =
  match t.connect_once () with
  | Error e -> Error e
  | Ok sock ->
      (match full_handshake sock t.config.handshake with
       | Ok (info, az) -> Ok (sock, info, az)
       | Error e ->
           sock.close ();
           Error e)

let set_state t new_state =
  t.state <- new_state;
  Eio.Condition.broadcast t.state_changed

let resolve_entry t (e : entry) (result : (Resp3.t, Error.t) result) =
  Byte_sem.release t.budget e.size;
  if not e.abandoned then Eio.Promise.resolve e.resolver result

let drain_sent t (result : (Resp3.t, Error.t) result) =
  Queue.iter (fun e -> resolve_entry t e result) t.sent;
  Queue.clear t.sent

let drain_unsent t (result : (Resp3.t, Error.t) result) =
  match t.unsent with
  | None -> ()
  | Some q ->
      t.unsent <- None;
      resolve_entry t q.entry result

let drain_cmd_queue t (result : (Resp3.t, Error.t) result) =
  let leftovers = Chan.drain t.cmd_queue in
  Queue.iter (fun q -> resolve_entry t q.entry result) leftovers

let jittered_backoff (policy : Reconnect.t) attempts =
  let base =
    Float.min
      (policy.initial_backoff *. (2.0 ** float_of_int attempts))
      policy.max_backoff
  in
  let amount = base *. policy.jitter in
  base +. (Random.float (2.0 *. amount)) -. amount

let write_worker (t : t) (sock : socket) : exn option =
  let rec loop () =
    if t.closing then None
    else begin
      let q =
        match t.unsent with
        | Some q -> t.unsent <- None; q
        | None ->
            (try Chan.take t.cmd_queue
             with Chan.Closed -> raise Exit)
      in
      let write_result =
        try Ok (sock.write q.bytes)
        with exn -> Error exn
      in
      match write_result with
      | Error exn ->
          t.unsent <- Some q;
          Some exn
      | Ok () ->
          Queue.push q.entry t.sent;
          loop ()
    end
  in
  try loop ()
  with Exit -> None

let read_worker (t : t) (sock : socket) : [ `Closed | `Error ] =
  let rec loop () =
    if t.closing then `Closed
    else
      let outcome =
        try
          Eio.Fiber.first
            (fun () -> `Read (Resp3_parser.read sock.reader))
            (fun () ->
              Eio.Promise.await t.cancel_signal;
              `Cancelled)
        with
        | End_of_file | Resp3_parser.Parse_error _ | Eio.Io _ -> `Err
        | _ -> `Err
      in
      match outcome with
      | `Cancelled -> `Closed
      | `Err -> `Error
      | `Read (Push _ as p) ->
          Eio.Stream.add t.pushes p;
          loop ()
      | `Read value ->
          (match Queue.take_opt t.sent with
           | Some e ->
               let result =
                 match value with
                 | Resp3.Simple_error s | Resp3.Bulk_error s ->
                     Error (Error.Server_error (Valkey_error.of_string s))
                 | v -> Ok v
               in
               resolve_entry t e result
           | None -> ());
          loop ()
  in
  loop ()

let run_connection (t : t) (sock : socket) : [ `Closed | `Disconnected ] =
  let outcome =
    try
      Eio.Fiber.first
        (fun () ->
          match read_worker t sock with
          | `Closed -> `Reader_closed
          | `Error -> `Reader_error)
        (fun () ->
          match write_worker t sock with
          | None -> `Writer_closed
          | Some _ -> `Writer_error)
    with _ -> `Reader_error
  in
  match outcome with
  | `Reader_closed | `Writer_closed -> `Closed
  | `Reader_error | `Writer_error -> `Disconnected

let recovery_loop (t : t) : unit =
  let policy = t.config.reconnect in
  let start = t.now () in
  let rec attempt n =
    let timed_out_total =
      match policy.max_total with
      | None -> false
      | Some max_total -> t.now () -. start > max_total
    in
    let maxed_attempts =
      match policy.max_attempts with
      | None -> false
      | Some m -> n >= m
    in
    if t.closing then ()
    else if timed_out_total || maxed_attempts then
      Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
          set_state t (Dead (Terminal "reconnect budget exhausted"));
          drain_sent t (Error Error.Interrupted);
          drain_unsent t (Error Error.Interrupted);
          drain_cmd_queue t (Error Error.Interrupted))
    else
      match
        t.with_timeout policy.handshake_timeout (fun () ->
            connect_and_handshake t)
      with
      | Error `Timeout ->
          t.sleep (jittered_backoff policy n);
          attempt (n + 1)
      | Ok (Error e) when Error.is_terminal e ->
          Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
              set_state t (Dead e);
              drain_sent t (Error e);
              drain_unsent t (Error e);
              drain_cmd_queue t (Error e))
      | Ok (Error _) ->
          t.sleep (jittered_backoff policy n);
          attempt (n + 1)
      | Ok (Ok (sock, info, az)) ->
          Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
              drain_sent t (Error Error.Interrupted);
              t.current <- Some sock;
              t.server_info <- Some info;
              t.availability_zone <- az;
              set_state t Alive)
  in
  attempt 0

let try_enqueue t q : [ `Dead of Error.t | `In_queue ] =
  match
    Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
        match t.state with
        | Dead e -> `Dead e
        | _ -> `Ok)
  with
  | `Dead e -> `Dead e
  | `Ok ->
      (try
         Chan.push t.cmd_queue q;
         `In_queue
       with Chan.Closed -> `Dead Error.Closed)

let request ?timeout t args =
  let timeout =
    match timeout with
    | Some _ as v -> v
    | None -> t.config.command_timeout
  in
  let bytes = Resp3_writer.command_to_string args in
  let size = String.length bytes in
  if size > t.config.max_queued_bytes then Error Error.Queue_full
  else
    let run () =
      Byte_sem.acquire t.budget size;
      let promise, resolver = Eio.Promise.create () in
      let entry = { resolver; size; abandoned = false } in
      match try_enqueue t { entry; bytes } with
      | `Dead e ->
          Byte_sem.release t.budget size;
          Error e
      | `In_queue ->
          (try Eio.Promise.await promise
           with exn ->
             entry.abandoned <- true;
             raise exn)
    in
    match timeout with
    | None -> run ()
    | Some secs ->
        (match t.with_timeout secs run with
         | Ok v -> v
         | Error `Timeout -> Error Error.Timeout)

let keepalive_loop t interval =
  let rec loop () =
    if t.closing then ()
    else
      let cancelled =
        Eio.Fiber.first
          (fun () -> t.sleep interval; false)
          (fun () -> Eio.Promise.await t.cancel_signal; true)
      in
      if cancelled || t.closing then ()
      else (
        (match t.state with
         | Alive ->
             (match request t [| "PING" |] with
              | Ok _ -> t.keepalive_count <- t.keepalive_count + 1
              | Error _ -> ())
         | _ -> ());
        loop ())
  in
  try loop () with _ -> ()

let rec supervisor_run t =
  if t.closing then ()
  else
    let state, current =
      Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
          t.state, t.current)
    in
    match state, current with
    | Dead _, _ -> ()
    | Alive, Some sock ->
        let _ = run_connection t sock in
        if t.closing then ()
        else (
          (try sock.close () with _ -> ());
          Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
              t.current <- None;
              set_state t Recovering);
          recovery_loop t;
          supervisor_run t)
    | _ -> ()

let connect ~sw ~net ~clock ?(config = Config.default) ~host ~port () =
  let connect_once = make_tcp_connector ~sw ~net ~host ~port ~tls:config.tls in
  let sleep d = Eio.Time.sleep clock d in
  let now () = Eio.Time.now clock in
  let with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result =
    fun d f ->
      match Eio.Time.with_timeout clock d (fun () -> Ok (f ())) with
      | Ok v -> Ok v
      | Error `Timeout -> Error `Timeout
  in
  let cancel_signal, cancel_resolver = Eio.Promise.create () in
  let t : t = {
    state = Connecting;
    current = None;
    unsent = None;
    state_mutex = Eio.Mutex.create ();
    state_changed = Eio.Condition.create ();
    sent = Queue.create ();
    cmd_queue = Chan.create ();
    budget = Byte_sem.make config.max_queued_bytes;
    config;
    pushes = Eio.Stream.create config.push_buffer_size;
    connect_once;
    sleep;
    now;
    with_timeout;
    server_info = None;
    availability_zone = None;
    closing = false;
    keepalive_count = 0;
    cancel_signal;
    cancel_resolver;
  }
  in
  (match
     with_timeout config.reconnect.handshake_timeout (fun () ->
         connect_and_handshake t)
   with
   | Error `Timeout ->
       raise (Handshake_failed (Terminal "initial handshake timeout"))
   | Ok (Error e) -> raise (Handshake_failed e)
   | Ok (Ok (sock, info, az)) ->
       t.current <- Some sock;
       t.server_info <- Some info;
       t.availability_zone <- az;
       t.state <- Alive);
  Eio.Fiber.fork ~sw (fun () -> supervisor_run t);
  (match config.keepalive_interval with
   | None -> ()
   | Some interval ->
       Eio.Fiber.fork ~sw (fun () -> keepalive_loop t interval));
  t

let pushes t = t.pushes
let availability_zone t = t.availability_zone
let server_info t = t.server_info
let state t = t.state
let keepalive_count t = t.keepalive_count

let close t =
  t.closing <- true;
  (if not (Eio.Promise.is_resolved t.cancel_signal) then
     Eio.Promise.resolve t.cancel_resolver ());
  Chan.close t.cmd_queue;
  let sock_to_close =
    Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
        let c = t.current in
        t.current <- None;
        (match t.state with
         | Dead _ -> ()
         | _ -> set_state t (Dead Closed));
        drain_sent t (Error Closed);
        drain_unsent t (Error Closed);
        drain_cmd_queue t (Error Closed);
        c)
  in
  match sock_to_close with
  | Some sock -> (try sock.close () with _ -> ())
  | None -> ()
