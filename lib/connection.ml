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
    close_signal : unit Eio.Promise.t;
    close_resolver : unit Eio.Promise.u;
  }

  exception Closed

  (* Eio.Stream is bounded by count. We want byte-budget to be the real
     backpressure; pick a very large count cap so the stream never blocks
     on count in realistic workloads. *)
  let capacity = 1_000_000

  let create () =
    let close_signal, close_resolver = Eio.Promise.create () in
    { stream = Eio.Stream.create capacity;
      closed = false;
      closed_mutex = Eio.Mutex.create ();
      close_signal;
      close_resolver }

  let push t v =
    Eio.Mutex.use_rw ~protect:true t.closed_mutex (fun () ->
        if t.closed then raise Closed);
    Eio.Stream.add t.stream v

  let take t =
    Eio.Mutex.use_rw ~protect:true t.closed_mutex (fun () ->
        if t.closed then raise Closed);
    match
      Eio.Fiber.first
        (fun () -> `Value (Eio.Stream.take t.stream))
        (fun () ->
          Eio.Promise.await t.close_signal;
          `Closed)
    with
    | `Value v -> v
    | `Closed -> raise Closed

  let close t =
    Eio.Mutex.use_rw ~protect:true t.closed_mutex (fun () ->
        if not t.closed then begin
          t.closed <- true;
          Eio.Promise.resolve t.close_resolver ()
        end)

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

(* The [Error] variant lives in [Connection_error] so sibling
   modules can reference the type without importing this whole
   module (which depends on [Client_cache], etc.). Kept re-exported
   here so users of [Connection.Error.t] continue to work. *)
module Error = Connection_error

module Circuit_breaker = struct
  module Config = struct
    type t = {
      window_size : float;
      error_threshold : int;
      open_timeout : float;
    }
    let default = {
      window_size = 60.0;
      error_threshold = 10_000;
      open_timeout = 10.0;
    }
  end

  type state = {
    cfg : Config.t;
    mutable phase : [ `Closed | `Open of float | `Half_open of bool ];
    errors : float Queue.t;
    mutable error_count : int;
    mutex : Eio.Mutex.t;
  }

  let make cfg = {
    cfg;
    phase = `Closed;
    errors = Queue.create ();
    error_count = 0;
    mutex = Eio.Mutex.create ();
  }

  let counts_as_error (e : Error.t) =
    match e with
    | Timeout | Interrupted | Queue_full
    | Tcp_refused _ | Dns_failed _ | Tls_failed _ -> true
    | Handshake_rejected _ | Auth_failed _ | Protocol_violation _
    | Server_error _ | Circuit_open | Closed | Terminal _ -> false

  let prune cb now =
    while
      not (Queue.is_empty cb.errors)
      && now -. Queue.peek cb.errors > cb.cfg.window_size
    do
      let _ = Queue.pop cb.errors in
      cb.error_count <- cb.error_count - 1
    done

  let on_entry cb now : [ `Allow | `Allow_probe | `Reject ] =
    Eio.Mutex.use_rw ~protect:true cb.mutex (fun () ->
        prune cb now;
        match cb.phase with
        | `Closed -> `Allow
        | `Open since ->
            if now -. since >= cb.cfg.open_timeout then (
              cb.phase <- `Half_open true;
              `Allow_probe)
            else `Reject
        | `Half_open false ->
            cb.phase <- `Half_open true;
            `Allow_probe
        | `Half_open true -> `Reject)

  let on_result cb ~was_probe now (result : (Resp3.t, Error.t) result) =
    Eio.Mutex.use_rw ~protect:true cb.mutex (fun () ->
        let is_error =
          match result with Error e -> counts_as_error e | _ -> false
        in
        match cb.phase, was_probe with
        | `Half_open _, true ->
            if is_error then cb.phase <- `Open now
            else (
              cb.phase <- `Closed;
              Queue.clear cb.errors;
              cb.error_count <- 0)
        | `Closed, _ when is_error ->
            Queue.push now cb.errors;
            cb.error_count <- cb.error_count + 1;
            if cb.error_count >= cb.cfg.error_threshold then
              cb.phase <- `Open now
        | _ -> ())
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
    circuit_breaker : Circuit_breaker.Config.t option;
    client_cache : Client_cache.t option;
    (** Client-side caching (Phase 8). [None] disables; [Some cfg]
        issues [CLIENT TRACKING ON ...] after each (re)connect per
        [cfg.mode], [cfg.noloop]. Storage is
        [cfg.cache]; later steps wire the invalidator fiber and
        race-safe GET path. *)
  }
  let default = {
    handshake = Handshake.default;
    reconnect = Reconnect.default;
    command_timeout = None;
    keepalive_interval = Some 30.0;
    push_buffer_size = 1024;
    max_queued_bytes = 10 * 1024 * 1024;
    tls = None;
    (* Always on, but defaults tuned so only a genuine catastrophe (>10k
       counted errors in 60s) trips the breaker. Users who want tighter
       protection lower [error_threshold] or shrink [window_size]; users
       who want it off set [circuit_breaker] to None. *)
    circuit_breaker = Some Circuit_breaker.Config.default;
    client_cache = None;
  }
end

type socket = {
  write : Cstruct.t -> unit;
  (* One allocation + blit per command; [Eio.Flow.write] uses
     scatter-gather when the transport supports it. Replaces the
     previous [string -> unit] which forced a Buffer.contents copy
     of the whole command. *)
  reader : Eio.Buf_read.t;              (* only used during handshake *)
  read_into : Cstruct.t -> int;         (* used by in_pump post-handshake *)
  close : unit -> unit;
}

type entry = {
  resolver : (Resp3.t, Error.t) result Eio.Promise.u;
  size : int;
  mutable abandoned : bool;
}

type queued = {
  entries : entry list;
  (* [[]] = fire-and-forget (no reply expected). Used by
     [send_fire_and_forget] for subscribe-mode commands; the
     entries are skipped at both out_pump push-to-sent time
     and drain time.
     A non-empty list of length > 1 represents a pipelined
     atomic submit (e.g. [CLIENT CACHING YES] + read for OPTIN
     CSC). The wire is the concatenation of the per-entry
     frames in order; entries are pushed to [sent] under a
     single mutex acquire so the matching FIFO stays
     adjacent. *)
  wire : Cstruct.t;
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
  sent_mutex : Eio.Mutex.t;                (* guards sent across domains *)
  cmd_queue : queued Chan.t;
  budget : Byte_sem.t;
  config : Config.t;
  pushes : Resp3.t Eio.Stream.t;
  (* Non-invalidation pushes (pubsub deliveries, tracking-redir-broken,
     future server-side pushes) — same stream users consumed before
     CSC landed, semantics unchanged. *)
  invalidations : Invalidation.t Eio.Stream.t;
  (* CSC invalidation pushes, post-classification. Drained by the
     invalidator fiber when client_cache is configured. An Eio.Stream
     with a generous cap so a bursty flush doesn't back-pressure
     the parse worker. *)
  host : string;                            (* for telemetry only *)
  port : int;                               (* for telemetry only *)
  connect_once : unit -> (socket, Error.t) result;
  sleep : float -> unit;
  now : unit -> float;
  with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result;
  dispatch_io : 'a. (unit -> 'a) -> 'a;     (* runs on IO domain if configured *)
  on_connected : unit -> unit;              (* hook fired after every
                                               successful handshake *)
  mutable server_info : Resp3.t option;
  mutable availability_zone : string option;
  closing : bool Atomic.t;
  mutable keepalive_count : int;
  cb : Circuit_breaker.state option;
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
  sock.write (Resp3_writer.command_to_cstruct (hello_args hs));
  match Resp3_parser.read (Resp3_parser.of_buf_read sock.reader) with
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
    (Resp3_writer.command_to_cstruct [| "SELECT"; string_of_int db |]);
  match Resp3_parser.read (Resp3_parser.of_buf_read sock.reader) with
  | Simple_string "OK" -> Ok ()
  | Simple_error s | Bulk_error s ->
      Error (Handshake_rejected (Valkey_error.of_string s))
  | other ->
      Error
        (Protocol_violation
           (Format.asprintf "SELECT returned %a" Resp3.pp other))
  | exception Resp3_parser.Parse_error msg ->
      Error (Protocol_violation msg)

(* Build the CLIENT TRACKING command line from a Client_cache.t
   config. ON + mode-specific options + NOLOOP when requested. *)
let client_tracking_args (cfg : Client_cache.t) : string array =
  let base = [ "CLIENT"; "TRACKING"; "ON" ] in
  let with_mode =
    match cfg.mode with
    | Client_cache.Default -> base
    | Client_cache.Optin -> base @ [ "OPTIN" ]
    | Client_cache.Bcast { prefixes } ->
        let prefix_flags =
          List.concat_map (fun p -> [ "PREFIX"; p ]) prefixes
        in
        base @ prefix_flags @ [ "BCAST" ]
  in
  let with_noloop =
    if cfg.noloop then with_mode @ [ "NOLOOP" ] else with_mode
  in
  Array.of_list with_noloop

(* Issue CLIENT TRACKING during handshake. Fail-closed: if the
   server rejects (older server, ACL denies, typo in prefixes),
   the whole connect fails rather than silently falling back to
   an unconfigured cache. *)
let run_client_tracking (sock : socket) (cfg : Client_cache.t) :
    (unit, Error.t) result =
  sock.write (Resp3_writer.command_to_cstruct (client_tracking_args cfg));
  match Resp3_parser.read (Resp3_parser.of_buf_read sock.reader) with
  | Simple_string "OK" -> Ok ()
  | Simple_error s | Bulk_error s ->
      Error (Handshake_rejected (Valkey_error.of_string s))
  | other ->
      Error
        (Protocol_violation
           (Format.asprintf "CLIENT TRACKING returned %a" Resp3.pp other))
  | exception Resp3_parser.Parse_error msg ->
      Error (Protocol_violation msg)

let full_handshake (sock : socket) (hs : Handshake.t)
    ~(client_cache : Client_cache.t option) :
    (Resp3.t * string option, Error.t) result =
  match run_hello sock hs with
  | Error _ as e -> e
  | Ok hello_map ->
      let az = extract_string_field "availability_zone" hello_map in
      let after_select =
        match hs.select_db with
        | None -> Ok ()
        | Some db -> run_select sock db
      in
      (match after_select with
       | Error e -> Error e
       | Ok () ->
           (match client_cache with
            | None -> Ok (hello_map, az)
            | Some cfg ->
                (match run_client_tracking sock cfg with
                 | Ok () -> Ok (hello_map, az)
                 | Error e -> Error e)))

(* Non-leaking exception describer for [Error.t] payloads.
   [Printexc.to_string] would print constructor args (paths, raw cert
   bytes, internal state). We classify the common cases by hand and
   fall back to the bare constructor name. Errno text from
   [Unix.error_message] is stable and path-free.
   The user matches on the [Error.t] variant for programmatic
   handling; this string is for logs only. *)
let describe_exn = function
  | End_of_file -> "peer_closed"
  | Unix.Unix_error (e, _, _) -> Unix.error_message e
  | Tls_eio.Tls_alert _ -> "tls_alert"
  | Tls_eio.Tls_failure _ -> "tls_failure"
  | Eio.Io _ -> "io_error"
  | exn -> Printexc.exn_slot_name exn

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
       with
       | (Tls_eio.Tls_alert _ | Tls_eio.Tls_failure _
         | End_of_file | Eio.Io _) as exn ->
           Error (Error.Tls_failed (describe_exn exn)))

let make_tcp_connector ~sw ~net ~host ~port ~tls =
  fun () ->
    match
      try
        Ok (Eio.Net.getaddrinfo_stream ~service:(string_of_int port) net host)
      with exn -> Error (Error.Dns_failed (describe_exn exn))
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
               let write cs = Eio.Flow.write tcp [ cs ] in
               let read_into buf = Eio.Flow.single_read tcp buf in
               let close () = try Eio.Resource.close tcp
               with Eio.Io _ | End_of_file | Invalid_argument _
                  | Unix.Unix_error _ -> () in
               Ok { write; reader; read_into; close }
           | Some tls_cfg ->
               (match wrap_with_tls ~tls_cfg tcp with
                | Error e ->
                    (try Eio.Resource.close tcp
               with Eio.Io _ | End_of_file | Invalid_argument _
                  | Unix.Unix_error _ -> ());
                    Error e
                | Ok flow ->
                    let reader =
                      Eio.Buf_read.of_flow ~max_size:(16 * 1024 * 1024) flow
                    in
                    let write cs = Eio.Flow.write flow [ cs ] in
                    let read_into buf = Eio.Flow.single_read flow buf in
                    let close () =
                      try Eio.Resource.close flow
                      with Eio.Io _ | End_of_file | Invalid_argument _
                         | Unix.Unix_error _ -> ()
                    in
                    Ok { write; reader; read_into; close })
         with exn -> Error (Error.Tcp_refused (describe_exn exn)))

let connect_and_handshake t : (socket * Resp3.t * string option, Error.t) result =
  Observability.connect_span
    ~host:t.host ~port:t.port
    ~tls:(t.config.tls <> None)
    ~proto:t.config.handshake.protocol
    (fun _span ->
      match t.connect_once () with
      | Error e -> Error e
      | Ok sock ->
          (match
             full_handshake sock t.config.handshake
               ~client_cache:t.config.client_cache
           with
           | Ok (info, az) -> Ok (sock, info, az)
           | Error e ->
               sock.close ();
               Error e))

let set_state t new_state =
  t.state <- new_state;
  Eio.Condition.broadcast t.state_changed

let resolve_entry t (e : entry) (result : (Resp3.t, Error.t) result) =
  Byte_sem.release t.budget e.size;
  if not e.abandoned then Eio.Promise.resolve e.resolver result

let drain_sent t (result : (Resp3.t, Error.t) result) =
  Eio.Mutex.use_rw ~protect:true t.sent_mutex (fun () ->
      Queue.iter (fun e -> resolve_entry t e result) t.sent;
      Queue.clear t.sent)

let drain_unsent t (result : (Resp3.t, Error.t) result) =
  match t.unsent with
  | None -> ()
  | Some q ->
      t.unsent <- None;
      List.iter (fun e -> resolve_entry t e result) q.entries

let drain_cmd_queue t (result : (Resp3.t, Error.t) result) =
  let leftovers = Chan.drain t.cmd_queue in
  Queue.iter
    (fun q -> List.iter (fun e -> resolve_entry t e result) q.entries)
    leftovers

let jittered_backoff (policy : Reconnect.t) attempts =
  let base =
    Float.min
      (policy.initial_backoff *. (2.0 ** float_of_int attempts))
      policy.max_backoff
  in
  let amount = base *. policy.jitter in
  base +. (Random.float (2.0 *. amount)) -. amount

let out_pump (t : t) (sock : socket) : unit =
  let rec loop () =
    if Atomic.get t.closing then ()
    else begin
      let q =
        match t.unsent with
        | Some q -> t.unsent <- None; q
        | None ->
            (try Chan.take t.cmd_queue
             with Chan.Closed -> raise Exit)
      in
      let write_result =
        try Ok (sock.write q.wire)
        with exn -> Error exn
      in
      match write_result with
      | Error _ ->
          t.unsent <- Some q
          (* leave loop; raise disconnect by returning *)
      | Ok () ->
          (match q.entries with
           | [] -> ()
           | es ->
               Eio.Mutex.use_rw ~protect:true t.sent_mutex (fun () ->
                   List.iter (fun e -> Queue.push e t.sent) es));
          loop ()
    end
  in
  try loop () with Exit -> ()

let in_pump (t : t) (sock : socket) (bytes_chan : Cstruct.t Eio.Stream.t)
    (reader : Byte_reader.t) : unit =
  let rec loop () =
    if Atomic.get t.closing then ()
    else
      (* create_unsafe skips the zero-fill; we overwrite with
         read_into immediately. Free micro-optimisation. *)
      let buf = Cstruct.create_unsafe 8192 in
      match
        try
          let n = sock.read_into buf in
          if n = 0 then `Eof else `N n
        with End_of_file -> `Eof
           | _ -> `Err
      with
      | `Eof | `Err -> ()
      | `N n ->
          let chunk = Cstruct.sub buf 0 n in
          Eio.Stream.add bytes_chan chunk;
          loop ()
  in
  (try loop () with _ -> ());
  Byte_reader.close reader

let parse_worker (t : t) (byte_src : Resp3_parser.byte_source) : unit =
  let rec loop () =
    if Atomic.get t.closing then ()
    else
      let outcome =
        try
          Eio.Fiber.first
            (fun () -> `Read (Resp3_parser.read byte_src))
            (fun () ->
              Eio.Promise.await t.cancel_signal;
              `Cancelled)
        with
        | Byte_reader.End_of_stream
        | Resp3_parser.Parse_error _
        | End_of_file
        | Eio.Io _ -> `Err
        | _ -> `Err
      in
      match outcome with
      | `Cancelled | `Err -> ()
      | `Read (Push _ as p) ->
          (* Route CSC invalidation frames onto a dedicated stream so
             the invalidator fiber can drain them without racing
             pubsub consumers on [pushes]. Everything else (pubsub
             deliveries, tracking-redir-broken, future pushes) stays
             on [pushes] — semantics unchanged for those consumers. *)
          (match Invalidation.of_push p with
           | Some inv -> Eio.Stream.add t.invalidations inv
           | None -> Eio.Stream.add t.pushes p);
          loop ()
      | `Read value ->
          let entry =
            Eio.Mutex.use_rw ~protect:true t.sent_mutex (fun () ->
                Queue.take_opt t.sent)
          in
          (match entry with
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
  let bytes_chan : Cstruct.t Eio.Stream.t = Eio.Stream.create 128 in
  let reader = Byte_reader.create bytes_chan in
  let byte_src = Byte_reader.to_byte_source reader in
  let io_block () =
    (try
       Eio.Fiber.first
         (fun () -> in_pump t sock bytes_chan reader)
         (fun () -> out_pump t sock)
     with _ -> ());
    Byte_reader.close reader
  in
  (try
     Eio.Fiber.first
       (fun () -> t.dispatch_io io_block)
       (fun () -> parse_worker t byte_src)
   with _ -> ());
  if Atomic.get t.closing then `Closed else `Disconnected

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
    if Atomic.get t.closing then ()
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
          (* The server forgot our CLIENT TRACKING context while we
             were disconnected. Any cached entry we own for a key
             that routed through this connection is now un-tracked
             on the server side — an external write won't produce
             an invalidation for us. Conservative fix: clear the
             whole CSC cache on every successful reconnect. Coarse
             (a shard blip in a large cluster costs us every hit
             until the cache warms back up) but correct, and
             matches redis-py. B1's full_handshake has already
             re-issued CLIENT TRACKING on [sock] so future reads
             start tracked. *)
          (match t.config.client_cache with
           | None -> ()
           | Some ccfg -> Cache.clear ccfg.Client_cache.cache);
          Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
              drain_sent t (Error Error.Interrupted);
              t.current <- Some sock;
              t.server_info <- Some info;
              t.availability_zone <- az;
              set_state t Alive);
          (* Fire the reconnect hook outside the state mutex — the
             hook typically calls [send_fire_and_forget], which takes
             its own locks and must not deadlock here. *)
          (try t.on_connected () with _ -> ())
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
  let wire = Resp3_writer.command_to_cstruct args in
  let size = Cstruct.length wire in
  if size > t.config.max_queued_bytes then Error Error.Queue_full
  else
    let cb_decision =
      match t.cb with
      | None -> `Allow
      | Some cb -> Circuit_breaker.on_entry cb (t.now ())
    in
    match cb_decision with
    | `Reject -> Error Error.Circuit_open
    | (`Allow | `Allow_probe) as decision ->
        let was_probe = decision = `Allow_probe in
        let run () =
          Byte_sem.acquire t.budget size;
          let promise, resolver = Eio.Promise.create () in
          let entry = { resolver; size; abandoned = false } in
          match try_enqueue t { entries = [ entry ]; wire } with
          | `Dead e ->
              Byte_sem.release t.budget size;
              Error e
          | `In_queue ->
              (try Eio.Promise.await promise
               with exn ->
                 entry.abandoned <- true;
                 raise exn)
        in
        let result =
          match timeout with
          | None -> run ()
          | Some secs ->
              (match t.with_timeout secs run with
               | Ok v -> v
               | Error `Timeout -> Error Error.Timeout)
        in
        (match t.cb with
         | None -> ()
         | Some cb -> Circuit_breaker.on_result cb ~was_probe (t.now ()) result);
        result

let send_fire_and_forget t args =
  (* Same front-end checks as [request] — circuit breaker, byte
     budget, Closed state — but we enqueue with entry = None so the
     writer does not push onto [t.sent] and no promise is created. *)
  let wire = Resp3_writer.command_to_cstruct args in
  let size = Cstruct.length wire in
  if size > t.config.max_queued_bytes then Error Error.Queue_full
  else
    let cb_decision =
      match t.cb with
      | None -> `Allow
      | Some cb -> Circuit_breaker.on_entry cb (t.now ())
    in
    match cb_decision with
    | `Reject -> Error Error.Circuit_open
    | `Allow | `Allow_probe ->
        Byte_sem.acquire t.budget size;
        match try_enqueue t { entries = []; wire } with
        | `Dead e ->
            Byte_sem.release t.budget size;
            Error e
        | `In_queue ->
            (* Release the budget eagerly — we're not waiting for a
               reply that would free it via resolve_entry. *)
            Byte_sem.release t.budget size;
            Ok ()

(* Pipelined atomic two-frame submit. Builds two wire frames,
   concatenates them into a single [queued] entry with two
   matching-queue resolvers, and enqueues as one indivisible
   unit. The writer flushes both frames back-to-back; the
   parser pulls two replies in order and resolves both
   promises FIFO.

   Used for OPTIN CSC where the spec requires
   [CLIENT CACHING YES] to be sent immediately before the
   tracked read with no other command between them on the
   wire (verified empirically: the flag is consumed by exactly
   the next single command). A pair of separate [request]
   calls would NOT satisfy this — another fiber's enqueue can
   land between them.

   The outer [Error.t] reports failures that prevented the
   submit (Closed, Circuit_open, Queue_full). The inner pair
   carries each frame's reply independently — frame 1 may
   succeed (Simple_string "OK") while frame 2 errors
   (WRONGTYPE etc.), or either may fail with transport
   errors if the connection drops mid-submit. *)
let request_pair ?timeout t args1 args2 =
  let timeout =
    match timeout with
    | Some _ as v -> v
    | None -> t.config.command_timeout
  in
  let wire1 = Resp3_writer.command_to_cstruct args1 in
  let wire2 = Resp3_writer.command_to_cstruct args2 in
  let size1 = Cstruct.length wire1 in
  let size2 = Cstruct.length wire2 in
  let total = size1 + size2 in
  if total > t.config.max_queued_bytes then Error Error.Queue_full
  else
    let cb_decision =
      match t.cb with
      | None -> `Allow
      | Some cb -> Circuit_breaker.on_entry cb (t.now ())
    in
    match cb_decision with
    | `Reject -> Error Error.Circuit_open
    | (`Allow | `Allow_probe) as decision ->
        let was_probe = decision = `Allow_probe in
        let run () =
          Byte_sem.acquire t.budget total;
          let p1, r1 = Eio.Promise.create () in
          let p2, r2 = Eio.Promise.create () in
          let e1 = { resolver = r1; size = size1; abandoned = false } in
          let e2 = { resolver = r2; size = size2; abandoned = false } in
          let wire = Cstruct.append wire1 wire2 in
          match try_enqueue t { entries = [ e1; e2 ]; wire } with
          | `Dead e ->
              Byte_sem.release t.budget total;
              Error e
          | `In_queue ->
              let r1 =
                try Eio.Promise.await p1
                with exn ->
                  e1.abandoned <- true; e2.abandoned <- true;
                  raise exn
              in
              let r2 =
                try Eio.Promise.await p2
                with exn ->
                  e2.abandoned <- true;
                  raise exn
              in
              Ok (r1, r2)
        in
        let result =
          match timeout with
          | None -> run ()
          | Some secs ->
              (match t.with_timeout secs run with
               | Ok v -> v
               | Error `Timeout -> Error Error.Timeout)
        in
        (* Trip the breaker only on transport-level outcomes, the
           same rule [request] applies. A successful submit where
           one inner frame returned Server_error counts as
           transport-success. *)
        let transport_outcome =
          match result with
          | Error e -> Error e
          | Ok ((Error e), _) -> Error e
          | Ok (_, (Error e)) -> Error e
          | Ok (Ok _, Ok _) -> Ok Resp3.Null  (* shape-irrelevant *)
        in
        (match t.cb with
         | None -> ()
         | Some cb ->
             Circuit_breaker.on_result cb ~was_probe (t.now ())
               transport_outcome);
        result

let keepalive_loop t interval =
  let rec loop () =
    if Atomic.get t.closing then ()
    else
      let cancelled =
        Eio.Fiber.first
          (fun () -> t.sleep interval; false)
          (fun () -> Eio.Promise.await t.cancel_signal; true)
      in
      if cancelled || Atomic.get t.closing then ()
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
  if Atomic.get t.closing then ()
  else
    let state, current =
      Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
          t.state, t.current)
    in
    match state, current with
    | Dead _, _ -> ()
    | Alive, Some sock ->
        let _ = run_connection t sock in
        if Atomic.get t.closing then ()
        else (
          (try sock.close ()
           with Eio.Io _ | End_of_file | Invalid_argument _
              | Unix.Unix_error _ -> ());
          Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () ->
              t.current <- None;
              set_state t Recovering);
          recovery_loop t;
          supervisor_run t)
    | _ -> ()

let connect ~sw ~net ~clock ?domain_mgr ?(config = Config.default)
    ?(on_connected = fun () -> ()) ~host ~port () =
  let connect_once = make_tcp_connector ~sw ~net ~host ~port ~tls:config.tls in
  let sleep d = Eio.Time.sleep clock d in
  let now () = Eio.Time.now clock in
  let with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result =
    fun d f ->
      match Eio.Time.with_timeout clock d (fun () -> Ok (f ())) with
      | Ok v -> Ok v
      | Error `Timeout -> Error `Timeout
  in
  let dispatch_io : 'a. (unit -> 'a) -> 'a = fun f ->
    match domain_mgr with
    | None -> f ()
    | Some mgr -> Eio.Domain_manager.run mgr f
  in
  let cancel_signal, cancel_resolver = Eio.Promise.create () in
  let t : t = {
    state = Connecting;
    current = None;
    unsent = None;
    state_mutex = Eio.Mutex.create ();
    state_changed = Eio.Condition.create ();
    sent = Queue.create ();
    sent_mutex = Eio.Mutex.create ();
    cmd_queue = Chan.create ();
    budget = Byte_sem.make config.max_queued_bytes;
    config;
    pushes = Eio.Stream.create config.push_buffer_size;
    invalidations = Eio.Stream.create config.push_buffer_size;
    host;
    port;
    connect_once;
    sleep;
    now;
    with_timeout;
    dispatch_io;
    on_connected;
    server_info = None;
    availability_zone = None;
    closing = Atomic.make false;
    keepalive_count = 0;
    cb = Option.map Circuit_breaker.make config.circuit_breaker;
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
       t.state <- Alive;
       (* Same hook as recovery: fires after the initial handshake
          too, so callers can register their "send on every
          connection" logic once. *)
       (try t.on_connected () with _ -> ()));
  (* Supervisor + keepalive run on the user's domain. run_connection uses
     t.dispatch_io internally so that in_pump + out_pump go to the IO domain
     (if domain_mgr was supplied), while parse_worker stays here. This is
     what keeps a CPU-heavy parser from blocking socket I/O. *)
  Eio.Fiber.fork ~sw (fun () -> supervisor_run t);
  (match config.keepalive_interval with
   | None -> ()
   | Some interval ->
       Eio.Fiber.fork ~sw (fun () -> keepalive_loop t interval));
  (* Invalidator fiber: drain [t.invalidations] and evict/clear the
     user's cache. Only runs when a cache is configured; otherwise
     the stream would fill and backpressure the parser, so we'd
     rather have the fiber present and bored than absent and
     harmful. Cheap — blocks on [take] when idle. *)
  (match config.client_cache with
   | None -> ()
   | Some ccfg ->
       Eio.Fiber.fork ~sw (fun () ->
           let rec loop () =
             if Atomic.get t.closing then ()
             else begin
               let inv =
                 Eio.Fiber.first
                   (fun () -> `Inv (Eio.Stream.take t.invalidations))
                   (fun () ->
                     Eio.Promise.await t.cancel_signal;
                     `Cancelled)
               in
               match inv with
               | `Cancelled -> ()
               | `Inv inv ->
                   Invalidation.apply ccfg.Client_cache.cache
                     ccfg.Client_cache.inflight inv;
                   loop ()
             end
           in
           try loop () with _ -> ()));
  t

let pushes t = t.pushes
let invalidations t = t.invalidations
let availability_zone t = t.availability_zone
let server_info t = t.server_info
let state t = t.state
let keepalive_count t = t.keepalive_count

let interrupt t =
  if Atomic.get t.closing then ()
  else
    let sock_to_close =
      Eio.Mutex.use_rw ~protect:true t.state_mutex (fun () -> t.current)
    in
    match sock_to_close with
    | Some sock ->
        (try sock.close ()
         with Eio.Io _ | End_of_file | Invalid_argument _
            | Unix.Unix_error _ -> ())
    | None -> ()

let close t =
  if Atomic.exchange t.closing true then ()
  else begin
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
  | Some sock -> (try sock.close ()
           with Eio.Io _ | End_of_file | Invalid_argument _
              | Unix.Unix_error _ -> ())
  | None -> ()
  end
