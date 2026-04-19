(* Cluster-aware pub/sub.

   Architecture:

     global            : one Connection, opened eagerly to any
                         primary the router can reach. Handles
                         SUBSCRIBE / PSUBSCRIBE. Valkey 7+
                         broadcasts messages cluster-wide, so any
                         node is fine.

     shards[slot]      : one Connection per distinct slot the user
                         has ssubscribed on. Opened lazily on first
                         [ssubscribe] for that slot. Each entry
                         tracks which primary_id it is bound to.

     watchdog fiber    : polls the router every second. If
                         [endpoint_for_slot] reports a new
                         primary_id for any tracked slot, close the
                         old connection, open a fresh one at the new
                         address, and the new connection's
                         on_connected hook replays that slot's
                         SSUBSCRIBE set.

     per-conn pump     : one fiber per live Connection pulls from
                         [Connection.pushes conn], filters out
                         subscription ACKs, and forwards real
                         deliveries into the shared [incoming]
                         stream.

   The switch passed to [create] owns all the fibers. [close] also
   closes every connection and flips [closing], which tears the
   pumps and watchdog down.

   Locking discipline:

     shards_mutex   — guards structural changes to [t.shards] and
                      to each [shard_entry]'s [conn] / [primary_id]
                      (re-pin path). Held across the lifetime of
                      any re-pin to prevent the watchdog and a
                      concurrent [ssubscribe] from racing on the
                      same slot.

     subs_mutex     — guards the [channels] / [patterns] / shard
                      [channels] lists. Fine-grained: held just
                      across the list mutation, never across I/O.

   Lock ordering: code that needs both acquires [shards_mutex] first,
   then [subs_mutex]. The reverse order never occurs.

   The [conn] field of [shard_entry] and [global_entry] is read
   without a lock on the send path (see [send_prefixed]). That is
   safe because every write to [conn] happens under [shards_mutex]
   whose unlock is a release barrier in OCaml 5; any subsequent
   `Mutex.lock` on a different thread observes the write. Readers
   that don't synchronise may briefly see an older conn, which is
   acceptable — they'll either succeed on the old conn or fail with
   `Closed`, at which point the caller retries. *)

type shard_entry = {
  mutable conn : Connection.t;
  mutable primary_id : string;
  slot : int;
  mutable channels : string list;
  mutable pump_cancel : unit Eio.Promise.u option;
  (* Fulfil the promise to stop the pump fiber for this conn
     without touching global close state. *)
}

type global_entry = {
  mutable conn : Connection.t;
  mutable channels : string list;
  mutable patterns : string list;
  mutable pump_cancel : unit Eio.Promise.u option;
}

type t = {
  router : Router.t;
  sw : Eio.Switch.t;
  closing : bool Atomic.t;
  close_signal : unit Eio.Promise.t;
  close_resolver : unit Eio.Promise.u;
  incoming : Pubsub.message Eio.Stream.t;
  shards : (int, shard_entry) Hashtbl.t;
  shards_mutex : Mutex.t;
  subs_mutex : Mutex.t;
  global : global_entry;
  open_conn :
    host:string -> port:int -> on_connected:(unit -> unit) ->
    Connection.t;
  with_timeout :
    'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result;
  watchdog_interval : float;
}

(* ---------- helpers ---------- *)

let with_mutex m f =
  Mutex.lock m;
  let r = try Ok (f ()) with e -> Error e in
  Mutex.unlock m;
  match r with Ok v -> v | Error e -> raise e

let add_unique lst xs =
  List.fold_left (fun acc x -> if List.mem x acc then acc else x :: acc)
    lst xs

let remove_all lst xs =
  List.filter (fun x -> not (List.mem x xs)) lst

let send_prefixed conn prefix xs =
  let args = Array.of_list (prefix :: xs) in
  Connection.send_fire_and_forget conn args

(* Per-connection pump. Forwards real deliveries into [incoming] and
   swallows ACKs. Exits when the per-entry cancel promise fires, or
   when the global close signal fires. *)
let pump ~incoming ~cancel ~close_signal conn =
  let pushes = Connection.pushes conn in
  let rec loop () =
    let outcome =
      try
        Eio.Fiber.first
          (fun () -> `Push (Eio.Stream.take pushes))
          (fun () -> Eio.Promise.await cancel; `Cancel)
      with _ -> `Cancel
    in
    let outcome =
      if outcome = `Cancel then `Cancel
      else if Eio.Promise.is_resolved close_signal then `Cancel
      else outcome
    in
    match outcome with
    | `Cancel -> ()
    | `Push v ->
        (match Pubsub.classify_push v with
         | `Deliver m -> Eio.Stream.add incoming m
         | `Ack | `Other -> ());
        loop ()
  in
  loop ()

let start_pump t conn ~(assign_cancel : (unit Eio.Promise.u -> unit)) =
  let cancel_signal, cancel_resolver = Eio.Promise.create () in
  assign_cancel cancel_resolver;
  Eio.Fiber.fork ~sw:t.sw (fun () ->
      pump ~incoming:t.incoming ~cancel:cancel_signal
        ~close_signal:t.close_signal conn)

let resolve_cancel = function
  | Some u ->
      (try Eio.Promise.resolve u () with _ -> ())
  | None -> ()

(* ---------- global (non-sharded) path ---------- *)

let replay_global t () =
  let channels, patterns =
    with_mutex t.subs_mutex (fun () ->
        t.global.channels, t.global.patterns)
  in
  let fire cmd xs =
    match xs with
    | [] -> ()
    | _ ->
        (match send_prefixed t.global.conn cmd xs with
         | Ok () -> ()
         | Error _ -> ())
  in
  fire "SUBSCRIBE" channels;
  fire "PSUBSCRIBE" patterns

(* ---------- shard path ---------- *)

let replay_shard t (entry : shard_entry) () =
  let channels =
    with_mutex t.subs_mutex (fun () -> entry.channels)
  in
  match channels with
  | [] -> ()
  | _ ->
      (match send_prefixed entry.conn "SSUBSCRIBE" channels with
       | Ok () -> ()
       | Error _ -> ())

let resolve_endpoint t slot =
  match Router.endpoint_for_slot t.router slot with
  | Some (id, host, port) -> Ok (id, host, port)
  | None ->
      Error
        (Connection.Error.Terminal
           (Printf.sprintf
              "cluster_pubsub: no primary for slot %d" slot))

(* Obtain (or lazily create) a shard_entry for [slot]. [fresh_entry]
   creates the connection + pump and registers the entry in the
   hashtable under the mutex. Idempotent: a second caller will see
   the existing entry and return it. *)
let get_or_create_shard_entry t slot : (shard_entry, Connection.Error.t) result =
  match Hashtbl.find_opt t.shards slot with
  | Some e -> Ok e
  | None ->
      match resolve_endpoint t slot with
      | Error e -> Error e
      | Ok (id, host, port) ->
          (* Build the entry first; [on_connected] closes over it so
             the replay hook sees subsequent channel additions. *)
          let entry : shard_entry = {
            conn = Obj.magic ();  (* placeholder, overwritten below *)
            primary_id = id;
            slot;
            channels = [];
            pump_cancel = None;
          } in
          let on_connected () = replay_shard t entry () in
          let conn = t.open_conn ~host ~port ~on_connected in
          entry.conn <- conn;
          start_pump t conn
            ~assign_cancel:(fun u -> entry.pump_cancel <- Some u);
          Hashtbl.replace t.shards slot entry;
          Ok entry

(* Re-pin a shard to a new primary address. Close the old conn,
   open a new one, restart the pump, rely on on_connected to replay
   SSUBSCRIBE. The entry stays in the hashtable. *)
let repin_shard t (entry : shard_entry) ~new_id ~new_host ~new_port =
  resolve_cancel entry.pump_cancel;
  (try Connection.close entry.conn with _ -> ());
  let on_connected () = replay_shard t entry () in
  let conn = t.open_conn ~host:new_host ~port:new_port ~on_connected in
  entry.conn <- conn;
  entry.primary_id <- new_id;
  start_pump t conn
    ~assign_cancel:(fun u -> entry.pump_cancel <- Some u)

(* The watchdog fiber is defined inline in [create] below so it can
   close over the `clock` and `sw` that `create` receives as
   arguments. See the loop starting at "Eio.Fiber.fork ~sw:t.sw" in
   [create]. *)

(* ---------- public API ---------- *)

let bootstrap_global_connection t =
  (* Pick any slot the router knows about, use its primary as the
     home for SUBSCRIBE/PSUBSCRIBE. In Valkey 7+ messages broadcast
     across shards, so any node works. *)
  let home_endpoint =
    match Router.endpoint_for_slot t.router 0 with
    | Some (_, h, p) -> Some (h, p)
    | None ->
        (* Try a few other slots in case slot 0 is unowned transiently. *)
        let rec try_slot i =
          if i >= 16384 then None
          else
            match Router.endpoint_for_slot t.router i with
            | Some (_, h, p) -> Some (h, p)
            | None -> try_slot (i + 4096)
        in
        try_slot 4096
  in
  match home_endpoint with
  | None ->
      failwith
        "cluster_pubsub: router reports no primary endpoint for \
         any slot — cannot open a subscriber connection"
  | Some (host, port) ->
      let on_connected () = replay_global t () in
      let conn = t.open_conn ~host ~port ~on_connected in
      t.global.conn <- conn;
      start_pump t conn
        ~assign_cancel:(fun u -> t.global.pump_cancel <- Some u)

let create ~sw ~net ~clock ?domain_mgr
    ?(connection_config = Connection.Config.default) ~router () =
  let close_signal, close_resolver = Eio.Promise.create () in
  let with_timeout : 'a. float -> (unit -> 'a) -> ('a, [ `Timeout ]) result =
    fun secs f ->
      match Eio.Time.with_timeout clock secs (fun () -> Ok (f ())) with
      | Ok v -> Ok v
      | Error `Timeout -> Error `Timeout
  in
  let open_conn ~host ~port ~on_connected =
    Connection.connect ~sw ~net ~clock ?domain_mgr
      ~config:connection_config ~on_connected ~host ~port ()
  in
  let t = {
    router;
    sw;
    closing = Atomic.make false;
    close_signal; close_resolver;
    incoming = Eio.Stream.create 1024;
    shards = Hashtbl.create 16;
    shards_mutex = Mutex.create ();
    subs_mutex = Mutex.create ();
    global = {
      conn = Obj.magic ();   (* set below by bootstrap *)
      channels = [];
      patterns = [];
      pump_cancel = None;
    };
    open_conn;
    with_timeout;
    watchdog_interval = 1.0;
  } in
  bootstrap_global_connection t;
  Eio.Fiber.fork ~sw:t.sw (fun () ->
      (* Use a local helper instead of relying on the placeholder in
         [watchdog]'s body. *)
      let rec loop () =
        if Atomic.get t.closing then ()
        else begin
          (try
             let _ : [ `Tick | `Cancel ] =
               Eio.Fiber.first
                 (fun () ->
                   Eio.Time.sleep clock t.watchdog_interval;
                   `Tick)
                 (fun () ->
                   Eio.Promise.await t.close_signal; `Cancel)
             in
             if not (Atomic.get t.closing) then
               let entries =
                 with_mutex t.shards_mutex (fun () ->
                     Hashtbl.fold (fun _ e acc -> e :: acc)
                       t.shards [])
               in
               List.iter
                 (fun entry ->
                   match
                     Router.endpoint_for_slot t.router entry.slot
                   with
                   | Some (id, host, port)
                     when id <> entry.primary_id ->
                       (try
                          with_mutex t.shards_mutex (fun () ->
                              repin_shard t entry
                                ~new_id:id ~new_host:host
                                ~new_port:port)
                        with _ -> ())
                   | _ -> ())
                 entries
           with _ -> ());
          loop ()
        end
      in
      try loop () with _ -> ());
  t


let close t =
  if not (Atomic.exchange t.closing true) then begin
    (try Eio.Promise.resolve t.close_resolver () with _ -> ());
    (* Cancel the pump fibers — each one will notice the resolved
       cancel promise and exit its Fiber.first race. *)
    resolve_cancel t.global.pump_cancel;
    with_mutex t.shards_mutex (fun () ->
        Hashtbl.iter
          (fun _ (e : shard_entry) -> resolve_cancel e.pump_cancel)
          t.shards);
    (try Connection.close t.global.conn with _ -> ());
    with_mutex t.shards_mutex (fun () ->
        Hashtbl.iter
          (fun _ (e : shard_entry) ->
            try Connection.close e.conn with _ -> ())
          t.shards)
  end

let subscribe t cs =
  if cs = [] then Ok ()
  else begin
    with_mutex t.subs_mutex (fun () ->
        t.global.channels <- add_unique t.global.channels cs);
    send_prefixed t.global.conn "SUBSCRIBE" cs
  end

let unsubscribe t cs =
  with_mutex t.subs_mutex (fun () ->
      t.global.channels <-
        (if cs = [] then [] else remove_all t.global.channels cs));
  send_prefixed t.global.conn "UNSUBSCRIBE" cs

let psubscribe t ps =
  if ps = [] then Ok ()
  else begin
    with_mutex t.subs_mutex (fun () ->
        t.global.patterns <- add_unique t.global.patterns ps);
    send_prefixed t.global.conn "PSUBSCRIBE" ps
  end

let punsubscribe t ps =
  with_mutex t.subs_mutex (fun () ->
      t.global.patterns <-
        (if ps = [] then [] else remove_all t.global.patterns ps));
  send_prefixed t.global.conn "PUNSUBSCRIBE" ps

(* Group channels by the slot their primary owns. Used to dispatch
   one SSUBSCRIBE per (slot, connection) pair. *)
let group_by_slot channels =
  let h = Hashtbl.create 8 in
  List.iter
    (fun ch ->
      let slot = Slot.of_key ch in
      let existing =
        try Hashtbl.find h slot with Not_found -> []
      in
      Hashtbl.replace h slot (ch :: existing))
    channels;
  Hashtbl.fold (fun slot chs acc -> (slot, List.rev chs) :: acc) h []

let ssubscribe t channels =
  if channels = [] then Ok ()
  else
    let groups = group_by_slot channels in
    let rec go = function
      | [] -> Ok ()
      | (slot, chs) :: rest ->
          (match
             with_mutex t.shards_mutex (fun () ->
                 get_or_create_shard_entry t slot)
           with
           | Error e -> Error e
           | Ok entry ->
               with_mutex t.subs_mutex (fun () ->
                   entry.channels <- add_unique entry.channels chs);
               (match send_prefixed entry.conn "SSUBSCRIBE" chs with
                | Error e -> Error e
                | Ok () -> go rest))
    in
    go groups

let sunsubscribe t channels =
  let groups = group_by_slot channels in
  let errs = ref [] in
  List.iter
    (fun (slot, chs) ->
      match
        with_mutex t.shards_mutex (fun () ->
            Hashtbl.find_opt t.shards slot)
      with
      | None -> ()
      | Some entry ->
          with_mutex t.subs_mutex (fun () ->
              entry.channels <- remove_all entry.channels chs);
          (match send_prefixed entry.conn "SUNSUBSCRIBE" chs with
           | Ok () -> ()
           | Error e -> errs := e :: !errs))
    groups;
  match !errs with
  | [] -> Ok ()
  | e :: _ -> Error e

let next_message ?timeout t =
  if Atomic.get t.closing then Error `Closed
  else
    match timeout with
    | None ->
        (try Ok (Eio.Stream.take t.incoming)
         with _ -> Error `Closed)
    | Some secs ->
        (match t.with_timeout secs
                 (fun () -> Eio.Stream.take t.incoming) with
         | Ok v -> Ok v
         | Error `Timeout -> Error `Timeout)
