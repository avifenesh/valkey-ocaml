(* Narrow lease pool for blocking Valkey commands. Design is
   the narrowest thing that works:

   - Per-node buckets. A bucket is
       { sem : Eio.Semaphore.t   (capacity = max_per_node)
       ; idle : (Connection.t * float) Queue.t   (LIFO; age
             tracked so [max_idle_age] can evict)
       ; generation : int Atomic.t  (bumped on refresh, set
             [`Drained] after [drain_node])
       ; counters : Stats.t record
       ; lock : Eio.Mutex.t }
     Lock guards [idle] + bucket-local counters. Semaphore is
     independent — acquired before the lock on borrow, and the
     idle queue is allowed to be empty (we'll build a fresh
     conn).
   - Borrow order:
       1. [Eio.Semaphore.acquire] (bounded by [max_per_node]).
          Under [`Fail_fast] we use non-blocking try + fall
          through to [Pool_exhausted]; under [`Block] we
          [Eio.Fiber.first] with a timeout promise.
       2. Lock bucket, pop idle queue; if empty, drop the lock
          and open a fresh Connection (handshake outside the
          lock — slow).
       3. Snapshot the bucket's [generation] on the conn for
          return-time staleness check.
   - Return order:
       1. If the lease raised / was cancelled / generation
          stale / node drained: [Connection.close] and skip
          re-idle.
       2. Else push back onto idle queue, release semaphore.

   Pool is [t.closed = true] after [close]: semaphore refuses
   every new borrow with [Pool_exhausted], idle conns are
   closed, in-flight leases close on return.

   The [idle] queue carries insertion timestamps so
   [max_idle_age] eviction can close old conns without a
   sweeper fiber — every borrow-miss that opens a fresh conn
   also evicts any too-old entries it walks past. A dedicated
   sweeper would be more responsive; we don't need one until a
   user complains. *)

module Config = struct
  type t = {
    max_per_node : int;
    min_idle_per_node : int;
    borrow_timeout : float option;
    on_exhaustion : [ `Block | `Fail_fast ];
    max_idle_age : float option;
  }

  let default = {
    max_per_node = 0;
    min_idle_per_node = 0;
    borrow_timeout = Some 5.0;
    on_exhaustion = `Fail_fast;
    max_idle_age = Some 300.0;
  }
end

module Stats = struct
  type t = {
    in_use : int;
    idle : int;
    waiters : int;
    total_borrowed : int;
    total_created : int;
    total_closed_dirty : int;
    total_borrow_timeouts : int;
    total_exhaustion_rejects : int;
  }

  let empty = {
    in_use = 0;
    idle = 0;
    waiters = 0;
    total_borrowed = 0;
    total_created = 0;
    total_closed_dirty = 0;
    total_borrow_timeouts = 0;
    total_exhaustion_rejects = 0;
  }
end

type borrow_error =
  | Pool_not_configured
  | Pool_exhausted
  | Borrow_timeout
  | Node_gone
  | Connect_failed of Connection.Error.t

let pp_borrow_error fmt = function
  | Pool_not_configured ->
      Format.fprintf fmt "Blocking_pool: not configured \
                          (max_per_node = 0)"
  | Pool_exhausted ->
      Format.fprintf fmt "Blocking_pool: exhausted (at max_per_node)"
  | Borrow_timeout ->
      Format.fprintf fmt "Blocking_pool: borrow_timeout elapsed"
  | Node_gone -> Format.fprintf fmt "Blocking_pool: node drained"
  | Connect_failed e ->
      Format.fprintf fmt "Blocking_pool: connect failed: %a"
        Connection.Error.pp e

(* A single idle conn carries its insertion time for
   [max_idle_age] eviction and the generation it was tagged
   with on the last return. Snapshot of the bucket's generation
   is cheap; comparing on use catches failover/refresh between
   idle and next borrow. *)
type idle_entry = {
  conn : Connection.t;
  idle_since : float;
  generation : int;
}

type bucket = {
  mutable state : [ `Active | `Drained ];
  sem : Eio.Semaphore.t;
  idle : idle_entry Queue.t;
  generation : int Atomic.t;
  mutable in_use : int;
  mutable total_borrowed : int;
  mutable total_created : int;
  mutable total_closed_dirty : int;
  mutable total_borrow_timeouts : int;
  mutable total_exhaustion_rejects : int;
  mutable waiters : int;
  lock : Eio.Mutex.t;
}

let make_bucket ~max_per_node =
  { state = `Active;
    sem = Eio.Semaphore.make max_per_node;
    idle = Queue.create ();
    generation = Atomic.make 0;
    in_use = 0;
    total_borrowed = 0;
    total_created = 0;
    total_closed_dirty = 0;
    total_borrow_timeouts = 0;
    total_exhaustion_rejects = 0;
    waiters = 0;
    lock = Eio.Mutex.create () }

let snapshot_bucket_stats b =
  Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
      { Stats.in_use = b.in_use;
        idle = Queue.length b.idle;
        waiters = b.waiters;
        total_borrowed = b.total_borrowed;
        total_created = b.total_created;
        total_closed_dirty = b.total_closed_dirty;
        total_borrow_timeouts = b.total_borrow_timeouts;
        total_exhaustion_rejects = b.total_exhaustion_rejects })

type t = {
  sw : Eio.Switch.t;
  net : [ `Generic | `Unix ] Eio.Net.ty Eio.Resource.t;
  clock : float Eio.Time.clock_ty Eio.Resource.t;
  domain_mgr : Eio.Domain_manager.ty Eio.Resource.t option;
  config : Config.t;
  connection_config : Connection.Config.t;
  (* Pool conns MUST NOT enable CLIENT TRACKING — they never
     receive CSC-tracked reads. [create] strips the cache
     setting from the supplied connection_config so a caller
     who builds a config from their Client.Config.t can't
     accidentally plumb tracking through. *)
  endpoint_for_node : node_id:string -> (string * int) option;
  buckets : (string, bucket) Hashtbl.t;
  buckets_mutex : Eio.Mutex.t;
  mutable closed : bool;
}

let strip_client_cache (cfg : Connection.Config.t) =
  { cfg with client_cache = None }

let create ~sw ~net ~clock ?domain_mgr ~config ~connection_config
    ~endpoint_for_node () =
  let net =
    (net :> [ `Generic | `Unix ] Eio.Net.ty Eio.Resource.t)
  in
  let clock =
    (clock :> float Eio.Time.clock_ty Eio.Resource.t)
  in
  let domain_mgr =
    Option.map
      (fun dm ->
        (dm :> Eio.Domain_manager.ty Eio.Resource.t))
      domain_mgr
  in
  { sw;
    net;
    clock;
    domain_mgr;
    config;
    connection_config = strip_client_cache connection_config;
    endpoint_for_node;
    buckets = Hashtbl.create 16;
    buckets_mutex = Eio.Mutex.create ();
    closed = false }

(* Get-or-create the bucket for [node_id]. Bucket creation is
   rare (once per node in the cluster lifetime) so the shared
   mutex here is fine. *)
let bucket_for t node_id =
  Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
      match Hashtbl.find_opt t.buckets node_id with
      | Some b -> b
      | None ->
          let b = make_bucket ~max_per_node:t.config.max_per_node in
          Hashtbl.add t.buckets node_id b;
          b)

let close_quietly c =
  try Connection.close c
  with Eio.Io _ | End_of_file | Invalid_argument _
     | Unix.Unix_error _ -> ()

let open_fresh_conn t node_id :
    (Connection.t, borrow_error) result =
  match t.endpoint_for_node ~node_id with
  | None -> Error Node_gone
  | Some (host, port) ->
      (try
         Ok (Connection.connect
               ~sw:t.sw ~net:t.net ~clock:t.clock
               ?domain_mgr:t.domain_mgr
               ~config:t.connection_config
               ~host ~port ())
       with
       | Connection.Handshake_failed e -> Error (Connect_failed e)
       | Eio.Io _ as e ->
           Error (Connect_failed
                    (Terminal (Printexc.to_string e))))

(* Try to pull a live idle conn out of the bucket's queue. Close
   and skip anything stale-by-age or stale-by-generation. *)
let rec take_live_idle t b now current_gen =
  match Queue.take_opt b.idle with
  | None -> None
  | Some entry ->
      let too_old =
        match t.config.max_idle_age with
        | None -> false
        | Some age_limit ->
            now -. entry.idle_since > age_limit
      in
      let stale_gen = entry.generation <> current_gen in
      if too_old || stale_gen then begin
        close_quietly entry.conn;
        b.total_closed_dirty <- b.total_closed_dirty + 1;
        take_live_idle t b now current_gen
      end
      else Some entry.conn

let acquire_semaphore ~clock ~config sem =
  match config.Config.on_exhaustion with
  | `Fail_fast ->
      if Eio.Semaphore.get_value sem > 0 then begin
        (* Best-effort: we still call [acquire] to decrement.
           A race between [get_value] and [acquire] could let
           us through when an empty slot has since been taken;
           [acquire] is blocking in that case. Rather than
           risk unbounded wait, guard with a zero-budget
           timeout via [Fiber.first]. *)
        let acquired = ref false in
        Eio.Fiber.first
          (fun () -> Eio.Semaphore.acquire sem; acquired := true)
          (fun () -> Eio.Time.sleep clock 0.);
        if !acquired then `Acquired
        else `Rejected_exhausted
      end
      else `Rejected_exhausted
  | `Block ->
      (match config.borrow_timeout with
       | None ->
           Eio.Semaphore.acquire sem;
           `Acquired
       | Some secs ->
           let acquired = ref false in
           Eio.Fiber.first
             (fun () -> Eio.Semaphore.acquire sem; acquired := true)
             (fun () -> Eio.Time.sleep clock secs);
           if !acquired then `Acquired else `Rejected_timeout)

(* Core borrow: acquire semaphore, pop idle or open fresh,
   return the conn tagged with the bucket generation at borrow
   time. *)
let borrow t ~node_id :
    (Connection.t * int, borrow_error) result =
  if t.closed then Error Pool_exhausted
  else if t.config.max_per_node <= 0 then Error Pool_not_configured
  else
    let b = bucket_for t node_id in
    if b.state = `Drained then Error Node_gone
    else begin
      Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
          b.waiters <- b.waiters + 1);
      let sem_outcome =
        acquire_semaphore ~clock:t.clock ~config:t.config b.sem
      in
      Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
          b.waiters <- b.waiters - 1);
      match sem_outcome with
      | `Rejected_exhausted ->
          Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
              b.total_exhaustion_rejects <- b.total_exhaustion_rejects + 1);
          Error Pool_exhausted
      | `Rejected_timeout ->
          Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
              b.total_borrow_timeouts <- b.total_borrow_timeouts + 1);
          Error Borrow_timeout
      | `Acquired ->
          if b.state = `Drained then begin
            Eio.Semaphore.release b.sem;
            Error Node_gone
          end
          else
            let gen = Atomic.get b.generation in
            let idle_conn =
              Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
                  take_live_idle t b (Unix.gettimeofday ()) gen)
            in
            (match idle_conn with
             | Some c ->
                 Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
                     b.in_use <- b.in_use + 1;
                     b.total_borrowed <- b.total_borrowed + 1);
                 Ok (c, gen)
             | None ->
                 (* Handshake outside the bucket lock — slow. *)
                 (match open_fresh_conn t node_id with
                  | Error e ->
                      Eio.Semaphore.release b.sem;
                      Error e
                  | Ok c ->
                      Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
                          b.in_use <- b.in_use + 1;
                          b.total_borrowed <- b.total_borrowed + 1;
                          b.total_created <- b.total_created + 1);
                      Ok (c, gen)))
    end

(* Return a conn to its bucket, or close it if anything about
   the lease went wrong. *)
let return_ t ~node_id conn ~borrow_gen ~dirty =
  match Hashtbl.find_opt t.buckets node_id with
  | None ->
      (* Bucket gone under us — shouldn't happen mid-lease,
         but be defensive. Close the conn and move on. *)
      close_quietly conn
  | Some b ->
      let now = Unix.gettimeofday () in
      let cur_gen = Atomic.get b.generation in
      let effective_dirty =
        dirty || borrow_gen <> cur_gen
        || b.state = `Drained || t.closed
      in
      Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
          b.in_use <- b.in_use - 1;
          if effective_dirty then
            b.total_closed_dirty <- b.total_closed_dirty + 1
          else
            Queue.push
              { conn; idle_since = now; generation = cur_gen }
              b.idle);
      Eio.Semaphore.release b.sem;
      if effective_dirty then close_quietly conn

let with_borrowed t ~node_id f =
  match borrow t ~node_id with
  | Error e -> Error (`Borrow e)
  | Ok (conn, gen) ->
      let dirty = ref false in
      let r =
        try f conn
        with exn ->
          (* Cancellation raises [Eio.Cancel.Cancelled] —
             treated the same as any other mid-lease
             exception: close the conn, don't re-idle. *)
          return_ t ~node_id conn ~borrow_gen:gen ~dirty:true;
          raise exn
      in
      (match r with Error _ -> dirty := true | Ok _ -> ());
      return_ t ~node_id conn ~borrow_gen:gen ~dirty:!dirty;
      (match r with
       | Ok v -> Ok v
       | Error e -> Error (`Exec e))

let stats t =
  Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
      Hashtbl.fold
        (fun _ b acc ->
          let s = snapshot_bucket_stats b in
          { Stats.in_use = acc.Stats.in_use + s.in_use;
            idle = acc.idle + s.idle;
            waiters = acc.waiters + s.waiters;
            total_borrowed =
              acc.total_borrowed + s.total_borrowed;
            total_created =
              acc.total_created + s.total_created;
            total_closed_dirty =
              acc.total_closed_dirty + s.total_closed_dirty;
            total_borrow_timeouts =
              acc.total_borrow_timeouts + s.total_borrow_timeouts;
            total_exhaustion_rejects =
              acc.total_exhaustion_rejects + s.total_exhaustion_rejects })
        t.buckets Stats.empty)

let stats_by_node t =
  Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
      Hashtbl.fold
        (fun k b acc -> (k, snapshot_bucket_stats b) :: acc)
        t.buckets [])

let drain_node t ~node_id =
  match
    Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
        Hashtbl.find_opt t.buckets node_id)
  with
  | None -> ()
  | Some b ->
      let to_close =
        Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
            b.state <- `Drained;
            (* Bump generation so every in-flight lease
               returns dirty. *)
            ignore (Atomic.fetch_and_add b.generation 1 : int);
            let drained = Queue.create () in
            Queue.iter (fun e -> Queue.push e drained) b.idle;
            Queue.clear b.idle;
            drained)
      in
      Queue.iter (fun e -> close_quietly e.conn) to_close

let refresh_node t ~node_id =
  match
    Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
        Hashtbl.find_opt t.buckets node_id)
  with
  | None -> ()
  | Some b ->
      let to_close =
        Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
            ignore (Atomic.fetch_and_add b.generation 1 : int);
            let drained = Queue.create () in
            Queue.iter (fun e -> Queue.push e drained) b.idle;
            Queue.clear b.idle;
            drained)
      in
      Queue.iter (fun e -> close_quietly e.conn) to_close

let close t =
  Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
      t.closed <- true);
  let all_buckets =
    Eio.Mutex.use_rw ~protect:true t.buckets_mutex (fun () ->
        Hashtbl.fold (fun _ b acc -> b :: acc) t.buckets [])
  in
  List.iter
    (fun b ->
      let to_close =
        Eio.Mutex.use_rw ~protect:true b.lock (fun () ->
            b.state <- `Drained;
            let drained = Queue.create () in
            Queue.iter (fun e -> Queue.push e drained) b.idle;
            Queue.clear b.idle;
            drained)
      in
      Queue.iter (fun e -> close_quietly e.conn) to_close)
    all_buckets
