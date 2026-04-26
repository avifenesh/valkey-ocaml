(* Doubly-linked list of entries threaded through a hashtable, all
   under one [Mutex]. Head = most recently used; tail = least
   recently used and the next eviction target.

   Why the stdlib [Mutex] and not [Eio.Mutex]: every public operation
   here is in-memory only — no IO, no fiber yields. The lock is held
   for a hashtbl/list-edit's worth of time. Plain [Mutex] is correct
   under cooperative Eio scheduling (no preemption mid-section) and
   keeps the cache usable from pure-unit tests with no Eio runtime.

   Locking discipline: every public operation acquires [t.mutex] for
   its full duration via [with_lock]. Hashtbl + node mutation are not
   concurrent-safe and the LRU invariants (head/tail consistency,
   total_bytes == sum of entry sizes) only hold under the lock. *)

type entry = {
  key : string;
  mutable value : Resp3.t;
  mutable size : int;
  mutable prev : entry option;
  mutable next : entry option;
  mutable expires_at : float option;
  (** Wall-clock deadline in seconds (same epoch as
      [Unix.gettimeofday]). [None] means no expiry — entry lives
      until explicitly evicted, LRU-evicted, or replaced. Set
      by [put] when the caller supplies [?ttl_ms]. *)
}

type t = {
  byte_budget : int;
  table : (string, entry) Hashtbl.t;
  mutable head : entry option;
  mutable tail : entry option;
  mutable total_bytes : int;
  mutex : Mutex.t;
}

(* --- size accounting -------------------------------------------- *)

(* Coarse but conservative accounting. The +16 covers the OCaml
   block header, the key copy, and the LRU node bookkeeping per
   entry. Aggregates pay the constant per element to discourage
   caching deeply-nested replies. *)
let rec size_of (v : Resp3.t) : int =
  match v with
  | Bulk_string s
  | Simple_string s
  | Big_number s
  | Bulk_error s
  | Simple_error s -> String.length s + 16
  | Integer _ | Boolean _ | Double _ | Null -> 16
  | Verbatim_string { encoding; data } ->
      String.length encoding + String.length data + 16
  | Array vs | Set vs | Push vs ->
      List.fold_left (fun acc v -> acc + size_of v) 16 vs
  | Map kvs ->
      List.fold_left
        (fun acc (k, v) -> acc + size_of k + size_of v)
        16 kvs

(* --- LRU list operations (must be called under t.mutex) --------- *)

let unlink t e =
  (match e.prev with
   | Some p -> p.next <- e.next
   | None -> t.head <- e.next);
  (match e.next with
   | Some n -> n.prev <- e.prev
   | None -> t.tail <- e.prev);
  e.prev <- None;
  e.next <- None

let push_front t e =
  e.prev <- None;
  e.next <- t.head;
  (match t.head with
   | Some h -> h.prev <- Some e
   | None -> t.tail <- Some e);
  t.head <- Some e

let move_to_front t e =
  match t.head with
  | Some h when h == e -> ()  (* already MRU *)
  | _ ->
      unlink t e;
      push_front t e

(* Pop the LRU tail and remove from table. Returns the popped entry's
   size for total_bytes accounting (caller subtracts). *)
let pop_lru t : int =
  match t.tail with
  | None -> 0
  | Some e ->
      unlink t e;
      Hashtbl.remove t.table e.key;
      e.size

let rec evict_until_fits t =
  if t.total_bytes <= t.byte_budget then ()
  else
    let popped = pop_lru t in
    if popped = 0 then ()  (* list empty; budget unsatisfiable *)
    else begin
      t.total_bytes <- t.total_bytes - popped;
      evict_until_fits t
    end

let with_lock t f =
  Mutex.lock t.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) f

(* --- public API ------------------------------------------------- *)

let create ~byte_budget =
  if byte_budget < 0 then
    invalid_arg "Cache.create: byte_budget must be non-negative";
  { byte_budget;
    table = Hashtbl.create 64;
    head = None;
    tail = None;
    total_bytes = 0;
    mutex = Mutex.create () }

let get t key =
  with_lock t (fun () ->
      match Hashtbl.find_opt t.table key with
      | None -> None
      | Some e ->
          (* Lazy TTL expiration: if the entry's deadline has
             passed, treat as a miss and evict in place. No
             background sweeper; expiration work only happens
             on the read path. *)
          let expired =
            match e.expires_at with
            | None -> false
            | Some deadline -> Unix.gettimeofday () > deadline
          in
          if expired then begin
            unlink t e;
            Hashtbl.remove t.table key;
            t.total_bytes <- t.total_bytes - e.size;
            None
          end else begin
            move_to_front t e;
            Some e.value
          end)

let put ?ttl_ms t key value =
  with_lock t (fun () ->
      let new_size = size_of value in
      if new_size > t.byte_budget then
        (* Single value exceeds the whole budget — reject. Any prior
           entry for this key is left untouched (the docstring spells
           this out). *)
        ()
      else begin
        let expires_at =
          match ttl_ms with
          | None -> None
          | Some ms ->
              Some (Unix.gettimeofday () +. (float_of_int ms /. 1000.0))
        in
        (match Hashtbl.find_opt t.table key with
         | Some e ->
             (* Replace existing entry; adjust total_bytes for the
                size delta, refresh TTL, and move to MRU. *)
             t.total_bytes <- t.total_bytes - e.size + new_size;
             e.value <- value;
             e.size <- new_size;
             e.expires_at <- expires_at;
             move_to_front t e
         | None ->
             let e = { key;
                       value;
                       size = new_size;
                       prev = None;
                       next = None;
                       expires_at } in
             Hashtbl.add t.table key e;
             push_front t e;
             t.total_bytes <- t.total_bytes + new_size);
        evict_until_fits t
      end)

let evict t key =
  with_lock t (fun () ->
      match Hashtbl.find_opt t.table key with
      | None -> ()
      | Some e ->
          unlink t e;
          Hashtbl.remove t.table key;
          t.total_bytes <- t.total_bytes - e.size)

let clear t =
  with_lock t (fun () ->
      Hashtbl.clear t.table;
      t.head <- None;
      t.tail <- None;
      t.total_bytes <- 0)

let size t =
  with_lock t (fun () -> t.total_bytes)

let count t =
  with_lock t (fun () ->
      Hashtbl.length t.table)
