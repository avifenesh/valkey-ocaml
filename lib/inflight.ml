(* Per-key in-flight table for CSC's single-flight + invalidation-
   race-safe GET path.

   Invariants (held under [t.mutex] for the duration of each op):

   - A key is either absent from [table] (no fetch pending) or
     present with exactly one outstanding fetch (the "owner") and
     zero or more joined waiters sharing the same promise.
   - [dirty = true] means an invalidation for this key arrived
     between [begin_fetch] and [complete]; the owner must NOT cache
     the value it eventually receives, because it is already known
     stale.
   - [complete] and [abandon] remove the entry. After removal,
     subsequent [begin_fetch]es are fresh.

   Why plain [Mutex], not [Eio.Mutex]:  every op here is in-memory
   (hashtbl edits + promise object creation). No fiber yields under
   the lock. The owner's wire fetch happens *after* the lock is
   released — the promise is created while holding the lock, then
   the lock is dropped, then the caller does the wire round-trip
   and calls [complete] later (which re-acquires briefly).

   Lock-ordering rule with [Cache]: the invalidator fiber calls
   [mark_dirty cache_inflight k] *before* [Cache.evict cache k].
   Both functions acquire their own mutex, but never hold both at
   once. There is no path that ever nests them, so no deadlock
   is possible. *)

type 'v entry = {
  promise : 'v Eio.Promise.t;
  resolver : 'v Eio.Promise.u;
  mutable dirty : bool;
}
[@@warning "-69"]

type 'v t = {
  table : (string, 'v entry) Hashtbl.t;
  mutex : Mutex.t;
}

let create () =
  { table = Hashtbl.create 16;
    mutex = Mutex.create () }

let with_lock t f =
  Mutex.lock t.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock t.mutex) f

type 'v begin_result =
  | Fresh of 'v Eio.Promise.u
  (** Caller is the owner of this fetch: send the wire request,
      then call [complete t key v] on success or [abandon t key]
      on failure. The resolver is returned so waiters joined via
      the [Joining] branch get woken. *)
  | Joining of 'v Eio.Promise.t
  (** Another fetch for this key is already in flight. Await the
      promise and use its value. *)

let begin_fetch t key : _ begin_result =
  with_lock t (fun () ->
      match Hashtbl.find_opt t.table key with
      | Some e -> Joining e.promise
      | None ->
          let promise, resolver = Eio.Promise.create () in
          let e = { promise; resolver; dirty = false } in
          Hashtbl.add t.table key e;
          Fresh resolver)

(* Mark a pending fetch as dirty. No-op if no fetch is in flight
   for this key — the invalidator just dropped it from the cache
   via its own path and there's nothing else to do. *)
let mark_dirty t key =
  with_lock t (fun () ->
      match Hashtbl.find_opt t.table key with
      | Some e -> e.dirty <- true
      | None -> ())

(* Flip the dirty flag on every pending fetch. Used by the
   invalidator fiber on a Flush_all push so in-flight owners
   don't cache values that were fresh pre-flush but stale
   post-flush. Cheap in practice: pending counts are small
   (< wire-level concurrency). *)
let mark_all_dirty t =
  with_lock t (fun () ->
      Hashtbl.iter (fun _ e -> e.dirty <- true) t.table)

(* Complete a successful fetch. Returns [`Clean] if the caller
   should populate the cache with the fetched value, or [`Dirty]
   if an invalidation arrived during the fetch and the value must
   not be cached. Either way the in-flight entry is removed. The
   promise resolver stored in the entry has already been resolved
   by the caller; this function is only about cache-write
   bookkeeping. *)
type completion = Clean | Dirty

let complete t key : completion =
  with_lock t (fun () ->
      match Hashtbl.find_opt t.table key with
      | None -> Clean   (* shouldn't happen; treat as clean *)
      | Some e ->
          let c = if e.dirty then Dirty else Clean in
          Hashtbl.remove t.table key;
          c)

(* Abandon a fetch (error path). Removes the entry; waiters who
   joined via [Joining] will see the error propagated through the
   resolver that the caller resolves. *)
let abandon t key =
  with_lock t (fun () ->
      Hashtbl.remove t.table key)

let pending_count t =
  with_lock t (fun () -> Hashtbl.length t.table)
