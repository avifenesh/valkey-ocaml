(** Per-key in-flight table.

    Small coordination layer that sits between [Client.get] and
    [Cache] for CSC. Two jobs:

    1. {b Single-flight.} When N fibers call [Client.get] for the
       same cold key, only the first one hits the wire; the other
       N-1 await the first's reply.

    2. {b Invalidation race.} If an invalidation for key [k]
       arrives while the fetch for [k] is in flight, the value
       that eventually comes back is already known stale and
       must not be cached.

    The module is a pure data structure: a hashtable of
    [{promise; resolver; dirty}] entries protected by a stdlib
    [Mutex].  It does not depend on Eio's scheduler for
    correctness (the only Eio types it touches are [Promise.t] and
    [Promise.u]).  Unit-testable without a runtime — see
    [test_inflight.ml]. *)

type 'v t

val create : unit -> 'v t

type 'v begin_result =
  | Fresh of 'v Eio.Promise.u
      (** Caller is the owner of this fetch. Wire-request the key,
          then call {!complete} (on success) or {!abandon} (on
          failure) and resolve this [Promise.u] with the result so
          waiters see it. *)
  | Joining of 'v Eio.Promise.t
      (** Another fetch for this key is already in flight. Await
          the promise and use its value; do not hit the wire. *)

val begin_fetch : 'v t -> string -> 'v begin_result

val mark_dirty : 'v t -> string -> unit
(** Flag the pending fetch for [key] as invalidated mid-flight.
    No-op if no fetch is in flight.  Called by the invalidator
    fiber before {!Cache.evict}. *)

val mark_all_dirty : 'v t -> unit
(** Flip the dirty flag on every pending fetch.  Used by the
    invalidator fiber on a flush-all push so in-flight owners
    don't cache values that were fresh pre-flush but stale
    post-flush. *)

type completion = Clean | Dirty

val complete : 'v t -> string -> completion
(** End the owner's fetch.  Returns [Clean] if the caller should
    populate the cache with the fetched value, [Dirty] if the
    value must be discarded because an invalidation raced the
    fetch.  Removes the in-flight entry. *)

val abandon : 'v t -> string -> unit
(** Remove the in-flight entry on error path.  Waiters see the
    error via the resolver the owner resolves. *)

val pending_count : 'v t -> int
(** Number of keys currently in flight.  Test / metrics use only. *)
