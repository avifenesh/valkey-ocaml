(** Bounded in-process cache for client-side caching.

    Each entry is keyed by a string (the Valkey key bytes) and holds
    a {!Resp3.t} value (the typed response). Eviction is LRU by
    recency-of-access; the cache stays under a byte budget by
    evicting the tail of the LRU list until the budget is satisfied.

    Concurrency: every public operation is internally synchronised by
    an [Eio.Mutex.t]. Safe to call from multiple Eio fibers
    concurrently — fibers serialise on the mutex.

    This module is the storage primitive only. The connection-side
    tracking handshake, invalidator fiber, and race-safe GET path
    that sit on top of it land in subsequent steps. See
    [docs/client-side-caching.md]. *)

type t

(** [create ~byte_budget] builds an empty cache. Eviction kicks in
    when the total accounted size of all entries exceeds
    [byte_budget].

    @raise Invalid_argument if [byte_budget < 0]. *)
val create : byte_budget:int -> t

(** [get t key] returns the cached value if present, else [None].
    A successful lookup updates the entry's recency, moving it to the
    head of the LRU list. *)
val get : t -> string -> Resp3.t option

(** [put ?ttl_ms t key value] inserts [(key, value)], replacing any
    prior entry for [key]. If after insertion the total size would
    exceed the byte budget, LRU entries are evicted from the tail
    until the budget is satisfied.

    If [ttl_ms] is given, the entry carries a wall-clock expiry
    (same epoch as [Unix.gettimeofday]). Expired entries are
    evicted lazily on the next {!get} that hits them. If [ttl_ms]
    is omitted, the entry has no expiry and lives until evicted
    or replaced.

    If the size of [value] alone (per {!size_of}) is strictly
    larger than the byte budget, the entry is not inserted and
    any prior entry for [key] is left untouched. *)
val put : ?ttl_ms:int -> t -> string -> Resp3.t -> unit

(** [evict t key] removes the entry if present. No-op if absent. *)
val evict : t -> string -> unit

(** [clear t] removes all entries. Used by the reconnect-flush
    invariant: when a tracked connection drops, the server forgets
    its per-client tracking state and we cannot trust any cached
    entry until the new connection re-tracks. *)
val clear : t -> unit

(** [size t] is the total accounted bytes currently held by the
    cache. Bounded above by [byte_budget]. *)
val size : t -> int

(** [count t] is the number of entries currently in the cache. *)
val count : t -> int

(** [size_of value] is the byte accounting for [value] used by
    {!put}. Exposed so tests and callers can predict eviction
    behaviour. Approximate — see [docs/client-side-caching.md] for
    the exact formula. *)
val size_of : Resp3.t -> int
