(** Configuration surface for client-side caching on a Connection.

    Carries the storage primitive ([Cache.t]) plus the mode knobs
    that shape the CLIENT TRACKING handshake. Separate from
    [Cache] so the storage stays a pure data structure — this
    module owns all the wire-protocol coupling.

    See [docs/client-side-caching.md] for the multi-step plan.
*)

type mode =
  | Default
  (** Per-client tracking: server remembers which keys this
      connection read and only sends invalidations for those.
      Default and strongly consistent at the cost of
      server-side per-client tracking table memory. *)
  | Optin
  (** Per-client tracking, opt-in per read: server only tracks
      keys read immediately after a [CLIENT CACHING YES] arming
      command on the same connection. Smaller server-side
      tracking table at the cost of one extra wire frame per
      cached read. Mutually exclusive with [Bcast] (the server
      rejects [TRACKING ... BCAST OPTIN]).

      OPTIN reads always go to the primary, regardless of any
      [~read_from] hint passed to [Client.get] / [mget] / etc.
      Replicas don't run [CLIENT TRACKING], so a replica-side
      [CACHING YES] would either error or silently fail to
      register tracking — neither is acceptable for a
      cache-correctness primitive. Use [Default] tracking if
      replica reads are required. *)
  | Bcast of { prefixes : string list }
  (** Broadcast mode: server sends invalidations for any key
      whose prefix matches. No per-client tracking table;
      scales to huge client counts. Mutually exclusive with
      [Optin]. *)

(* The value in flight is the full result of [Connection.request]
   so the owner can resolve joined fibers with the server error
   (not just a success). Shape matches [Client.exec]'s return.
   Uses [Connection_error.t] directly to avoid pulling [Connection]
   into this module's dependency closure (cycle: Connection →
   Client_cache → Connection). *)
type inflight_value = (Resp3.t, Connection_error.t) result

type t = {
  cache : Cache.t;
  inflight : inflight_value Inflight.t;
  (** Per-key in-flight coordination table. Owned by this config
      (not by [Cache]) because single-flight + invalidation-race
      semantics are CSC-specific; the cache itself stays a neutral
      storage primitive. Populated by [Client.get] on fetch;
      touched by the invalidator fiber via [Inflight.mark_dirty]
      before it evicts from [cache]. *)
  mode : mode;
  noloop : bool;
  (** [true] → ask server not to echo our own writes back as
      invalidations. Requires us to evict locally on our own
      writes. [false] → server echoes our writes; we handle
      them uniformly through the invalidator path. Default is
      [false]. *)
  entry_ttl_ms : int option;
  (** Defence-in-depth TTL. When [Some ms], cached entries
      older than [ms] milliseconds are treated as a miss even
      if no invalidation has arrived. [None] means rely
      entirely on server-side invalidation. *)
}

(** Convenience constructor that initialises [inflight] to a
    fresh empty table. Use this in preference to a literal record
    so the storage layer's housekeeping stays internal. *)
let make ~cache ?(mode = Default) ?(noloop = false)
    ?entry_ttl_ms () =
  { cache;
    inflight = Inflight.create ();
    mode;
    noloop;
    entry_ttl_ms }
