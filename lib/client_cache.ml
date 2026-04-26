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
  | Bcast of { prefixes : string list }
  (** Broadcast mode: server sends invalidations for any key
      whose prefix matches. No per-client tracking table;
      scales to huge client counts. Not yet implemented —
      present in the type so callers can be future-proof. *)

type t = {
  cache : Cache.t;
  mode : mode;
  optin : bool;
  (** [true] → send [CLIENT TRACKING ... OPTIN] and pipeline
      [CLIENT CACHING YES] before each cached read. Keeps
      server-side tracking table small. [false] → track every
      readonly command automatically (OPTOUT-style). *)
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
