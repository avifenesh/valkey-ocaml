(** Scatter-gather batch of commands.

    A [Batch.t] is a list of queued commands that execute together.
    Two modes, one primitive:

    - **Non-atomic** ([atomic=false], the default) — commands are
      bucketed by slot, each slot's bucket runs as a parallel
      pipeline on that slot's connection, results are merged back
      into input order. Each command gets its own result; partial
      success is the norm. Across slots the operations are
      concurrent and have no collective ordering. Within a single
      slot the connection processes commands serially in the
      order they were queued. Fan-out commands (SCRIPT LOAD,
      FLUSHALL, CLUSTER NODES, …) are dispatched via
      [Client.exec_multi] and their results carry per-node replies.

    - **Atomic** ([atomic=true]) — in cluster mode, every command
      must target the same slot. Standalone ignores slot
      distinctions because there's only one server. The batch runs
      as a [MULTI] / [EXEC] block on the pinned primary with
      optional [WATCH]. Replies arrive as a single aggregate;
      [run] returns [Ok (Some array)] on commit or [Ok None] on a
      WATCH abort. This is the same primitive the [Transaction]
      module sits on top of.

    {1 Choosing atomic vs non-atomic}

    Atomic when the commands form one logical transaction over
    keys in the same slot — counters that must update together, a
    CAS retry loop, etc. Non-atomic for everything else: bulk
    import, cache fan-in/out, heterogeneous fetches across the
    cluster.

    {1 Typed helpers}

    Most users won't build a [Batch.t] directly — they'll call
    [Client.mget_cluster], [Client.mset_cluster], etc. Those
    helpers build a non-atomic batch under the hood, dispatch it,
    and unwrap the per-command reply into the typed shape the
    caller expects. *)

type t

(** Non-overlapping reasons [queue] may reject a command. *)
type queue_error =
  | Fan_out_in_atomic_batch of string
  (** Atomic mode was selected but the queued command's
      [Command_spec] is [Fan_primaries] or [Fan_all_nodes]. A
      MULTI/EXEC block lives on one connection to one primary
      and cannot atomically span the fleet. *)

(** One entry's result in the batch outcome array.

    - [One r] for single-target commands (keyed or keyless-random).
    - [Many rs] for fan-out commands: one [(node_id, result)] per
      primary or per node depending on [Command_spec]. Matches
      the shape of [Client.exec_multi]. *)
type batch_entry_result =
  | One of (Resp3.t, Connection.Error.t) result
  | Many of (string * (Resp3.t, Connection.Error.t) result) list

val create :
  ?atomic:bool ->
  ?hint_key:string ->
  ?watch:string list ->
  unit -> t
(** [hint_key] and [watch] only matter when [atomic=true]. In
    atomic mode they pin the slot the transaction will run on
    (see [Transaction]). [watch] is sent before [MULTI]. *)

val is_atomic : t -> bool

val queue : t -> string array -> (unit, queue_error) result
(** Append a command to the batch. The caller must pattern-match
    the [result]: when it's [Error], the command was not queued
    and [run] won't reference it. *)

val length : t -> int

val run :
  ?timeout:float ->
  Client.t ->
  t ->
  (batch_entry_result array option, Connection.Error.t) result
(** Execute the batch.

    Non-atomic: always returns [Ok (Some results)] with one
    entry per queued command in input order. Timed-out
    sub-commands are filled with [One (Error Timeout)]; completed
    ones keep their actual reply. [timeout] applies as a wall-
    clock deadline to the whole batch; each in-flight command's
    per-call timeout is computed against the remaining window.

    Atomic: serial on the slot's primary connection. Sends
    [WATCH] (if any), [MULTI], each queued command, [EXEC]. Parses
    the aggregate EXEC reply:
    - [Ok (Some results)] — the block committed; one [One reply]
      per queued command in queue order. Individual per-command
      errors surface as [One (Ok (Simple_error …))] inside the
      array, or propagate via EXECABORT on bad-arity / unknown
      command during queueing.
    - [Ok None] — a watched key was modified between [WATCH] and
      [EXEC]; caller should retry the whole batch.
    - [Error _] — transport or protocol failure; outcome unknown.

    Slot is determined by [hint_key] if given, else the first
    queued command's key. Returns [Server_error "CROSSSLOT" …] at
    [run] time if any queued command's key doesn't hash to the
    pinned slot. [Transaction] continues to exist with its eager
    (server-side-queued) semantics; atomic [Batch] is the buffered
    variant. A later version will fold them together.

    {2 A note on [~watch:] and the buffered model}

    Passing [~watch:] to [create] sends the [WATCH] inside the
    same wire batch as [MULTI], so the only window protected is
    the submillisecond span between [WATCH] and [EXEC] — useful
    for paranoia, useless for the classic "read, decide, then
    commit" CAS pattern. Use {!watch} / {!run_with_guard} /
    {!with_watch} below for that. *)

(** {1 WATCH guards — read-modify-write CAS}

    A {!guard} is a live, server-side [WATCH] held open while the
    caller reads, decides, builds a batch, and finally commits.
    The guard pins a connection to the slot of the watched keys
    and holds that primary's atomic mutex (see
    {!Client.atomic_lock_for_slot}) for its lifetime, so other
    atomic ops on the same primary serialise behind it; non-atomic
    pipeline traffic is unaffected.

    Typical use:
    {[
      Batch.with_watch client ["counter"] (fun guard ->
        match Client.get client "counter" with
        | Ok (Some s) ->
          let n = int_of_string s in
          let b = Batch.create ~atomic:true ~hint_key:"counter" () in
          ignore (Batch.queue b [| "SET"; "counter"; string_of_int (n+1) |]);
          (match Batch.run_with_guard b guard with
           | Ok (Some _) -> `Committed
           | Ok None     -> `Aborted_retry
           | Error e     -> `Failed e)
        | _ -> `Empty)
    ]}

    Caveats:
    - All watched keys must hash to the same slot (cluster). Pass
      [hint_key] to override; mismatching keys return CROSSSLOT.
    - Reads inside the guard window must reach the watched
      primary on the {i same connection}. The router pins by slot
      automatically when [target] / [read_from] aren't overridden.
    - The mutex is non-reentrant: don't open a second atomic
      operation on the same slot inside the closure.
    - On exception the guard is released (UNWATCH + mutex unlock),
      not committed. *)

type guard

val watch :
  ?hint_key:string ->
  Client.t -> string list ->
  (guard, Connection.Error.t) result
(** Open a WATCH guard. Resolves the slot from [hint_key] (if
    given) or from the first watched key, validates every other
    watched key hashes to the same slot, acquires the primary's
    atomic mutex, then sends [WATCH k1 k2 …] on the pinned
    connection. Caller must eventually call {!run_with_guard} or
    {!release_guard}; {!with_watch} handles the release for you. *)

val run_with_guard :
  ?timeout:float ->
  t -> guard ->
  (batch_entry_result array option, Connection.Error.t) result
(** Commit an atomic batch under an open guard. Sends [MULTI],
    every queued command, [EXEC] on the guard's connection, then
    releases the guard (mutex unlock; EXEC implicitly clears
    WATCH on the server). Returns:
    - [Ok (Some results)] — committed, one [One reply] per
      queued command in order.
    - [Ok None] — a watched key was modified between {!watch}
      and EXEC; caller retries the read-modify-write loop.
    - [Error _] — transport / protocol failure; outcome unknown.

    Errors before commit (non-atomic batch, slot mismatch with
    guard, queued key in wrong slot) release the guard and return
    [Error]. An empty batch is treated as "user decided not to
    write": UNWATCH is sent and [Ok (Some [||])] returned. *)

val release_guard : guard -> unit
(** Send UNWATCH and release the guard's mutex. Idempotent —
    safe to call after {!run_with_guard} has already released. *)

val with_watch :
  ?hint_key:string ->
  Client.t -> string list ->
  (guard -> 'a) -> ('a, Connection.Error.t) result
(** Scoped WATCH guard. Opens the guard, runs [f guard], and
    releases (UNWATCH + mutex unlock) whether [f] returns
    normally or raises. If [watch] itself fails, returns the
    error and never calls [f]. *)

(** {1 Cluster-aware typed helpers}

    Convenience wrappers that build a non-atomic batch, run it,
    and decode per-command replies into typed shapes. In
    standalone mode these collapse to a pipeline on the one
    connection; in cluster mode they split by slot and dispatch
    in parallel. A single sub-command error propagates as a
    whole-call [Error]; writes to other slots that completed
    are not rolled back. *)

val mget_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  ((string * string option) list, Connection.Error.t) result
(** [MGET] that spans cluster slots. Returns
    [(key, value option) list] in input order. *)

val mset_cluster :
  ?timeout:float ->
  Client.t -> (string * string) list ->
  (unit, Connection.Error.t) result
(** [MSET] that spans cluster slots. Each slot's subset lands
    atomically (server-side [MSET] is atomic); across slots the
    updates interleave. *)

val del_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  (int, Connection.Error.t) result
(** [DEL] that spans cluster slots. Returns the total number of
    keys actually removed (sum of per-slot counts). *)

val unlink_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  (int, Connection.Error.t) result
(** [UNLINK] that spans cluster slots — same semantics as
    [del_cluster] but the server reclaims memory in a background
    thread. Returns the total count of keys actually removed. *)

val exists_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  (int, Connection.Error.t) result
(** [EXISTS] that spans cluster slots. Returns the total count of
    the given keys that exist. Matches server-side [EXISTS]
    semantics where duplicate keys in the input count separately:
    [exists_cluster c ["a"; "a"]] returns 2 if [a] exists. *)

val touch_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  (int, Connection.Error.t) result
(** [TOUCH] that spans cluster slots. Bumps each key's
    last-access time; returns the count of keys that existed. *)

val pfcount_cluster :
  ?timeout:float ->
  Client.t -> string list ->
  (int, Connection.Error.t) result
(** [PFCOUNT] that spans cluster slots. Returns the approximate
    cardinality of the union of every HLL in [keys].

    Implementation: when every key already hashes to the same
    slot, defers to {!Client.pfcount} (one round-trip). Otherwise,
    each input HLL is [DUMP]ed and [RESTORE]d under a temporary
    key in a dedicated hashtag slot, the temps are merged via
    [PFMERGE] into a destination HLL, that destination's
    cardinality is read via [PFCOUNT], and every temporary plus
    the destination is cleaned up before returning. Missing input
    keys are treated as empty HLLs (no contribution).

    Correctness: simply summing per-slot [PFCOUNT] values
    over-counts any element that appears in HLLs on more than one
    slot; this wrapper always goes through [PFMERGE] in the
    cross-slot case.

    Cost: one [PFCOUNT] round-trip in the single-slot case. In
    the cross-slot case: [DUMP + RESTORE] per non-missing key,
    plus [PFMERGE], [PFCOUNT], and cleanup [DEL]. All HLLs are
    small (≤ 12KiB dense encoding), so the wire volume stays
    modest. *)
