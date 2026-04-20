# Batch

`Batch` is a list of queued commands that execute together. One
primitive, two modes:

- **Non-atomic** (default) — scatter-gather: bucket by slot, run
  a per-slot pipeline in parallel, merge replies in input order.
  Each command gets its own result; partial success is the norm.
- **Atomic** (`~atomic:true`) — every queued key must hash to the
  same slot. The batch runs as a `WATCH` / `MULTI` / ... / `EXEC`
  block on that slot's primary; all commands commit together or
  none do (WATCH abort → `Ok None`).

Same module, one flag. Matches the GLIDE model.

## When to pick which

**Atomic** when the commands form one logical transaction:
counter updates that must stay consistent, optimistic
read-then-write via `WATCH`, producing + index-updating an item
together. Requires keys to co-locate (hashtags in cluster mode).

**Non-atomic** for everything else: bulk ingest, bulk lookup,
scatter-fetch across the cluster, heterogeneous "send these 10
things and tell me what came back."

## Non-atomic example

```ocaml
(* Build a batch of heterogeneous commands. *)
let b = Valkey.Batch.create () in
let _ = Valkey.Batch.queue b [| "SET"; "k1"; "v1" |] in
let _ = Valkey.Batch.queue b [| "INCR"; "counter" |] in
let _ = Valkey.Batch.queue b [| "GET"; "k1" |] in

match Valkey.Batch.run ~timeout:2.0 client b with
| Error e -> Format.eprintf "batch: %a@." Valkey.Connection.Error.pp e
| Ok None -> assert false  (* non-atomic always returns Some *)
| Ok (Some results) ->
    Array.iter
      (function
       | Valkey.Batch.One (Ok v) ->
           Format.printf "  ok: %a@." Valkey.Resp3.pp v
       | Valkey.Batch.One (Error e) ->
           Format.printf "  err: %a@." Valkey.Connection.Error.pp e
       | Valkey.Batch.Many _ -> ())   (* fan-out shape *)
      results
```

Under the hood the router groups these by slot — `k1`, `counter`,
and `k1` again may or may not hash to the same primary. Each
group runs as a pipeline on its primary's connection, all three
groups in parallel.

## Atomic example

```ocaml
let b = Valkey.Batch.create
          ~atomic:true ~hint_key:"cart:{u42}" () in
let _ = Valkey.Batch.queue b [| "SET"; "cart:{u42}:ver"; "2" |] in
let _ = Valkey.Batch.queue b [| "LPUSH"; "cart:{u42}:items"; "laptop" |] in
let _ = Valkey.Batch.queue b [| "EXPIRE"; "cart:{u42}:ver"; "3600" |] in
match Valkey.Batch.run client b with
| Ok (Some rs) -> (* committed; rs has one entry per queued cmd *)
| Ok None -> (* WATCH abort — see caveat below *)
| Error e -> (* transport / protocol failure *)
```

## WATCH guards (read-modify-write)

For the classic optimistic-concurrency pattern — *read, decide,
commit, retry on conflict* — use `Batch.with_watch`:

```ocaml
let rec bump_counter client k =
  let outcome =
    Valkey.Batch.with_watch client [ k ] (fun guard ->
      let cur =
        match Valkey.Client.get client k with
        | Ok (Some s) -> int_of_string s
        | _ -> 0
      in
      let b =
        Valkey.Batch.create ~atomic:true ~hint_key:k ()
      in
      let _ =
        Valkey.Batch.queue b
          [| "SET"; k; string_of_int (cur + 1) |]
      in
      Valkey.Batch.run_with_guard b guard)
  in
  match outcome with
  | Ok (Ok (Some _))  -> `Committed
  | Ok (Ok None)      -> bump_counter client k   (* retry *)
  | Ok (Error e)      -> `Failed e
  | Error e           -> `Failed e
```

`with_watch` sends `WATCH` immediately (so the server is already
tracking the keys when you read), holds the watched primary's
atomic mutex across the closure (so other atomic ops on that
primary serialise behind you, but non-atomic traffic still
multiplexes), and runs `MULTI`/queued commands/`EXEC` when you
call `run_with_guard`. On commit, `EXEC` implicitly clears the
watch state on the server (per the spec); on closure-exit
without `run_with_guard`, an `UNWATCH` is sent and the mutex is
released.

If you need explicit lifetime control (e.g. interleave with code
that mustn't be inside a closure), use the lower-level pieces:

```ocaml
match Valkey.Batch.watch client [ "user:{42}:counter" ] with
| Error e -> ...
| Ok guard ->
    (* read, compute *)
    let b =
      Valkey.Batch.create ~atomic:true
        ~hint_key:"user:{42}:counter" ()
    in
    let _ = Valkey.Batch.queue b [| "INCR"; "user:{42}:counter" |] in
    (match Valkey.Batch.run_with_guard b guard with
     | Ok (Some _) -> `Committed
     | Ok None     -> `Aborted
     | Error e     -> `Failed e)
    (* On the early-return paths above the guard is already
       released. If you bail out before run_with_guard, call
       Batch.release_guard manually to UNWATCH and unlock. *)
```

A few rules baked in:

- **All watched keys must hash to one slot** (cluster). Mismatch
  surfaces as `Server_error { code = "CROSSSLOT"; … }` from
  `watch` itself — no I/O is sent. Pass `~hint_key` to override
  the slot.
- **The mutex isn't reentrant.** Don't open a second atomic op
  (Transaction, atomic Batch, nested `with_watch`) on the same
  slot inside the closure.
- **Reads inside the guard window must reach the watched primary
  on the same connection** for `WATCH` to see them. The router
  pins by slot automatically when `target` / `read_from` aren't
  overridden.
- **Empty batch under guard** is a no-op: `UNWATCH` is sent and
  `Ok (Some [||])` returned. Use this to abort cleanly when your
  read says "no write needed."

### Legacy `~watch:` on `Batch.create`

`Batch.create ~watch:[...]` still exists but is largely useless
in the buffered model — `WATCH` is sent inside the same wire
batch as `MULTI`, protecting only the submillisecond window
before `EXEC`. Treat it as paranoia padding; the real CAS API
is `with_watch`/`run_with_guard` above.

[`Transaction`](transactions.md) (eager MULTI/EXEC, server-side
per-command validation) also remains supported and works the
same way it always has — its `~watch:` happens at `begin_` time
on a pinned connection.

## Typed helpers

Most users don't build a batch by hand. Convenience wrappers in
the `Valkey.Batch` module collapse to one call:

```ocaml
(* MGET spanning cluster slots. *)
Valkey.Batch.mget_cluster client [ "a"; "b"; "c"; ... ]
  (* -> (string * string option) list, input order *)

(* MSET spanning cluster slots. *)
Valkey.Batch.mset_cluster client [ "a", "1"; "b", "2"; ... ]

(* DEL / UNLINK / EXISTS / TOUCH across slots. *)
Valkey.Batch.del_cluster    client [ "a"; "b"; ... ]  (* -> int *)
Valkey.Batch.unlink_cluster client [ "a"; "b"; ... ]  (* async *)
Valkey.Batch.exists_cluster client [ "a"; "b"; ... ]
Valkey.Batch.touch_cluster  client [ "a"; "b"; ... ]
```

These all run as non-atomic batches under the hood.

## Result shape

Per-command entries are:

```ocaml
type batch_entry_result =
  | One of (Resp3.t, Connection.Error.t) result
  | Many of (string * (Resp3.t, Connection.Error.t) result) list
```

- **`One`** for single-target commands (keyed or keyless-random) —
  one reply, matches the usual `Client.exec` shape.
- **`Many`** for fan-out commands (`SCRIPT LOAD`, `FLUSHALL`,
  `CLUSTER NODES`, …). Each primary or node returns its own
  reply; the entry carries `(node_id, reply) list`, same as
  `Client.exec_multi`.

Fan-out commands are **rejected at queue time** in atomic mode:

```ocaml
match Valkey.Batch.queue b [| "SCRIPT"; "LOAD"; "..." |] with
| Ok () -> ...
| Error (Valkey.Batch.Fan_out_in_atomic_batch "SCRIPT") ->
    (* can't atomically dispatch a fan-out inside MULTI/EXEC *)
```

## Timeout semantics

`Batch.run ?timeout` applies as a **wall-clock deadline** to the
whole batch. Commands that reply before the deadline keep their
reply; commands still outstanding when the deadline fires are
filled with `One (Error Timeout)`. Completed slots in cluster
batches keep their writes; no rollback. This is partial-success
by design — use atomic mode if you need all-or-nothing.

Typed helpers (`mget_cluster`, `mset_cluster`, …) collapse any
timeout into a whole-call `Error` to match the single-command
semantic they impersonate — users who want partial-timeout
visibility use `Batch.run` directly.

## Ordering

- **Within a slot** (atomic or not): commands run in queue order
  on that connection.
- **Across slots** (non-atomic only): sub-batches run
  concurrently. No global ordering.
- **Atomic mode**: all keys must hash to the same slot, checked
  client-side before `MULTI` is sent. Mismatch surfaces as
  `Server_error { code = "CROSSSLOT"; … }` at `run` time.

## Relationship to Transaction

`Transaction.t` (the eager MULTI/EXEC module) and `Batch ~atomic:true`
both run MULTI/EXEC blocks. They differ in when per-command
validation happens:

| | `Transaction` | `Batch ~atomic:true` |
|---|---|---|
| Model | Eager | Buffered |
| Round-trips | N + 3 | 1 pipelined burst |
| Bad-arity error | At `queue` time (server rejects) | At `run` time (inside the EXEC aggregate or EXECABORT) |
| Use | Explicit server-side queue validation | Standard batch flow |

Both remain supported. A future version will fold `Transaction`
into a thin facade over `Batch ~atomic:true`.

## Gotchas

- **Non-atomic partial success is real.** If the cluster loses a
  primary mid-batch, some slots commit and some don't. If you
  need atomicity, keys must hash to one slot and atomic mode is
  the answer.
- **Hashtags aren't optional for atomic mode across related
  keys.** `SET user:42 ...` and `SET user:42:profile ...` hash to
  different slots. Use `user:{42}` and `user:{42}:profile`.
- **`Batch.queue` returns `result`.** Even though non-atomic
  mode never rejects, the signature is uniform. Ignore with
  `let _ = Batch.queue b args in` if you're sure.
- **`pfcount_cluster` is intentionally missing.** Summing
  per-slot `PFCOUNT` values over-counts union cardinality when
  the same element appears in HLLs across slots. Use
  `Client.pfcount` with keys that share a slot (via hashtags)
  or do a client-side HLL merge.
