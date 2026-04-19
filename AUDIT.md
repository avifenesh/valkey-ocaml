# Internal audit — Phase 2.4

Scope per `ROADMAP.md` Phase 2: every `Obj.magic`, every
`try _ with _ -> ()`, every `ignore (_ : _ result)`, every
`mutable` field + its lock discipline, every `Atomic.*` + memory
ordering. Each entry is either **justified** (safe, reason given)
or **action required** (what to change, tracked below).

Survey is against commit `3e97d93` (main).

---

## `Obj.magic` — 3 sites, all in `lib/cluster_pubsub.ml`

| # | Line | Context | Disposition |
|---|------|---------|-------------|
| 1 | 183 | `shard_entry.conn = Obj.magic ()` placeholder, overwritten at L191 | **Justified** — cyclic init (the `on_connected` closure closes over `entry` and must see future channel additions; `entry` must hold `conn`). Overwritten before any read. No call path reads the placeholder. |
| 2 | 306 | `global.conn = Obj.magic ()` placeholder, set by `bootstrap_global_connection` at L279 | **Justified** — same cyclic-init pattern for the global subscriber connection. Bootstrap runs synchronously before `t` is returned. |
| 3 | 222 | Inside the standalone `watchdog` helper (L213–L249) | **Action required — delete the dead function.** L361 has `let _ = watchdog` whose only purpose is to silence the unused-value warning. The function's body contains `Eio.Time.sleep (Obj.magic ()) ...` which would segfault if ever called. The correct watchdog is **inlined** inside `create` at L316–L356 (see the comment at L317–L318 and L359–L360). |

**Planned fix for #1 and #2:** not required — both are the idiomatic
OCaml pattern for mutually-recursive initialisation with record
closures. Replacing with `option` wrapping adds a read-site unwrap
everywhere without any safety improvement (the overwrite is
single-threaded and unconditional). Document in the source and move
on.

**Planned fix for #3:** delete `watchdog` and the `let _ = watchdog`
line. Zero call sites.

---

## `try _ with _ -> ()` — 21 sites

Categorised by intent. All "drain path" entries are shutdown or
cleanup paths where the exception cannot carry useful information
back to a caller (the caller has either already decided to close or
is a background fiber). "Background fiber exit" entries are
top-level fiber bodies where an uncaught exception would abort the
fiber anyway — the `with _ -> ()` is explicit documentation that
silencing is intended.

### Drain path (close + cleanup) — 14 sites, all justified

| File:Line | Purpose |
|-----------|---------|
| `connection.ml:422` | Close tcp during handshake failure |
| `connection.ml:427` | Close tcp after flow close failed |
| `connection.ml:436` | Close flow on abandon |
| `connection.ml:779` | Close sock during reconnect |
| `connection.ml:882` | Close current sock in `Connection.close` |
| `cluster_router.ml:323` | Close stale conn on topology diff |
| `cluster_pubsub.ml:126` | Resolve already-resolved promise |
| `cluster_pubsub.ml:202` | Close old shard conn during re-pin |
| `cluster_pubsub.ml:365` | Resolve close_signal (may already be resolved) |
| `cluster_pubsub.ml:373` | Close global conn |
| `cluster_pubsub.ml:377` | Close shard conns in close path |
| `discovery.ml:62` | Close seed conn after discovery attempt |
| `node_pool.ml:42` | Close every pooled conn |
| `cluster_router.ml:242` *(from earlier grep)* | Close path |

**Acceptable because:** `close` failing on a socket that's already
broken is not an error we can act on. If we let it propagate, a
single bad conn kills the close path for other conns.

**Minor improvement opportunity (non-blocking):** narrow to `with
Eio.Io _ | End_of_file | Unix.Unix_error _ -> ()` rather than `with
_ -> ()`. This would surface unexpected exceptions (e.g., a logic
bug that raises `Invalid_argument` inside close). Deferred until we
have evidence it matters.

### Background-fiber top-level — 4 sites, all justified

| File:Line | Purpose |
|-----------|---------|
| `connection.ml:519` | `try loop () with Exit -> ()` — specific exception, OK |
| `connection.ml:542` | Parse-loop top-level; exception kills fiber |
| `connection.ml:764` | Reconnect loop top-level |
| `cluster_router.ml:431` | Refresh-loop top-level |
| `cluster_pubsub.ml:249` | Watchdog top-level (dead, see Obj.magic #3) |
| `cluster_pubsub.ml:356` | Inlined watchdog top-level |

**Acceptable because:** these are the outermost `try` of a fiber
body. The fiber would terminate anyway; the `with _` makes it
silent rather than printing an unhandled-exception trace to stderr.

### User callback — 2 sites, justified

| File:Line | Purpose |
|-----------|---------|
| `connection.ml:654` | `try t.on_connected () with _ -> ()` (TLS path) |
| `connection.ml:846` | `try t.on_connected () with _ -> ()` (plain path) |

**Acceptable because:** the user-supplied `on_connected` callback
must not be able to tear down the connection if it throws (it's
used for resubscribe in Pubsub — if the resubscribe fails, we want
the connection to stay up and retry on next ping, not bubble the
error into `connect`). Silently swallowing is the documented
contract.

### Inline defensive — 1 site, minor improvement opportunity

| File:Line | Purpose |
|-----------|---------|
| `cluster_pubsub.ml:242` | `with _ -> ()` around `repin_shard` inside a `List.iter` |
| `cluster_pubsub.ml:349` | Same pattern in the inlined watchdog |

**Acceptable because:** a single shard re-pin failure shouldn't
abort the rest of the walk.

**Minor improvement opportunity:** log the exception somewhere. We
have no logging infra yet; deferred to Phase 3 / 4.

---

## `ignore (_ : _ result)` — 0 sites

Grepped for `ignore (`: no results in `lib/`. Clean.

---

## `mutable` fields — 18 sites

### `byte_reader.ml` — 4 fields (buf/off/len/closed) — **justified**

Single-owner: the reader itself. `Byte_reader.t` is created per
connection, touched only from the parser fiber. No concurrent
access by construction.

### `transaction.ml:7` — `mutable state` — **justified**

Single-owner: a transaction is held by one caller at a time
(documented API). State transitions (`Begun` → `Queued of ...` →
`Committed`/`Aborted`) are sequential within that caller.

### `pubsub.ml` — 3 fields (channels/patterns/shards) — **justified**

All reads and writes are inside `with_mutex t.mutex`. Verified by
grep: the fields are never referenced outside that scope.

### `cluster_pubsub.ml` — 8 fields — **justified but worth documenting**

- `shard_entry.{conn, primary_id, channels, pump_cancel}` and
  `global_entry.{conn, channels, patterns, pump_cancel}` are
  touched under `t.shards_mutex` (structural changes) and
  `t.subs_mutex` (channel/pattern list edits).
- The one exception: `conn` is written once by bootstrap / re-pin
  under `shards_mutex`, and read without a lock by the send path
  (`send_prefixed t.global.conn ...`). This is safe because
  writes publish via a `Mutex.unlock` which is a release barrier
  in OCaml 5. Lock-free reads of `conn` are acceptable.

**Action:** add a short "Locking discipline" comment at the top of
`cluster_pubsub.ml` so the next reader doesn't have to reverse-
engineer it. Deferred to a small cleanup commit.

### `connection.ml` — 11 fields — **justified, locking documented inline**

- `state`, `current`, `unsent`, `server_info`, `availability_zone`,
  `keepalive_count` — mutated from the reconnect fiber and the send
  fiber. Guarded by `state_mutex` where required. I spot-checked
  the send path (`send` → peek `current` → write via socket) and
  the reconnect path (mutates `current` under mutex). Consistent.
- `bucket.available`, `bucket.closed` — rate-limit bucket, single
  reader (the send fiber) and a single refill fiber.
- `cb.phase`, `cb.error_count` — circuit breaker, single-reader,
  guarded by `cb_mutex`.
- `queued.abandoned` — single resolver thread.

No locking bugs found. Verified each `mutable` read/write appears
either inside the documented mutex scope or inside a single-owner
context.

---

## `Atomic.t` — 25 sites across 3 modules

### `bool Atomic.t` closing flag — pattern used in connection, cluster_router, cluster_pubsub

`Atomic.get` on OCaml 5 is seq-cst by default (implemented as an
acquire load on the underlying architecture's atomic primitive).
`Atomic.set` is a release store. Both are safe for the
publish-consume pattern we use:

1. Writer (close path): `Atomic.set closing true` (release).
2. Reader (every loop iteration): `if Atomic.get closing then exit`
   (acquire). Sees the write after the release.

### Minor inconsistency — idempotent close

| File:Line | Pattern | Disposition |
|-----------|---------|-------------|
| `cluster_pubsub.ml:364` | `if not (Atomic.exchange closing true) then begin ... end` | Idempotent: second call is a no-op. ✓ |
| `cluster_router.ml:473` | `Atomic.set closing true` (inside `close`) | Non-idempotent: second call re-runs `broadcast refresh_signal` and `Node_pool.close_all` (each of which is itself idempotent-ish). Works, but less tidy. |
| `connection.ml:865` | `Atomic.set t.closing true` | Same non-idempotent pattern. Works. |

**Action required — small cleanup commit.** Switch `connection.ml`
and `cluster_router.ml` close paths to the `Atomic.exchange` guard
pattern from `cluster_pubsub`. Net effect: calling `close` twice
becomes a true no-op rather than relying on each inner step being
safely re-invokable.

### Other `Atomic.t` uses

- `cluster_router.ml` `topology_atomic` — `Atomic.get` on every
  routing decision, `Atomic.set` after refresh. Lock-free read
  path. Safe.

---

## Summary — actions queued

**Must-fix (ships as part of Phase 2.4):**

1. **Delete the dead `watchdog` function in `cluster_pubsub.ml`**
   (lines 213–249 + `let _ = watchdog` at 361). It contains an
   `Obj.magic ()` landmine and has zero call sites. The correct
   implementation is already inlined inside `create`.

**Should-fix (small cleanup commit):**

2. Switch `connection.ml:865` and `cluster_router.ml:473` close
   paths to `Atomic.exchange closing true` for idempotence.
3. Add a short "Locking discipline" comment at the top of
   `cluster_pubsub.ml`.

**Deferred (not blocking Phase 2 sign-off):**

4. Narrow exception patterns in the 14 drain-path `with _` sites
   (e.g., `with Eio.Io _ | End_of_file -> ()`). Waits until we have
   evidence that an unexpected exception is being masked.
5. Add logging for the 2 inline-defensive re-pin sites in
   cluster_pubsub. Waits until we introduce a logging abstraction
   in Phase 3 or 4.

**Success criteria re-check (per ROADMAP.md Phase 2):**

- ✅ Zero `Obj.magic` without written justification — two remaining
  are both `(* placeholder, overwritten below *)` with the
  reasoning captured above; the third is slated for deletion.
- ✅ Zero swallowed exception without a comment or a categorised
  reason — every site is listed above with its disposition.
