# ocaml-valkey — project memory

A modern Valkey client for OCaml 5 + Eio, RESP3-native, no legacy baggage.

Owner: Avi (Valkey org member, main maintainer of Valkey GLIDE).
Status: Day 1 — learning OCaml, deciding connection-management model.

## Why this exists

Existing OCaml Redis clients (notably `ocaml-redis`) are Lwt/Async-era, RESP2-only, and predate Valkey's modern feature set. The goal here is a small, modern, focused client that targets *only* current OCaml (5.3+) and current Valkey (7.2+), and deliberately drops legacy surface area.

## Rules

### 1. Love of software first
We do this for the love of software and fun. All other considerations are secondary, **except** when other people's businesses or feelings are involved — then that becomes the primary value and we re-assess.

### 2. We own everything — no dismissals
Never describe an issue as "pre-existing," "out of scope," "minor," or "skippable." Everything in this repo is ours. If we find a bug, dead code, bad doc, flaky test, or sloppy error path — we fix it or clearly track it for fixing. We take responsibility.

### 3. Learning pace, not production pace (early sessions)
Every new OCaml, Eio, or ecosystem concept is explained before it's used. Pace is deliberately slow so Avi actually learns and enjoys the language. Small working snippets over grand designs. No "just trust me" code.

## Scope

**In:**
- OCaml 5.3+ only
- Eio-native concurrency (effects-based)
- RESP3 protocol only
- Valkey 7.2+ features: hash field TTL, functions, streams w/ consumer groups, CLIENT NO-EVICT / TRACKING
- Cluster with automatic topology refresh (MOVED/ASK)
- Pipelining + connection pool as first-class

**Out:**
- RESP2, Redis <7, Valkey <7.2
- Lwt, Async backends
- Sentinel
- Blocking-connection APIs
- Legacy `EVAL` ergonomics (lead with Functions)

## Open architectural questions

- Connection model: single connection vs pool vs cluster-aware router (pending Eio study).
- Picos substrate vs Eio direct.
- Typed command/reply via GADTs.

## Conventions

### Documentation-first per command
Before writing OCaml code for any Valkey command, we always:
1. Fetch the current page on `valkey.io/commands/<name>/`.
2. Quote the exact syntax and argument list.
3. Enumerate every documented reply type (success + error).
4. Flag any undocumented edge case (e.g., RESP3 PING-in-subscribe) as "needs empirical verification" — never guess and move on.

Rationale: semantics drift across versions (e.g., HELLO gained `availability_zone` in 8.1; `protover` became optional in 6.2). My training data lags; valkey.io is the source of truth.

### API surface: typed, not raw
The public API exposes **typed command functions** — `Client.ping c`, `Client.hello c ~proto:3` — returning semantically-typed replies (`ping_reply`, `hello`), not raw `resp3` values. A raw-reply escape hatch may exist for power users, but the default is typed.

Rationale: users shouldn't pattern-match on `Bulk_string`/`Simple_string` for every call. The client knows what each command returns; it's the client's job to decode.

### Error type: domain-typed, not raw
Command errors surface as a named `valkey_error` record with fields like `code` (e.g., `"WRONGTYPE"`, `"NOPROTO"`) and `message`, not bare `resp3`. Callers match on `code`.

Rationale: matching `code = "WRONGTYPE"` is idiomatic; pattern-matching a raw `Error { kind; msg }` variant nested inside `resp3` is awkward and leaks protocol details.

### Connection: abstract behind a signature
The connection type is abstract — defined by a module signature (`.mli`), its representation hidden from callers. Internals (sockets, buffers, reader fibers) are free to evolve without breaking users.

Rationale: connection internals will change a lot (pipelining, reconnect state, cluster-node variant). Abstract from day 1 so we never have to un-leak implementation details.

### Connection-setup errors: exceptions, not `result`
Handshake failures (TCP refused, `HELLO` rejected, AUTH wrong) raise exceptions during `connect`. Per-command failures (WRONGTYPE, MOVED, …) return `result`.

Rationale: if the connection can't be established, there's nothing to `result`-handle. Users get a clean "connection failed" exception at the boundary and proceed normally after.
