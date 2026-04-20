# Getting started

This guide takes you from zero to a working program in under 10
minutes. It assumes a recent OCaml (≥ 5.3) and a running Valkey
instance (any version ≥ 7.2, but Valkey 9 is what we test against).

## Install

```bash
opam install valkey     # once published; until then pin to this repo
opam install eio_main   # runtime for the examples below
```

Or to hack on a checkout:

```bash
git clone https://github.com/avifenesh/ocaml-valkey
cd ocaml-valkey
opam install . --deps-only --with-test --yes
dune build
```

## Start a local Valkey

If you don't have one already:

```bash
docker compose up -d     # uses docker-compose.yml from this repo
# or:
docker run --rm -p 6379:6379 valkey/valkey:9
```

## Hello, Valkey

A complete program — connect, SET, GET, close:

```ocaml
(* hello_valkey.ml *)
let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let client =
    Valkey.Client.connect ~sw ~net ~clock
      ~host:"localhost" ~port:6379 ()
  in
  match Valkey.Client.set client "greeting" "hello" with
  | Error e ->
      Format.eprintf "SET failed: %a@." Valkey.Connection.Error.pp e
  | Ok _ ->
      match Valkey.Client.get client "greeting" with
      | Ok (Some s) -> print_endline s
      | Ok None -> print_endline "(nil)"
      | Error e ->
          Format.eprintf "GET failed: %a@." Valkey.Connection.Error.pp e
```

With a small `dune`:

```dune
(executable
 (name hello_valkey)
 (libraries valkey eio_main))
```

Run it:

```bash
dune exec ./hello_valkey.exe
# => hello
```

## Anatomy of that program

### The Eio switch

```ocaml
Eio_main.run @@ fun env ->
Eio.Switch.run @@ fun sw ->
```

Eio is structured-concurrency: every fiber lives inside a
[switch](https://ocaml-multicore.github.io/eio/eio/Eio/Switch/index.html).
`Valkey.Client.connect` spawns background fibers (for reads,
reconnects, keepalive) that inherit `sw`. When `sw` ends, every
fiber it owns is cancelled. You never end up with leaked
connections — as long as you run under a switch.

### Connect vs. Open

`Valkey.Client.connect` performs a TCP dial + RESP3 `HELLO`
handshake synchronously. If the server is unreachable or rejects
the handshake, it raises. This is deliberate — there is no result
value to inspect before the connection exists (see
[`CLAUDE.md`](../CLAUDE.md#connection-setup-errors-exceptions-not-result)
in this repo for the rationale).

Per-command failures (`WRONGTYPE`, `MOVED`, timeouts) return
`result` and are matched like any other error.

### Typed wrappers

`Client.set` returns `(bool, Connection.Error.t) result`. The
`bool` is `true` on success; `false` if `NX` / `XX` / `IFEQ`
prevented the write. Every typed wrapper returns a specific type
chosen to match the Valkey reply's semantics:

| Command                 | Return                                    |
|-------------------------|-------------------------------------------|
| `get`                   | `(string option, _) result`               |
| `set`                   | `(bool, _) result`                        |
| `set_and_get`           | `(string option, _) result`               |
| `del`, `unlink`         | `(int, _) result`                         |
| `incr`, `incrby`        | `(int64, _) result`                       |
| `hgetall`               | `((string * string) list, _) result`      |
| `ttl`                   | `(ttl_state, _) result`                   |

See the [API reference](../valkey/Valkey/Client/index.html) for
the full list.

### Errors

`Connection.Error.t` is a small sum type that separates the
**protocol** failure modes from the **server-side** rejections:

- `Server_error { code; message }` — Valkey returned an error with
  a code like `WRONGTYPE` or `NOPROTO`. Match on `code`.
- `Interrupted` / `Closed` — the connection went away. These are
  retried internally by `Cluster_router` up to `max_redirects`;
  for standalone, the caller must handle them (usually by calling
  again after a small sleep; `Client.connect` has auto-reconnect).
- `Timeout` — the per-command `?timeout` fired.
- `Terminal msg` — unrecoverable protocol problem.

Format with `Valkey.Connection.Error.pp`.

## Closing

```ocaml
Valkey.Client.close client
```

`close` is idempotent. Calling it twice is a no-op. Under a
switch, you usually don't need to call it explicitly — the switch
ends and cleans up.

## Command not wrapped yet?

The library ships about 130 typed wrappers. For anything else —
an ACL command, a Valkey module command, something released after
the last client update — there's an escape hatch:

```ocaml
match Valkey.Client.custom client [| "DEBUG"; "OBJECT"; "greeting" |] with
| Ok r -> Format.printf "%a@." Valkey.Resp3.pp r
| Error e -> Format.eprintf "%a@." Valkey.Connection.Error.pp e
```

`custom` returns the raw `Resp3.t` reply. You'll need to decode it
yourself. For commands used repeatedly, register a template with
`Named_commands` so the routing metadata is cached:

```ocaml
let nc = Valkey.Named_commands.create client in
Valkey.Named_commands.register nc ~name:"mydbsize"
  ~template:[| "DBSIZE" |];
let _ = Valkey.Named_commands.run_command nc ~name:"mydbsize" ~args:[||]
```

## What next?

- [cluster.md](cluster.md) — point at a cluster, understand
  topology refresh + MOVED/ASK retries + `Read_from`.
- [transactions.md](transactions.md) — `MULTI` / `EXEC` / `WATCH`,
  slot pinning, CROSSSLOT handling.
- [pubsub.md](pubsub.md) — regular vs sharded pub/sub, auto-resubscribe.
- [performance.md](performance.md) — tuning, benchmarks, when to
  open multiple clients.

If something is unclear or broken, please open an issue — this
library is young and feedback is the fastest way to improve it.
