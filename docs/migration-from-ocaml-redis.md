# Migrating from `ocaml-redis`

[`ocaml-redis`](https://github.com/0xffea/ocaml-redis) is the
long-standing OCaml Redis client. This library (`ocaml-valkey`)
is a RESP3-only, Eio-native rewrite targeting Valkey 7.2+ — not
a drop-in replacement.

This guide lists the common idioms side-by-side so you can port
without surprises.

## The big differences

| Aspect                     | ocaml-redis                 | ocaml-valkey                     |
|----------------------------|-----------------------------|----------------------------------|
| Concurrency                | Lwt / Async / blocking      | Eio only                         |
| Wire protocol              | RESP2 (`HELLO` absent)      | RESP3                            |
| Redis version              | ≥ 2.6                       | Valkey 7.2+ / Redis 7+ only      |
| Cluster                    | No (separate lib)           | First-class                      |
| TLS                        | Optional                    | First-class, default-off         |
| Error type                 | Exceptions OR polymorphic variants | `result` + `valkey_error`  |
| Connection on failure      | `Unix.Unix_error` / exn     | Abstract `Connection.Error.t`    |

If you're committed to Lwt/Async or need RESP2 fallback, stay on
`ocaml-redis`. If you're on OCaml 5 with Eio (directly or via a
framework like `piaf`), this library is a better fit.

## Connect

**Before (`ocaml-redis` Lwt):**

```ocaml
let%lwt conn = Redis_lwt.Client.connect { host; port } in
```

**After:**

```ocaml
Eio_main.run @@ fun env ->
Eio.Switch.run @@ fun sw ->
let client =
  Valkey.Client.connect ~sw
    ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env)
    ~host ~port ()
```

The switch (`sw`) gives you guaranteed cleanup: when the switch
ends, the connection and all its fibers terminate.

## GET / SET

**Before:**

```ocaml
let%lwt () = Redis_lwt.Client.set conn "k" "v" in
let%lwt v = Redis_lwt.Client.get conn "k"
```

(`v` has type `string option`, but command errors like
`WRONGTYPE` raise exceptions.)

**After:**

```ocaml
let _ = Valkey.Client.set client "k" "v" in
match Valkey.Client.get client "k" with
| Ok (Some v) -> v
| Ok None     -> "(missing)"
| Error e     -> Format.asprintf "%a" Valkey.Connection.Error.pp e
```

Every per-command failure is a `result`. No exception-handling
needed for `WRONGTYPE`, timeout, `MOVED`, etc. — you pattern-match.

## Transactions

**Before:**

```ocaml
Redis_lwt.Client.Transaction.execute conn [
  `Incr "counter";
  `Hset ("user:42", "name", "alice");
]
```

**After:**

```ocaml
Valkey.Transaction.with_transaction tx_client
  ~hint_key:"counter"
  (fun tx ->
     let _ = Valkey.Transaction.queue tx [| "INCR"; "counter" |] in
     let _ = Valkey.Transaction.queue tx [| "HSET"; "user:42"; "name"; "alice" |] in
     ())
```

Note the dedicated `tx_client` — see
[transactions.md](transactions.md) for why.

## Pub/sub

**Before:**

```ocaml
let%lwt sub = Redis_lwt.Client.Subscriber.connect { host; port } in
let%lwt () = Redis_lwt.Client.Subscriber.subscribe sub [ "orders" ] in
let stream = Redis_lwt.Client.Subscriber.stream sub in
Lwt_stream.iter_s handle_msg stream
```

**After:**

```ocaml
let ps = Valkey.Pubsub.create ~sw ~net ~clock ~host ~port () in
let _ = Valkey.Pubsub.subscribe ps [ "orders" ] in
let rec loop () =
  match Valkey.Pubsub.next_message ps with
  | Ok msg -> handle_msg msg; loop ()
  | Error `Closed -> ()
in loop ()
```

You get a typed `message` sum (`Channel`, `Pattern`, `Shard`)
instead of a raw stream.

## Pipelining

**Before:** `ocaml-redis` has no explicit pipelining API — you
rely on the Lwt scheduler interleaving issues and replies.

**After:** just issue commands from parallel fibers; the client
pipelines automatically.

```ocaml
Eio.Fiber.List.iter
  (fun k -> ignore (Valkey.Client.set client k "v"))
  [ "a"; "b"; "c" ]
```

## What's missing

Things `ocaml-redis` has that this library intentionally does
not:

- **RESP2 fallback.** We're RESP3-only.
- **Sentinel support.** Cluster mode supersedes Sentinel.
- **Redis < 7 support.** Valkey 7.2+ is the baseline.
- **Blocking Lwt/Async bindings.** Eio only.
- **`EVAL`-first scripting ergonomics.** We lead with Functions
  (`FUNCTION LOAD`, `FCALL`); `EVAL` works via `Client.custom`.

Things this library has that `ocaml-redis` doesn't:

- **Cluster with auto-refresh and transparent retries.**
- **Read_from AZ-affinity.**
- **Sharded pub/sub with watchdog re-pinning on failover.**
- **Valkey 8/9 features** — hash field TTL, functions, IFEQ/
  DELIFEQ, CLIENT TRACKING RESP3 invalidations.
- **End-to-end benchmarks + parser fuzzer + chaos tests + retry
  state-machine tests.**

## When both make sense

If your codebase is on Lwt and you're not ready to migrate, keep
`ocaml-redis` for the Lwt pieces and use this library from a
separate Eio-based service.

If you're on OCaml 5 with Eio, this library is roughly 3× faster
than `ocaml-redis` at concurrency 100 (loopback; see
[performance.md](performance.md) for the matrix), plus the
cluster-awareness and modern-Valkey features.

## Porting checklist

- [ ] Replace `Redis_lwt.*` with `Valkey.Client.*` imports.
- [ ] Wrap the top of main in `Eio_main.run` + `Eio.Switch.run`.
- [ ] Replace exception-based error handling with `result`
      pattern-matching.
- [ ] For blocking commands and transactions, open a dedicated
      `Client.t`.
- [ ] Replace pub/sub stream iteration with `next_message` loops.
- [ ] If you were on Sentinel, migrate to cluster mode (or raw
      failover at the connection layer).
- [ ] Update `HELLO` requirements: this library needs RESP3,
      which means Valkey 7+ / Redis 7+.
- [ ] For TLS, pass `Tls_config.t` explicitly (no implicit
      plaintext/TLS selection by port).

## See also

- [getting-started.md](getting-started.md) — for someone brand
  new to the library.
- [cluster.md](cluster.md) — cluster concepts.
- [transactions.md](transactions.md) — `MULTI`/`EXEC` patterns.
- [pubsub.md](pubsub.md) — regular + sharded pub/sub.
