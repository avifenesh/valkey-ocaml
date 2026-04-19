# Transactions

`MULTI` / `EXEC` groups a sequence of commands that run atomically
and in order on a single Valkey server. `WATCH` adds optimistic
concurrency control — if any watched key is modified between
`MULTI` and `EXEC`, the transaction aborts and the caller retries.

This guide covers the common patterns, the two rules you must
follow in cluster mode, and why transactions need a dedicated
`Client.t`.

## The two-line version

```ocaml
let result =
  Valkey.Transaction.with_transaction client ~hint_key:"cart:{42}"
    (fun tx ->
       let _ = Valkey.Transaction.queue tx [| "INCR"; "cart:{42}:ver" |] in
       let _ = Valkey.Transaction.queue tx [| "LPUSH"; "cart:{42}:items"; "laptop" |] in
       ())
in
match result with
| Ok (Some replies) -> (* committed *)
| Ok None          -> (* WATCH tripped; retry *)
| Error e          -> (* transport failure *)
```

`with_transaction` handles `begin_` → `f tx` → `exec`. If `f`
raises, it `discard`s before re-raising.

## The rules

### 1. Use a dedicated `Client.t`

A transaction pins a single connection out of the client's pool
and drives `MULTI` / ... / `EXEC` on it. The pool uses the same
connection for other commands. If another fiber issues a command
on the same `Client.t` while a transaction is open, the
interleaved traffic corrupts both.

Same rule as blocking commands (`BLPOP`, `BRPOP`): **open a
dedicated `Client.t` and use it single-threaded for the
transaction's duration.**

```ocaml
(* Wrong: shared client. *)
let client = Valkey.Client.from_router ~config router

(* Right: one dedicated client for transactions, another for
   the rest of the application. *)
let tx_client = Valkey.Client.from_router ~config router
let tx = Valkey.Transaction.begin_ tx_client ~hint_key:"cart:{42}"
(* ... use tx_client only for the transaction ... *)
Valkey.Client.close tx_client
```

### 2. Cluster mode: keys must share a slot

Every key referenced inside the transaction — queued commands
**and** `WATCH`ed keys — must hash to the same cluster slot.
Otherwise Valkey rejects with `CROSSSLOT` and the offending
`queue` returns an `Error`.

Use hashtags:

```
cart:{42}:ver     -> slot CRC16("42")
cart:{42}:items   -> slot CRC16("42")   (same)
cart:{42}:total   -> slot CRC16("42")   (same)
```

Pass any one of them as `~hint_key` so `begin_` pins the
transaction's connection to the right primary:

```ocaml
Valkey.Transaction.begin_ tx_client ~hint_key:"cart:{42}:ver"
```

In standalone mode `hint_key` is ignored — there's only one node.

## WATCH — optimistic concurrency

The classic "check-and-set" pattern: read, compute, write
atomically, retry on conflict.

```ocaml
let rec apply_delta ~max_attempts attempts =
  if attempts >= max_attempts then
    Error (Valkey.Connection.Error.Terminal "too many retries")
  else
    match
      Valkey.Transaction.with_transaction tx_client
        ~hint_key:"balance:{user:42}"
        ~watch:["balance:{user:42}"]
        (fun tx ->
           let current =
             match Valkey.Client.get tx_client "balance:{user:42}" with
             | Ok (Some s) -> int_of_string s
             | _ -> 0
           in
           let _ =
             Valkey.Transaction.queue tx
               [| "SET"; "balance:{user:42}";
                  string_of_int (current + 10) |]
           in
           ())
    with
    | Ok (Some _) -> Ok ()      (* committed *)
    | Ok None -> apply_delta ~max_attempts (attempts + 1)
    | Error e -> Error e
```

`Ok None` means WATCH tripped — someone else modified
`balance:{user:42}` between the `GET` and the `EXEC`. Retry.

**Don't retry on `Error e`.** A transport failure means the
transaction's outcome is unknown. Valkey's `MULTI` is not
idempotent — the commands may have run. If you retry and they
had run, you apply the delta twice. Prefer an idempotency key
(e.g., store `delta_id` in the transaction, have the read step
check "already applied?").

## What `exec` returns

```ocaml
val exec : t -> (Resp3.t list option, Connection.Error.t) result
```

- `Ok (Some replies)` — the transaction ran; `replies` has one
  entry per queued command, in order. Decode each as you would a
  `Client.custom` reply.
- `Ok None` — WATCH aborted. The queued commands did **not** run.
- `Error e` — transport or protocol failure. Outcome unknown.

Queue-time errors surface immediately:

```ocaml
match Valkey.Transaction.queue tx [| "GET" |] with (* wrong arity *)
| Error e -> Valkey.Transaction.discard tx; handle e
| Ok () -> ...
```

Server replies to `QUEUE`d commands with `+QUEUED`; anything else
(wrong arity, unknown command, CROSSSLOT) is an error on the
`queue` call itself, not a deferred error at `EXEC` time.

## When *not* to use a transaction

A MULTI/EXEC block doesn't do what new users often think:

- **It's not isolation across connections.** The server runs the
  batch atomically, but between the block's individual commands,
  other clients see intermediate state through standard reads.
  Cross-connection isolation needs WATCH or a different
  primitive.
- **It's not rollback.** If one command in the batch fails at
  runtime (e.g., `INCR` on a non-numeric value), the rest still
  run. There's no rollback; you get back a list of mixed
  `Ok`/`Error` replies.
- **It's not for batching throughput.** If all you want is to
  send 100 SETs faster, use pipelining (Phase 7) — not MULTI.
  MULTI/EXEC adds a round-trip and forces the server to buffer
  the whole block before executing.

Use MULTI when:
- You need atomicity within the block (all-or-nothing abort via
  WATCH).
- You need a strict order that no other client can observe broken.

For idempotent point operations, `SET IFEQ` and `DELIFEQ` (Valkey
9+) are simpler — see [`Client.set_and_get`](../valkey/Valkey/Client/index.html).

## Lua / Functions as an alternative

Valkey's Function API (loaded via `FUNCTION LOAD`, called via
`FCALL`) runs a script server-side atomically. For complex
check-and-set logic, this is often cleaner than WATCH/MULTI:

- No WATCH races: the script runs without interleaving.
- No retry loop: success or failure is terminal.
- Logic is server-local: less latency for multi-step flows.

```ocaml
let _ =
  Valkey.Client.function_load client
    ~code:{|
      #!lua name=mylib
      redis.register_function('incr_if_below', function(keys, args)
        local current = tonumber(redis.call('GET', keys[1]) or '0')
        local cap = tonumber(args[1])
        if current >= cap then return 'full' end
        return redis.call('INCR', keys[1])
      end)
    |} ~replace:true
in
let _ =
  Valkey.Client.fcall client ~name:"incr_if_below"
    ~keys:["counter:{42}"] ~args:["100"]
```

Transactions remain the right choice for "watch this key, do a
few ops atomically, retry on conflict" — they're concise and
don't require deploying Lua code.

## See also

- [cluster.md](cluster.md#multi-key-commands--hashtags) — hashtag
  strategy for keeping keys co-located.
- [`Transaction.mli`](../valkey/Valkey/Transaction/index.html) —
  API reference.
- [`test/test_transaction.ml`](../../test/test_transaction.ml) — working
  examples of every pattern in this guide, in test form.
