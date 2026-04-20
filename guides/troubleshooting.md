# Troubleshooting

Common errors, what they mean, and what to do. Grouped by where
they surface in the code.

## Connection-setup errors (raise at `connect`)

### `Tcp_refused "…"`

The TCP dial failed before any RESP3 traffic. Most common causes:

- Wrong host/port — check `docker ps`, `netstat -an`.
- Server not up yet — add a short retry loop around
  `Client.connect` or wait for a `docker-compose ... up -d` to
  finish.
- Firewall dropping the packet.
- TLS mismatch — connecting plaintext to a TLS-only port shows
  up here because the server resets the connection during the
  initial byte. See [tls.md](tls.md).

Nothing to retry automatically; fix the environment and retry.

### `Tls_alert (…)`

TLS handshake failed. Message tells you which alert:

- `Bad_certificate` — the server's cert didn't verify. Either
  the CA is untrusted or the CN/SAN doesn't match the host you
  dialled.
- `Handshake_failure` — cipher / protocol mismatch.
- `Unknown_ca` — you need to pass the right trust store.

See [tls.md](tls.md).

### `Hello_rejected "…"`

TCP + TLS succeeded, but the server rejected the RESP3 `HELLO`.
Two common cases:

- Valkey < 7 — this library requires RESP3, which Valkey 7+ and
  Redis 7+ speak. Upgrade the server.
- `HELLO 3 AUTH username password` failed — bad credentials.

## Per-command errors (returned as `Error _`)

### `Server_error { code = "WRONGTYPE"; … }`

You're running a command that doesn't match the key's type.
Classic case: `LPUSH` on a string. Re-check your types.

### `Server_error { code = "MOVED"; … }`

Shouldn't surface to user code — `Cluster_router` handles it
transparently. If you see it:

- You're using the raw `Connection.t` API without a router.
  Switch to `Client.from_router`.
- Topology is changing faster than `max_redirects`. Raise it.

### `Server_error { code = "CROSSSLOT"; … }`

Multi-key command with keys in different cluster slots. Co-locate
keys via `{...}` hashtags or call per-key. See
[cluster.md](cluster.md#multi-key-commands--hashtags).

### `Server_error { code = "CLUSTERDOWN"; … }`

Shouldn't surface — the retry machinery absorbs it with
exponential backoff up to `max_redirects`. If you see it
repeatedly, the cluster genuinely has no primary for some slot
(mid-failover, misconfigured, or stalled). Check `CLUSTER INFO`
on a live node.

### `Server_error { code = "NOSCRIPT"; … }`

`EVALSHA`'d a script that isn't cached on this node. The typed
`script_eval` wrapper falls back to `SCRIPT LOAD` + retry
automatically; seeing `NOSCRIPT` means you called `EVALSHA`
directly via `custom`. Either use the wrapper, or handle the
fallback yourself.

### `Server_error { code = "NOPROTO"; … }`

Server doesn't support RESP3. This library is RESP3-only. Upgrade
to Valkey 7.2+ or Redis 7+.

### `Interrupted` / `Closed`

Connection went away mid-flight. In cluster mode, these are
retried up to `max_redirects` times. In standalone, they surface
to the caller — the connection will reconnect on the next call
(you get a fresh `Closed` or success). Build a small retry
wrapper at the app level:

```ocaml
let rec with_retry attempts f =
  if attempts <= 0 then f ()
  else
    match f () with
    | Error Valkey.Connection.Error.Closed
    | Error Valkey.Connection.Error.Interrupted ->
        Eio.Time.sleep clock 0.05;
        with_retry (attempts - 1) f
    | r -> r
```

### `Timeout`

The `?timeout` you passed fired. Either the command is slow
(`DEBUG SLEEP`, `WAIT`, a giant `KEYS`), the server is
overloaded, or the network is flapping. Raise the timeout or
investigate the server.

### `Terminal "…"`

Unrecoverable protocol-level failure. The message describes the
specific invariant violation. If it's reproducible, open an
issue with the minimal reproducer — these indicate a real bug
either in the library or in the server.

## Pub/sub oddities

### Messages arrive, then stop

Most likely the connection dropped and the subscriptions didn't
replay. Check:

- You're using `Pubsub` / `Cluster_pubsub` (which have auto-
  resubscribe), not raw `Connection.t`.
- The subscribe call actually returned `Ok ()` (a lost network
  packet during subscribe can leave the server not actually
  listening).

### `next_message` returns `Error \`Closed`

The pubsub handle was closed — either explicitly via `close` or
because the owning switch ended. Open a new one.

### `publish` returns `0` in cluster mode even though there's a subscriber

`PUBLISH` returns the count of subscribers **on the node it
hits**. In cluster mode, messages broadcast across shards for
delivery but the count is local. This is a Valkey-wide quirk, not
a client issue. See comment in
[`test/test_cluster.ml`](../../test/test_cluster.ml) for the
full explanation.

## Cluster failover symptoms

### "One request failed during failover, the next succeeded"

Working as intended. `Cluster_router` retries transparently up to
`max_redirects` times with exponential backoff on CLUSTERDOWN.
If the failover finishes inside that window, you see zero
user-visible errors. If it doesn't, one request fails — and the
topology-refresh fiber has updated by the next call.

### "Every request fails for ~5 s during failover"

Cluster gossip takes time to converge. Default backoff schedule
tolerates this up to ~3 s cumulative across retries. Widen:

```ocaml
let config =
  { (Valkey.Cluster_router.Config.default ~seeds) with
    max_redirects = 10 }
```

This raises the cumulative backoff to ~6 s while keeping the per-
call cap at 1.6 s.

### "I get `no shard owns slot N` errors"

Topology is stale and the new primary hasn't been added to the
pool yet. Usually self-heals within a refresh cycle (5 s).
Shorter:

```ocaml
let config =
  { (Valkey.Cluster_router.Config.default ~seeds) with
    refresh_interval = 1.0; refresh_jitter = 0.5 }
```

## TLS

See [tls.md](tls.md#common-pitfalls).

## Still stuck?

Enable verbose internal tracing (once Phase 3.5 ships — currently
there's no logging abstraction; you can add `Printf.eprintf` in
`lib/cluster_router.ml:handle_retries` for spot debugging).

For parser / protocol bugs, reproduce under
`bin/fuzz_parser/fuzz_parser.exe --seed <N> --strict`; the
shrinker will minimise the failing input.

For a live-server bug, capture the request with `tcpdump -i lo -s
0 -w capture.pcap port 6379` and open it in Wireshark — the
client's framing is vanilla RESP3.

Open an issue with what you tried.
