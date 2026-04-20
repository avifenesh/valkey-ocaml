# Security

Advice for shipping this client against an internet-facing or
multi-tenant Valkey. Not exhaustive — defer to your security
team for the authoritative threat model.

## What goes on the wire

Every command the client sends is RESP3 over either:

- Plaintext TCP (`Connection.Config.tls = None`) — **don't use
  this outside loopback or a fully-trusted network**.
- TLS via `ocaml-tls` — see [tls.md](tls.md).

AUTH credentials, transaction payloads, `SET` values, `EVAL`
scripts — all of that flows through the same socket. If it's
sensitive, TLS is non-negotiable.

## AUTH

Valkey has two AUTH shapes:

- Legacy single-password: `AUTH <password>`.
- ACL users: `AUTH <username> <password>`.

The client wires both via `Connection.Config`:

```ocaml
let config =
  { Valkey.Connection.Config.default with
    auth = Some { username = Some "app-prod"; password = "…" };
    tls = Some (Valkey.Tls_config.system_cas ()) }
```

`AUTH` runs inside the `HELLO` handshake, which runs inside the
TLS tunnel, so the password never leaves the encrypted channel.

**Don't put secrets in source control.** Read from env / secret
store at startup:

```ocaml
let password =
  try Sys.getenv "VALKEY_PASSWORD"
  with Not_found -> failwith "set VALKEY_PASSWORD"
```

## ACLs

Use per-service ACL users with the minimum commands they need.
Start with `+@read +@write +@connection -@dangerous`, then
tighten.

A few pitfalls to know about:

- ACL applies to the whole connection. If one service needs a
  different ACL, use a different `Client.t` (and probably a
  different user).
- `ACL WHOAMI` is keyless — any node can answer, but the answer
  only tells you who you are on *that* connection.
- Some commands are silently dropped (not errored) under
  restricted ACLs. Test your subscribing ACL with an actual
  `SUBSCRIBE` — don't assume.

## IAM

Not yet wired. Tracked under ROADMAP Phase 10 — IAM token-based
auth (as used by AWS MemoryDB / ElastiCache IAM auth). If you
need this before Phase 10, the change is localised to
`Connection.Config.auth` — open an issue.

## Secrets in the repo

There's currently one `"pass"` string literal in
`scripts/cluster-hosts-setup.sh` — that's the sudo password for
a local dev VM, not production. It's flagged in AUDIT.md for
removal during Phase 10.

No production secrets should ever land in this repo. Standard
rule: `.env` files are gitignored; CI secrets live in the
provider's secret store.

## TLS — specifics

See [tls.md](tls.md) for the full picture. Key do's and don'ts:

- ✅ Use `system_cas ()` against services with publicly-signed
  certs (ElastiCache, hosted Valkey).
- ✅ Use `from_pem_file` with your private CA for internal
  clusters.
- ❌ Never use `no_verification` in production. It disables
  MITM protection entirely.
- ❌ Don't pin to specific cipher suites unless you know why —
  `ocaml-tls`'s defaults are current.

## Audit of what goes where

| Data                | Destination                  | Protection |
|---------------------|------------------------------|------------|
| AUTH password       | Server, once at handshake    | TLS        |
| Command args (keys + values) | Server, every command | TLS        |
| Function / Lua src  | Server, at `FUNCTION LOAD`   | TLS        |
| Client logs         | stderr (no built-in logging) | Local     |
| Bench / fuzz results | CI artifacts                | GH perms   |

The client does not send telemetry anywhere. No phone-home, no
auto-upload of crash reports.

## Multi-tenant environments

If multiple applications share a Valkey:

- Use ACL per service. Don't share a root-level password.
- Use a separate keyspace prefix per service (`svc-a:…`,
  `svc-b:…`). Prevents accidental cross-reads.
- For cluster mode, pin keyspace prefixes to known slot ranges
  using hashtags so capacity-planning is predictable.
- `FLUSHALL` needs careful ACL gating — it'll wipe everyone.

## Reporting vulnerabilities

For security issues, please email the maintainer directly rather
than opening a public issue. Responses within 72 hours.

## See also

- [tls.md](tls.md) — TLS configuration, dev certs, mTLS.
- [cluster.md](cluster.md) — `prefer_hostname` and how the
  client resolves cluster addresses.
