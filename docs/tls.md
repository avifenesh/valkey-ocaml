# TLS

This library uses [ocaml-tls](https://github.com/mirleft/ocaml-tls)
as the TLS stack via `tls-eio`. All TLS work happens on the
client's Eio fiber; no separate TLS process or thread.

## Enabling TLS

Pass a `Tls_config.t` in `Connection.Config`:

```ocaml
let tls_cfg = Valkey.Tls_config.system_cas () in
let config =
  { Valkey.Connection.Config.default with
    tls = Some tls_cfg }
in
let client =
  Valkey.Client.connect ~sw ~net ~clock
    ~config:{ Valkey.Client.Config.default with connection = config }
    ~host:"valkey.example.com" ~port:6390 ()
```

`Tls_config` is a thin wrapper picking sane defaults; the knobs
you may want:

| Value                                | When                              |
|--------------------------------------|-----------------------------------|
| `Tls_config.system_cas ()`           | Default. Uses the OS trust store. |
| `Tls_config.from_pem_file path`      | Self-signed or internal CA chain. |
| `Tls_config.no_verification`         | Dev only. Accepts *any* cert. **Never in production.** |

## Self-signed dev certs

The repo includes `scripts/gen-tls-certs.sh` which generates a
throwaway CA + server cert under `.dev-certs/`:

```bash
bash scripts/gen-tls-certs.sh
docker compose up -d valkey-tls   # started with the cert installed
```

Point the client at the generated CA:

```ocaml
let tls_cfg =
  Valkey.Tls_config.from_pem_file ".dev-certs/ca.pem"
```

## ElastiCache / Valkey managed services

AWS ElastiCache and similar hosted services require TLS and use
publicly-signed certs. Just use `system_cas ()`:

```ocaml
let config =
  { Valkey.Connection.Config.default with
    tls = Some (Valkey.Tls_config.system_cas ()) }
```

Make sure `ca-certs` is installed and its dependencies are
available (some distros need `ca-certificates` or equivalent).

The client's `HELLO` handshake runs inside the TLS tunnel; you
usually also want to pass `AUTH` credentials — see the
`Connection.Config` docs.

## mTLS (client certificate)

Not yet wired. The underlying `tls-eio` supports client certs;
the integration is tracked under ROADMAP Phase 10. If you need
it sooner, open an issue — the change is localised to
`Tls_config` and `Connection.connect`.

## TLS renegotiation + reconnect

TLS sessions are established per-TCP-connection. On a reconnect
(network blip, keepalive failure, server restart), the client
goes through a fresh TLS handshake. This is a few extra
round-trips of latency per reconnect, not steady-state overhead.

If your environment frequently resets TCP, consider raising
`Connection.Config.keepalive_interval` so the client detects
dead connections faster and cycles them, rather than discovering
it mid-command.

## Observed overhead

Throughput under TLS vs plaintext on loopback, SET 16 KiB @
concurrency 100:

- Plaintext: ~91 % of C reference (valkey-benchmark).
- TLS via ocaml-tls: ~55 % of C reference.

TLS costs roughly 40 % throughput on large payloads. On small
payloads (100 B) the cost is ~10 % — TCP/TLS framing dominates
less. Most of the TLS overhead is AEAD cipher work, not
protocol; using AES-NI-accelerated ciphers via `mirage-crypto`
builds with hardware crypto enabled is already the default.

## Common pitfalls

- **Cert CN mismatch.** The server's cert must match the
  hostname you connect to. If you dial `localhost` but the cert
  is for `valkey.internal`, you'll get
  `Tls_alert (Bad_certificate ...)` in the `Tcp_refused` error.
  Fix by either correcting the hostname or re-issuing the cert.
- **System trust store doesn't include your CA.** Either install
  the CA system-wide (`update-ca-certificates` on Debian-family)
  or switch to `Tls_config.from_pem_file`.
- **Not using TLS when the server requires it.** Valkey with
  `tls-port 6379` accepts only TLS on that port; plaintext
  clients get an immediate RST.
- **Using `no_verification` in production.** This disables MITM
  protection entirely. Don't.

## See also

- [`Tls_config.mli`](../valkey/Valkey/Tls_config/index.html) —
  API reference.
- [`security.md`](security.md) — broader security guidance (ACL,
  secrets hygiene).
