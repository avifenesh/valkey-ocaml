# Observability — OpenTelemetry tracing

This library emits OpenTelemetry spans for the bounded operations
where latency, frequency, or failure mode are worth knowing about:

| Span                          | When                                                      |
|-------------------------------|-----------------------------------------------------------|
| `valkey.connect`              | TCP + TLS + `HELLO`/`SELECT` handshake (one per connection / reconnect) |
| `valkey.cluster.discover`     | Initial topology discovery from seeds                     |
| `valkey.cluster.refresh`      | Periodic or trigger-driven topology refresh               |

Span attributes:

| Attribute                      | Meaning                                  |
|--------------------------------|------------------------------------------|
| `valkey.host`                  | Destination host (cluster member or standalone) |
| `valkey.port`                  | Destination port                         |
| `valkey.tls`                   | `true` if the connection negotiates TLS  |
| `valkey.proto`                 | RESP protocol version (always `3`)       |
| `valkey.cluster.seed_count`    | Number of seeds queried on this discover |
| `valkey.cluster.outcome`       | `agreed` / `agreed_fallback` / `no_agreement` |

If the body of a span raises, the exception class is recorded on the
span (via `record_exception`) and the span ends with `Error` status.

## What is *not* emitted

By design — see redaction invariants below — the library does not
emit:

- Per-command spans. Default valkey traffic is 100k+ ops/sec; a span
  per request would dominate trace volume and storage. Add
  command-level instrumentation in *your* code if you need it (e.g.,
  wrap the call site with your own span).
- Auth events with credentials. Username/password/token never appear
  on spans.
- Command keys or values. The operator's data stays out of traces.
- Server error message bodies. Only the error code (e.g. `WRONGTYPE`,
  `MOVED`) is generic and could be exposed by your own wrappers — the
  message can echo a key. The library does not put either into spans.

## Redaction invariants

The library guarantees:

- No span attribute or event contains a `HELLO`/`AUTH` argument.
- No span attribute or event contains a Valkey command argument
  (key or value).
- No span attribute contains a server-error `message` field.

These rules are enforced by the very small surface in
[`lib/observability.ml`](../lib/observability.ml) — every attribute
assigned anywhere in the codebase goes through that module's helpers.

## Setup (consumer side)

The library only emits spans. The application configures the
exporter and the [ambient-context][ambient] backend (used by the
opentelemetry library to propagate spans across calls and fibers).

With no exporter configured, span operations are near-no-op and have
negligible overhead — safe to leave instrumented in production builds
even if you don't currently collect.

A typical setup with the cohttp-eio exporter:

```ocaml
let main () =
  Opentelemetry.Globals.service_name := "my-service";
  (* Wire the exporter.  See opentelemetry-client-cohttp-eio docs. *)
  Opentelemetry_client_cohttp_eio.with_setup () @@ fun () ->
  ...
```

Standard OpenTelemetry environment variables apply:

- `OTEL_SDK_DISABLED=1` — disable telemetry entirely.
- `OTEL_SERVICE_NAME` — service name (overrides `Globals.service_name`).
- `OTEL_EXPORTER_OTLP_ENDPOINT` — collector endpoint
  (default `http://localhost:4318`).
- `OTEL_RESOURCE_ATTRIBUTES` — extra resource attributes
  (e.g. `deployment.environment=staging`).

## Cache metrics (CSC)

`Valkey.Observability.observe_cache_metrics` registers an
OpenTelemetry meter callback that exposes the
`Valkey.Cache.metrics` counters as cumulative monotonic sums.
One call per CSC-enabled client at startup:

```ocaml
let client =
  Valkey.Client.connect ~sw ~net ~clock ~config:cfg ~host ~port ()
in
Valkey.Observability.observe_cache_metrics
  (fun () -> Valkey.Client.cache_metrics client)
```

Six metrics emit per collect tick, named `<prefix>.<counter>`
(default prefix `valkey.cache`):

| Metric                          | Counter                                 |
|---------------------------------|-----------------------------------------|
| `valkey.cache.hits`             | `Cache.metrics.hits`                    |
| `valkey.cache.misses`           | `Cache.metrics.misses`                  |
| `valkey.cache.evicts.budget`    | LRU evictions under byte-budget pressure |
| `valkey.cache.evicts.ttl`       | TTL safety-net evictions                |
| `valkey.cache.invalidations`    | Server-invalidation pushes               |
| `valkey.cache.puts`             | Total `Cache.put` calls (incl. rejects)  |

When `cache_metrics` returns `None` (CSC not configured for that
client) the callback emits no metrics for that tick. With no
exporter configured the cost is the OTel registry's no-op walk.

Pass `~name:"my.client.cache"` to use a different prefix for
multi-client deployments.

## Adding your own command-level spans

If you want per-command tracing in your application, wrap the call:

```ocaml
let@ span = Opentelemetry.Tracer.with_
    ~kind:Opentelemetry.Span.Span_kind_client
    ~attrs:["db.system", `String "valkey"; "db.operation", `String "GET"]
    "valkey.command" in
let result = Valkey.Client.get client key in
result
```

Span attributes follow the
[OpenTelemetry semantic conventions for database client calls][otelsem].
Don't put the *key value* into attributes — only the operation verb.

[ambient]: https://github.com/ELLIOTTCABLE/ocaml-ambient-context
[otelsem]: https://opentelemetry.io/docs/specs/semconv/database/database-spans/
