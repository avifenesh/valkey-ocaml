(** OpenTelemetry instrumentation for valkey.

    Span and attribute names live here — one place to grep, one place
    to enforce the redaction invariants below.

    {1 Setup}

    The library only emits spans. The application configures the
    exporter (and ambient-context backend, for span propagation across
    fibers). With no exporter configured, span ops are near-no-op
    cheap.

    See [docs/observability.md] for the full setup recipe.

    {1 Redaction invariants — non-negotiable}

    Never put any of the following into span attributes or events:

    - [HELLO]/[AUTH] credentials (username, password, token).
    - Command keys or values (the operator's data).
    - Server error message bodies (only the [code] field is public).

    Generic values (host, port, slot number, command verb, error code)
    are fair game.
*)

module Otel = Opentelemetry

(** Span guard for [Connection.connect_and_handshake]. Records the
    bounded operation of opening a TCP socket, optionally upgrading
    to TLS, and running the [HELLO] / [SELECT] handshake. *)
let connect_span ~host ~port ~tls ~proto cb =
  Otel.Tracer.with_
    ~kind:Otel.Span.Span_kind_client
    ~attrs:
      [
        "valkey.host", `String host;
        "valkey.port", `Int port;
        "valkey.tls", `Bool tls;
        "valkey.proto", `Int proto;
      ]
    "valkey.connect" cb

(** Span guard for [Discovery.discover_from_seeds]. *)
let discover_span ~seed_count cb =
  Otel.Tracer.with_
    ~attrs:[ "valkey.cluster.seed_count", `Int seed_count ]
    "valkey.cluster.discover" cb

(** Span guard for the periodic topology refresh. *)
let refresh_span cb =
  Otel.Tracer.with_ "valkey.cluster.refresh" cb

(** Tag a discover/refresh outcome onto the active span. Keep the
    surface tiny — three outcomes, never the topology itself. *)
let record_discovery_outcome span outcome =
  let v =
    match outcome with
    | `Agreed -> "agreed"
    | `Agreed_fallback -> "agreed_fallback"
    | `No_agreement -> "no_agreement"
  in
  Otel.Span.add_attrs span [ "valkey.cluster.outcome", `String v ]

(** Bridge [Cache.metrics] counters to OpenTelemetry as cumulative
    monotonic sums. Each counter (hits, misses, evicts.budget,
    evicts.ttl, invalidations, puts) becomes a metric named
    [<prefix>.<counter>] (default prefix [valkey.cache]). The
    callback runs at every meter collect; with no exporter
    configured, the cost is the OTel registry's no-op path.
    No-op when [metrics_fn] returns [None] (CSC not configured). *)
let observe_cache_metrics ?(name = "valkey.cache")
    (metrics_fn : unit -> Cache.metrics option) =
  let start_ns = Otel.Clock.now_main () in
  Otel.Meter.add_cb (fun ~clock:_ () ->
      match metrics_fn () with
      | None -> []
      | Some (m : Cache.metrics) ->
          let now = Otel.Clock.now_main () in
          let mk suffix v =
            Otel.Metrics.sum
              ~name:(name ^ "." ^ suffix)
              ~is_monotonic:true
              [ Otel.Metrics.int ~start_time_unix_nano:start_ns ~now v ]
          in
          [ mk "hits" m.hits;
            mk "misses" m.misses;
            mk "evicts.budget" m.evicts_budget;
            mk "evicts.ttl" m.evicts_ttl;
            mk "invalidations" m.invalidations;
            mk "puts" m.puts ])
