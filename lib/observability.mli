(** OpenTelemetry instrumentation for valkey.

    All spans the library emits live behind these helpers — one place
    to grep for span names, attribute keys, and the redaction policy.

    See [docs/observability.md] for setup, the full attribute schema,
    and the redaction invariants this module enforces. *)

(** Wrap a connect-and-handshake (TCP + optional TLS + [HELLO] /
    [SELECT]) in a [valkey.connect] span. Attributes:
    [valkey.host], [valkey.port], [valkey.tls], [valkey.proto].

    [host] and [port] are address metadata only; the library never
    surfaces the [HELLO]/[AUTH] credentials it sends inside the span. *)
val connect_span :
  host:string ->
  port:int ->
  tls:bool ->
  proto:int ->
  (Opentelemetry.Span.t -> 'a) ->
  'a

(** Wrap [Discovery.discover_from_seeds] in a [valkey.cluster.discover]
    span. Records [valkey.cluster.seed_count]. The outcome
    ([agreed]/[agreed_fallback]/[no_agreement]) is added later via
    {!record_discovery_outcome}. *)
val discover_span :
  seed_count:int -> (Opentelemetry.Span.t -> 'a) -> 'a

(** Wrap one tick of the periodic topology refresh in a
    [valkey.cluster.refresh] span. *)
val refresh_span : (Opentelemetry.Span.t -> 'a) -> 'a

(** Stamp a discover/refresh outcome onto the active span. The body
    of [discover_span]/[refresh_span] should call this exactly once,
    with the variant matching the [Discovery.select] result. *)
val record_discovery_outcome :
  Opentelemetry.Span.t ->
  [< `Agreed | `Agreed_fallback | `No_agreement ] ->
  unit

(** Register a meter callback that exposes the CSC cache counters
    as OpenTelemetry cumulative monotonic sums.

    [metrics_fn] is a 0-argument closure that reads the current
    counters; the typical caller passes [(fun () -> Client.cache_metrics c)].
    Returning [None] (CSC not configured for that client) emits no
    metrics for that tick.

    Six metrics are emitted per tick, named [<name>.<counter>]:
    {ul
    {- [hits], [misses] — read accounting.}
    {- [evicts.budget] — entries pushed out by byte-budget pressure.}
    {- [evicts.ttl] — entries pushed out by TTL safety net.}
    {- [invalidations] — entries removed by server-invalidation pushes.}
    {- [puts] — total [Cache.put] calls (incl. oversize-rejects).}}

    Default [name] is ["valkey.cache"]. With no exporter
    configured the cost is the OTel registry's no-op path
    (a list-of-callbacks walk on collect, each of which is a
    cheap atomic read).

    Idempotent at the OTel layer: each call adds another
    callback. Don't call this twice for the same client unless
    you want duplicate metrics. *)
val observe_cache_metrics :
  ?name:string ->
  (unit -> Cache.metrics option) ->
  unit
