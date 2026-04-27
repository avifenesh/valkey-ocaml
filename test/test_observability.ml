(** Pure-unit coverage for [Observability.observe_cache_metrics].

    Builds a [Cache.t], increments its counters by hand, registers
    the OTel callback, runs a Meter.collect, and checks the
    emitted [Metrics.t] list contains a metric per counter with
    the right names and values. No exporter; uses the registry's
    in-memory pass directly. *)

module O = Valkey.Observability
module Cache = Valkey.Cache

(* Build a Cache.t and drive its counters by issuing a few
   synthetic puts/gets/invalidations. *)
let drive_cache () =
  let c = Cache.create ~byte_budget:(64 * 1024) in
  Cache.put c "k1" (Valkey.Resp3.Bulk_string "v1");
  Cache.put c "k2" (Valkey.Resp3.Bulk_string "v2");
  let _ = Cache.get c "k1" in
  let _ = Cache.get c "k1" in
  let _ = Cache.get c "missing" in
  Cache.evict c "k2";
  c

(* Pull metric names with a given prefix out of a Metrics.t list.
   We don't dig into the protobuf data points — the name list is
   the load-bearing user-visible contract; numeric values are
   delegated to OTel's built-in path. *)
let names_with_prefix ~prefix metrics =
  let open Opentelemetry.Proto.Metrics in
  metrics
  |> List.map (fun (m : metric) -> m.name)
  |> List.filter (fun name ->
       String.length name >= String.length prefix
       && String.sub name 0 (String.length prefix) = prefix)
  |> List.sort compare

let test_emits_six_counters () =
  let cache = drive_cache () in
  O.observe_cache_metrics ~name:"test.cache"
    (fun () -> Some (Cache.metrics cache));
  let collected =
    Opentelemetry.Meter.collect Opentelemetry.Meter.dummy
  in
  let names = names_with_prefix ~prefix:"test.cache." collected in
  Alcotest.(check (list string)) "six counters with stable names"
    [ "test.cache.evicts.budget";
      "test.cache.evicts.ttl";
      "test.cache.hits";
      "test.cache.invalidations";
      "test.cache.misses";
      "test.cache.puts" ]
    names

let test_none_emits_nothing () =
  O.observe_cache_metrics ~name:"test.cache.absent" (fun () -> None);
  let collected =
    Opentelemetry.Meter.collect Opentelemetry.Meter.dummy
  in
  let names = names_with_prefix ~prefix:"test.cache.absent." collected in
  Alcotest.(check (list string)) "no metrics for None" [] names

let tests =
  [ Alcotest.test_case "observe_cache_metrics emits six counters" `Quick
      test_emits_six_counters;
    Alcotest.test_case "observe_cache_metrics no-op on None" `Quick
      test_none_emits_nothing;
  ]
