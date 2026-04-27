(* Pure-unit test suites — no Valkey server required. These run
   under [dune build @runtest] and ship in opam CI. Integration
   suites that need a live server live in [run_tests.ml] and are
   invoked explicitly with [dune exec test/run_tests.exe]. *)
let () =
  Alcotest.run "valkey-unit"
    [ "resp3", Test_resp3.tests;
      "resp3 round-trip", Test_resp3_roundtrip.tests;
      "retry state machine", Test_retry_state.tests;
      "byte_reader", Test_byte_reader.tests;
      "valkey_error", Test_valkey_error.tests;
      "slot", Test_slot.tests;
      "topology", Test_topology.tests;
      "discovery", Test_discovery.tests;
      "redirect", Test_redirect.tests;
      "command_spec", Test_command_spec.tests;
      "cache", Test_cache.tests;
      "invalidation parser", Test_invalidation.tests;
      "inflight", Test_inflight.tests;
      "csc optin (pure)", Test_csc_optin_unit.tests;
      "observability (pure)", Test_observability.tests;
    ]
