let () =
  Alcotest.run "valkey"
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
      "connection (needs docker valkey :6379)", Test_connection.tests;
      "client (needs docker valkey :6379)", Test_client.tests;
      "csc tracking (needs docker valkey :6379 >=7.4)",
        Test_csc_tracking.tests;
      "csc invalidation (needs docker valkey :6379 >=7.4)",
        Test_csc_invalidation.tests;
      "csc hash+set (needs docker valkey :6379 >=7.4)",
        Test_csc_hash_set.tests;
      "csc mget (needs docker valkey :6379 >=7.4)",
        Test_csc_mget.tests;
      "csc cluster (needs docker compose -f docker-compose.cluster.yml)",
        Test_csc_cluster.tests;
      "csc lifecycle (standalone + optional cluster failover)",
        Test_csc_lifecycle.tests;
      "csc bcast (needs docker valkey :6379 >=7.4)",
        Test_csc_bcast.tests;
      "csc optin (needs docker valkey :6379 >=7.4)",
        Test_csc_optin.tests;
      "csc optin cluster (needs docker compose -f docker-compose.cluster.yml)",
        Test_csc_optin_cluster.tests;
      "csc optin migration (needs docker compose -f docker-compose.cluster.yml)",
        Test_csc_optin_migration.tests;
      "sorted set (needs docker valkey :6379)", Test_sorted_set.tests;
      "transaction (needs docker valkey :6379)", Test_transaction.tests;
      "pubsub (needs docker valkey :6379)", Test_pubsub.tests;
      "named_commands (needs docker valkey :6379)", Test_named_commands.tests;
      "bitmap (needs docker valkey :6379)", Test_bitmap.tests;
      "hll + generic (needs docker valkey :6379)",
        Test_generic_hll.tests;
      "geo (needs docker valkey :6379)", Test_geo.tests;
      "client admin (needs docker valkey :6379)",
        Test_client_admin.tests;
      "admin family (needs docker valkey :6379)",
        Test_admin_family.tests;
      "cluster (needs docker compose -f docker-compose.cluster.yml)",
        Test_cluster.tests;
      "command_spec property (needs docker compose -f docker-compose.cluster.yml)",
        Test_command_spec_property.tests;
      "batch (needs docker compose -f docker-compose.cluster.yml)",
        Test_batch.tests;
    ]
