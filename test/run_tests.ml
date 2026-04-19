let () =
  Alcotest.run "valkey"
    [ "resp3", Test_resp3.tests;
      "byte_reader", Test_byte_reader.tests;
      "valkey_error", Test_valkey_error.tests;
      "slot", Test_slot.tests;
      "topology", Test_topology.tests;
      "discovery", Test_discovery.tests;
      "redirect", Test_redirect.tests;
      "command_spec", Test_command_spec.tests;
      "connection (needs docker valkey :6379)", Test_connection.tests;
      "client (needs docker valkey :6379)", Test_client.tests;
      "cluster (needs docker compose -f docker-compose.cluster.yml)",
        Test_cluster.tests;
    ]
