let () =
  Alcotest.run "valkey"
    [ "resp3", Test_resp3.tests;
      "byte_reader", Test_byte_reader.tests;
      "valkey_error", Test_valkey_error.tests;
      "connection (needs docker valkey :6379)", Test_connection.tests;
      "client (needs docker valkey :6379)", Test_client.tests;
    ]
