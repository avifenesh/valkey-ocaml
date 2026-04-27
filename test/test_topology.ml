module T = Valkey.Topology
module R = Valkey.Resp3

(* Build a Resp3 Map of (Bulk_string key, value) pairs. *)
let map kvs =
  R.Map (List.map (fun (k, v) -> R.Bulk_string k, v) kvs)

let node ~id ~ip ~port ~role ~health ?hostname ?az ?(tls_port = None) () =
  map
    ([ "id", R.Bulk_string id;
       "endpoint", R.Bulk_string ip;
       "ip", R.Bulk_string ip;
       "port", R.Integer (Int64.of_int port);
       "role", R.Bulk_string role;
       "health", R.Bulk_string health;
       "replication-offset", R.Integer 1234L;
     ]
     @ (match hostname with
        | None -> []
        | Some h -> [ "hostname", R.Bulk_string h ])
     @ (match tls_port with
        | None -> []
        | Some p -> [ "tls-port", R.Integer (Int64.of_int p) ])
     @ (match az with
        | None -> []
        | Some a -> [ "availability-zone", R.Bulk_string a ]))

let shard ~id_opt ~slot_ranges ~nodes =
  let slot_items =
    List.concat_map
      (fun (s, e) ->
        [ R.Integer (Int64.of_int s); R.Integer (Int64.of_int e) ])
      slot_ranges
  in
  map
    ((match id_opt with
      | None -> []
      | Some id -> [ "id", R.Bulk_string id ])
     @ [ "slots", R.Array slot_items;
         "nodes", R.Array nodes ])

let sample_cluster =
  R.Array
    [ shard
        ~id_opt:(Some "shard-a")
        ~slot_ranges:[ 0, 5460 ]
        ~nodes:
          [ node ~id:"n1" ~ip:"10.0.0.1" ~port:6379 ~role:"master"
              ~health:"online" ~az:"us-east-1a" ();
            node ~id:"n2" ~ip:"10.0.0.2" ~port:6379 ~role:"replica"
              ~health:"online" ~az:"us-east-1b" ();
          ];
      shard
        ~id_opt:(Some "shard-b")
        ~slot_ranges:[ 5461, 10922 ]
        ~nodes:
          [ node ~id:"n3" ~ip:"10.0.0.3" ~port:6379 ~role:"master"
              ~health:"online" ~az:"us-east-1a" ();
            node ~id:"n4" ~ip:"10.0.0.4" ~port:6379 ~role:"replica"
              ~health:"online" ~az:"us-east-1c" ();
          ];
      shard
        ~id_opt:(Some "shard-c")
        ~slot_ranges:[ 10923, 16383 ]
        ~nodes:
          [ node ~id:"n5" ~ip:"10.0.0.5" ~port:6379 ~role:"master"
              ~health:"online" ~az:"us-east-1b" ();
            node ~id:"n6" ~ip:"10.0.0.6" ~port:6379 ~role:"replica"
              ~health:"online" ~az:"us-east-1c" ();
          ];
    ]

let test_parse_ok () =
  match T.of_cluster_shards sample_cluster with
  | Ok t ->
      Alcotest.(check int) "shards count" 3 (List.length (T.shards t));
      Alcotest.(check int) "primaries count" 3 (List.length (T.primaries t));
      Alcotest.(check int) "replicas count" 3 (List.length (T.replicas t));
      Alcotest.(check int) "all nodes" 6 (List.length (T.all_nodes t))
  | Error e -> Alcotest.failf "parse failed: %s" e

let test_slot_lookup () =
  match T.of_cluster_shards sample_cluster with
  | Error e -> Alcotest.failf "parse: %s" e
  | Ok t ->
      let n0 = T.node_for_slot t 0 in
      Alcotest.(check string) "slot 0 -> n1" "n1" n0.id;
      let n_mid = T.node_for_slot t 8000 in
      Alcotest.(check string) "slot 8000 -> n3" "n3" n_mid.id;
      let n_last = T.node_for_slot t 16383 in
      Alcotest.(check string) "slot 16383 -> n5" "n5" n_last.id

let test_replicas_visible () =
  match T.of_cluster_shards sample_cluster with
  | Error e -> Alcotest.failf "parse: %s" e
  | Ok t ->
      (match T.shard_for_slot t 100 with
       | Some s ->
           Alcotest.(check int) "1 replica in shard" 1 (List.length s.replicas);
           let rep = List.hd s.replicas in
           Alcotest.(check string) "replica id" "n2" rep.id;
           Alcotest.(check bool) "replica role"
             true (rep.role = T.Node.Replica)
       | None -> Alcotest.fail "shard_for_slot 100 = None")

let test_az_preserved () =
  match T.of_cluster_shards sample_cluster with
  | Error e -> Alcotest.failf "parse: %s" e
  | Ok t ->
      let n1 = T.node_for_slot t 0 in
      Alcotest.(check (option string))
        "AZ preserved" (Some "us-east-1a") n1.availability_zone

let test_sha_differs_on_change () =
  let altered =
    R.Array
      (match sample_cluster with
       | R.Array [ s1; _; s3 ] ->
           [ s1;
             shard ~id_opt:(Some "shard-b")
               ~slot_ranges:[ 5461, 10922 ]
               ~nodes:
                 [ node ~id:"n3-moved" ~ip:"10.0.0.99" ~port:6379
                     ~role:"master" ~health:"online" ();
                   node ~id:"n4" ~ip:"10.0.0.4" ~port:6379 ~role:"replica"
                     ~health:"online" ();
                 ];
             s3;
           ]
       | _ -> assert false)
  in
  match T.of_cluster_shards sample_cluster,
        T.of_cluster_shards altered
  with
  | Ok a, Ok b ->
      Alcotest.(check bool) "different SHA for different input"
        true (T.sha a <> T.sha b)
  | Error e, _ | _, Error e -> Alcotest.failf "parse: %s" e

let test_parse_error_no_primary () =
  let bad =
    R.Array
      [ shard ~id_opt:None
          ~slot_ranges:[ 0, 100 ]
          ~nodes:
            [ node ~id:"only-replica" ~ip:"10.0.0.1" ~port:6379
                ~role:"replica" ~health:"online" ();
            ];
      ]
  in
  match T.of_cluster_shards bad with
  | Error _ -> ()
  | Ok _ -> Alcotest.fail "expected error on shard with no primary"

let tests =
  [ Alcotest.test_case "parse ok" `Quick test_parse_ok;
    Alcotest.test_case "slot lookup" `Quick test_slot_lookup;
    Alcotest.test_case "replicas visible" `Quick test_replicas_visible;
    Alcotest.test_case "AZ preserved" `Quick test_az_preserved;
    Alcotest.test_case "SHA differs on change" `Quick test_sha_differs_on_change;
    Alcotest.test_case "parse error on no-primary shard" `Quick
      test_parse_error_no_primary;
  ]
