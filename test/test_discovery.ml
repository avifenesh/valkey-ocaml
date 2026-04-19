module D = Valkey.Discovery
module T = Valkey.Topology
module R = Valkey.Resp3

(* Build a minimal valid topology with an identifiable "tag" (node id). *)
let topo_with_primary_id id =
  let node =
    R.Map
      [ R.Bulk_string "id", R.Bulk_string id;
        R.Bulk_string "endpoint", R.Bulk_string "10.0.0.1";
        R.Bulk_string "ip", R.Bulk_string "10.0.0.1";
        R.Bulk_string "port", R.Integer 6379L;
        R.Bulk_string "role", R.Bulk_string "master";
        R.Bulk_string "health", R.Bulk_string "online";
        R.Bulk_string "replication-offset", R.Integer 0L;
      ]
  in
  let shard =
    R.Map
      [ R.Bulk_string "slots",
        R.Array [ R.Integer 0L; R.Integer 16383L ];
        R.Bulk_string "nodes", R.Array [ node ];
      ]
  in
  match T.of_cluster_shards (R.Array [ shard ]) with
  | Ok t -> t
  | Error e -> failwith ("test fixture broken: " ^ e)

let a = topo_with_primary_id "A"
let b = topo_with_primary_id "B"
let c = topo_with_primary_id "C"

let test_all_agree_wins () =
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:3 ~views:[ Some a; Some a; Some a ]
  with
  | D.Agreed t -> Alcotest.(check string) "sha" (T.sha a) (T.sha t)
  | _ -> Alcotest.fail "expected Agreed"

let test_majority_wins () =
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:3 ~views:[ Some a; Some a; Some b ]
  with
  | D.Agreed t -> Alcotest.(check string) "winner" (T.sha a) (T.sha t)
  | _ -> Alcotest.fail "expected Agreed"

let test_tie_rejected () =
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:4 ~views:[ Some a; Some a; Some b; Some b ]
  with
  | D.No_agreement -> ()
  | _ -> Alcotest.fail "expected No_agreement on tie"

let test_below_ratio_fallback () =
  (* 1 of 10 replied — below 20% threshold *)
  let views =
    Some a
    :: List.init 9 (fun _ -> None)
  in
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:10 ~views
  with
  | D.Agreed_fallback t -> Alcotest.(check string) "fb" (T.sha a) (T.sha t)
  | _ -> Alcotest.fail "expected Agreed_fallback"

let test_all_failed_is_no_agreement () =
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:3 ~views:[ None; None; None ]
  with
  | D.No_agreement -> ()
  | _ -> Alcotest.fail "expected No_agreement"

let test_small_cluster_skips_quorum () =
  (* Queried < min; just accept the first successful view. *)
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3
      ~queried:2 ~views:[ Some a; Some b ]
  with
  | D.Agreed _ -> ()
  | _ -> Alcotest.fail "expected Agreed when below min"

let test_three_way_split_winner () =
  (* 4 a, 2 b, 2 c — a wins with 4/8 = 50% > 20% *)
  match
    D.select ~agreement_ratio:0.2 ~min_nodes_for_quorum:3 ~queried:8
      ~views:
        [ Some a; Some a; Some a; Some a;
          Some b; Some b;
          Some c; Some c ]
  with
  | D.Agreed t -> Alcotest.(check string) "a wins" (T.sha a) (T.sha t)
  | _ -> Alcotest.fail "expected Agreed"

let tests =
  [ Alcotest.test_case "all 3 agree -> Agreed" `Quick test_all_agree_wins;
    Alcotest.test_case "majority 2/3 -> Agreed" `Quick test_majority_wins;
    Alcotest.test_case "2-2 tie -> No_agreement" `Quick test_tie_rejected;
    Alcotest.test_case "below ratio -> Agreed_fallback" `Quick
      test_below_ratio_fallback;
    Alcotest.test_case "all failed -> No_agreement" `Quick
      test_all_failed_is_no_agreement;
    Alcotest.test_case "below min_quorum -> accept any" `Quick
      test_small_cluster_skips_quorum;
    Alcotest.test_case "3-way split with clear winner" `Quick
      test_three_way_split_winner;
  ]
