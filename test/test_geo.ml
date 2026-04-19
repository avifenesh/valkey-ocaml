(* Geo command tests — Batch 1c. *)

module C = Valkey.Client
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379
let err_pp = E.pp

let with_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let c = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect ~finally:(fun () -> C.close c) (fun () -> f c)

let check_ok msg = function
  | Ok v -> v
  | Error e -> Alcotest.failf "%s: %a" msg err_pp e

(* Standard reference points for the test fixtures. Lat/lon for a
   few well-known places — exact values don't matter as long as
   they are stable. *)
let palermo =
  { C.longitude = 13.361389; latitude = 38.115556;
    member = "Palermo" }
let catania =
  { C.longitude = 15.087269; latitude = 37.502669;
    member = "Catania" }
let agrigento =
  { C.longitude = 13.583333; latitude = 37.316667;
    member = "Agrigento" }

let test_geoadd_and_count () =
  with_client @@ fun c ->
  let k = "geo:city" in
  ignore (C.del c [ k ]);
  let added1 =
    check_ok "GEOADD 3 new"
      (C.geoadd c k ~points:[ palermo; catania; agrigento ])
  in
  Alcotest.(check int) "3 added" 3 added1;
  (* Re-add same: should be 0 without CH. *)
  let added2 =
    check_ok "GEOADD same"
      (C.geoadd c k ~points:[ palermo ])
  in
  Alcotest.(check int) "no new" 0 added2;
  (* NX rejects updates. *)
  let added_nx =
    check_ok "GEOADD NX existing"
      (C.geoadd c k ~cond:C.Geoadd_nx ~points:[ palermo ])
  in
  Alcotest.(check int) "NX on existing = 0" 0 added_nx;
  ignore (C.del c [ k ])

let test_geodist () =
  with_client @@ fun c ->
  let k = "geo:dist" in
  ignore (C.del c [ k ]);
  ignore (C.geoadd c k ~points:[ palermo; catania ]);
  let km =
    check_ok "GEODIST km"
      (C.geodist c k ~member1:"Palermo" ~member2:"Catania"
         ~unit:C.Kilometers)
  in
  (match km with
   | Some d when d > 160.0 && d < 170.0 -> ()
   | Some d -> Alcotest.failf "unexpected distance %.2f km" d
   | None -> Alcotest.fail "nil distance on existing members");
  (* Missing member -> None. *)
  let missing =
    check_ok "GEODIST missing"
      (C.geodist c k ~member1:"Palermo" ~member2:"Nope")
  in
  Alcotest.(check bool) "None on missing" true (missing = None);
  ignore (C.del c [ k ])

let test_geopos_and_geohash () =
  with_client @@ fun c ->
  let k = "geo:pos" in
  ignore (C.del c [ k ]);
  ignore (C.geoadd c k ~points:[ palermo; catania ]);
  let pos =
    check_ok "GEOPOS"
      (C.geopos c k ~members:[ "Palermo"; "Nope"; "Catania" ])
  in
  Alcotest.(check int) "three slots" 3 (List.length pos);
  (match pos with
   | [ Some p; None; Some q ] ->
       Alcotest.(check bool) "Palermo lon ~= 13.36" true
         (abs_float (p.longitude -. 13.361389) < 0.01);
       Alcotest.(check bool) "Catania lat ~= 37.5" true
         (abs_float (q.latitude -. 37.502669) < 0.01)
   | _ -> Alcotest.fail "unexpected geopos shape");
  let hashes =
    check_ok "GEOHASH"
      (C.geohash c k ~members:[ "Palermo"; "Nope" ])
  in
  (match hashes with
   | [ Some h; None ] when String.length h = 11 -> ()
   | _ -> Alcotest.fail "unexpected geohash shape");
  ignore (C.del c [ k ])

let test_geosearch_radius_with_all_flags () =
  with_client @@ fun c ->
  let k = "geo:search" in
  ignore (C.del c [ k ]);
  ignore (C.geoadd c k ~points:[ palermo; catania; agrigento ]);
  let results =
    check_ok "GEOSEARCH BYRADIUS"
      (C.geosearch c k
         ~from:(C.From_lonlat
                  { longitude = 15.0; latitude = 37.5 })
         ~shape:(C.By_radius
                   { radius = 300.0; unit = C.Kilometers })
         ~order:C.Geo_asc
         ~with_coord:true
         ~with_dist:true
         ~with_hash:true)
  in
  (* All 3 cities are within 300 km of (15, 37.5). Nearest = Catania. *)
  Alcotest.(check int) "3 matches" 3 (List.length results);
  (match results with
   | first :: _ ->
       Alcotest.(check string) "nearest is Catania"
         "Catania" first.member;
       Alcotest.(check bool) "distance filled" true
         (first.distance <> None);
       Alcotest.(check bool) "hash filled" true (first.hash <> None);
       Alcotest.(check bool) "coord filled" true (first.coord <> None)
   | [] -> Alcotest.fail "empty results");
  ignore (C.del c [ k ])

let test_geosearchstore () =
  with_client @@ fun c ->
  let src = "{geo}:src" and dst = "{geo}:dst" in
  ignore (C.del c [ src; dst ]);
  ignore (C.geoadd c src ~points:[ palermo; catania; agrigento ]);
  let n =
    check_ok "GEOSEARCHSTORE"
      (C.geosearchstore c
         ~destination:dst ~source:src
         ~from:(C.From_lonlat
                  { longitude = 13.5; latitude = 37.8 })
         ~shape:(C.By_box
                   { width = 400.0; height = 400.0;
                     unit = C.Kilometers }))
  in
  if n < 1 then Alcotest.failf "stored %d, expected >= 1" n;
  ignore (C.del c [ src; dst ])

let tests =
  [ Alcotest.test_case "GEOADD + NX behaviour" `Quick
      test_geoadd_and_count;
    Alcotest.test_case "GEODIST + missing member" `Quick
      test_geodist;
    Alcotest.test_case "GEOPOS / GEOHASH with holes" `Quick
      test_geopos_and_geohash;
    Alcotest.test_case "GEOSEARCH BYRADIUS + WITH* decoding" `Quick
      test_geosearch_radius_with_all_flags;
    Alcotest.test_case "GEOSEARCHSTORE" `Quick test_geosearchstore;
  ]
