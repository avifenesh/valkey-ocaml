(* Bulk import + bulk lookup across cluster slots.

   Populates 1000 keys via mset_cluster, fetches them back via
   mget_cluster, cleans up via del_cluster. Shows the wall-clock
   speedup of the batch path over a per-key loop. *)

module B = Valkey.Batch
module C = Valkey.Client
module CR = Valkey.Cluster_router

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> failwith m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->

      let n = 1000 in
      let kvs =
        List.init n (fun i ->
            Printf.sprintf "ex:bulk:%d" i,
            Printf.sprintf "value-%d" i)
      in
      let keys = List.map fst kvs in

      let _ = B.del_cluster client keys in

      let t0 = Unix.gettimeofday () in
      (match B.mset_cluster client kvs with
       | Ok () ->
           Printf.printf "mset_cluster %d keys: %.3fs\n"
             n (Unix.gettimeofday () -. t0)
       | Error e ->
           Format.eprintf "mset: %a@." Valkey.Connection.Error.pp e);

      let t1 = Unix.gettimeofday () in
      (match B.mget_cluster client keys with
       | Ok pairs ->
           Printf.printf "mget_cluster %d keys: %.3fs\n"
             (List.length pairs) (Unix.gettimeofday () -. t1);
           let hits =
             List.length (List.filter (fun (_, v) -> v <> None) pairs)
           in
           Printf.printf "  hits: %d / %d\n" hits n
       | Error e ->
           Format.eprintf "mget: %a@." Valkey.Connection.Error.pp e);

      (* Compare with per-key loop — expect this to be much slower
         because each call is a full request/reply round-trip. *)
      let t2 = Unix.gettimeofday () in
      let per_key =
        List.fold_left
          (fun n k ->
            match C.get client k with Ok (Some _) -> n + 1 | _ -> n)
          0 keys
      in
      Printf.printf "per-key loop %d gets: %.3fs  (hits %d)\n"
        n (Unix.gettimeofday () -. t2) per_key;

      (match B.del_cluster client keys with
       | Ok removed ->
           Printf.printf "del_cluster removed %d keys\n" removed
       | Error e ->
           Format.eprintf "del: %a@." Valkey.Connection.Error.pp e)
