(* Cluster routing: all four [Read_from] modes side by side.

   Spins up one cluster client and runs the same readonly probe
   (CLUSTER MYID) under each Read_from setting, recording which
   node served each call. Demonstrates that:

     - Primary always lands on the primary
     - Prefer_replica spreads across replicas (now randomised, see
       lib/cluster_router.ml:pick_random)
     - Az_affinity prefers replicas with a matching availability zone
     - Az_affinity_replicas_and_primary widens the candidate pool

   Run against the docker-compose.cluster.yml cluster:
     docker compose -f docker-compose.cluster.yml up -d
     dune exec examples/02-cluster/routing.exe *)

module C = Valkey.Client
module CR = Valkey.Cluster_router
module R = Valkey.Router
module E = Valkey.Connection.Error

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let myid_via ~client ~slot ~read_from =
  match
    C.custom client ~target:(R.Target.By_slot slot) ~read_from
      [| "CLUSTER"; "MYID" |]
  with
  | Ok (Valkey.Resp3.Bulk_string id)
  | Ok (Valkey.Resp3.Simple_string id) -> id
  | Ok other ->
      Format.asprintf "<unexpected reply: %a>" Valkey.Resp3.pp other
  | Error e ->
      Format.asprintf "<error: %a>" E.pp e

(* Run [n] probes through one Read_from mode and tally per-node hits. *)
let probe ~client ~slot ~read_from ~label n =
  let counts = Hashtbl.create 6 in
  for _ = 1 to n do
    let id = myid_via ~client ~slot ~read_from in
    let prev = try Hashtbl.find counts id with Not_found -> 0 in
    Hashtbl.replace counts id (prev + 1)
  done;
  Printf.printf "\n[%s] for slot %d, %d probes:\n" label slot n;
  Hashtbl.iter
    (fun id n -> Printf.printf "  %d  %s\n" n id)
    counts

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  Random.self_init ();
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error msg -> failwith ("cluster_router: " ^ msg)
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->

      (* Slot 0: any one shard. With 3 primaries we expect to see
         the primary plus one replica show up under Prefer_replica. *)
      let slot = 0 in
      probe ~client ~slot ~read_from:R.Read_from.Primary
        ~label:"Primary" 12;
      probe ~client ~slot ~read_from:R.Read_from.Prefer_replica
        ~label:"Prefer_replica" 12;

      (* AZ-affinity fallback chain (3 tiers):
           1. replica in the requested AZ
           2. any other replica
           3. primary
         The docker-compose cluster doesn't set availability_zone on
         any node, so tier 1 always returns []. Tier 2 kicks in and
         spreads across the local replicas; tier 3 only fires if the
         shard somehow has zero replicas (e.g. mid-failover). The
         distribution below should match Prefer_replica because tier
         1 is empty.

         To exercise tier 1 against a real multi-AZ cluster (e.g.
         AWS ElastiCache), pass the AZ string the cluster reports
         (visible via `CLUSTER SHARDS` -> "availability-zone" field
         on each node). *)
      probe ~client ~slot
        ~read_from:(R.Read_from.Az_affinity { az = "us-east-1a" })
        ~label:"Az_affinity (no in-AZ -> tier 2: any replica)" 12;
      probe ~client ~slot
        ~read_from:
          (R.Read_from.Az_affinity_replicas_and_primary
             { az = "us-east-1a" })
        ~label:"Az_affinity_replicas_and_primary (no in-AZ -> tier 2)"
        12
