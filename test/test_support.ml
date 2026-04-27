(** Shared helpers across integration tests.

    Until this module landed, [seeds], [force_skip],
    [cluster_reachable], [sleep_ms], and the CLUSTER NODES parsing
    helpers were duplicated verbatim across a dozen test files —
    surfaced by a code-review pass on Phase 8. Centralised here so
    a future change to seed list / readiness predicate / parser
    edge case lands in one place. *)

module Conn = Valkey.Connection

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

(* Set [VALKEY_CLUSTER=skip] in the environment to force-skip
   cluster-dependent tests even if the cluster is reachable.
   Useful when running the integration suite under a network-
   isolated harness where the cluster ports are bound but the
   gossip plane isn't. *)
let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

(* True iff a TCP connection to the first seed succeeds. Cheap
   probe; doesn't talk RESP3 beyond the connection handshake. *)
let cluster_reachable () =
  if force_skip () then false
  else
    try
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let clock = Eio.Stdenv.clock env in
      let host, port = List.hd seeds in
      let conn =
        Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
          ~host ~port ()
      in
      Conn.close conn;
      true
    with _ -> false

let skipped name =
  Printf.printf "    [SKIP] %s (cluster not reachable)\n" name

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* ---------- CLUSTER NODES parsing ------------------------------

   Each line: <id> <host:port@bus[,hostname]> <flags>
              <master-id-or--> <ping> <pong> <epoch> <link>
              [<slot-range> ...]

   Replica lines have flags containing "slave" / "replica" and a
   non-"-" master-id field. Slot ranges (e.g. "0-5460") only
   appear on master lines. *)

module Cluster_nodes = struct
  module R = Valkey.Resp3

  (* "host:port@bus[,hostname]" -> (host, port). The first
     comma-separated chunk is the announced address; the
     hostname suffix is for downstream resolution and isn't
     load-bearing here. *)
  let parse_addr addr_field =
    let primary_chunk = List.hd (String.split_on_char ',' addr_field) in
    match String.index_opt primary_chunk ':' with
    | None -> None
    | Some i ->
        let host = String.sub primary_chunk 0 i in
        let rest =
          String.sub primary_chunk (i + 1)
            (String.length primary_chunk - i - 1)
        in
        let port =
          let at =
            try String.index rest '@' with Not_found -> String.length rest
          in
          int_of_string (String.sub rest 0 at)
        in
        Some (host, port)

  let line_is_replica flags =
    List.exists (fun f -> f = "slave" || f = "replica")
      (String.split_on_char ',' flags)

  let line_is_master flags =
    List.exists (fun f -> f = "master")
      (String.split_on_char ',' flags)

  let line_owns_slot fields slot =
    match fields with
    | _id :: _addr :: _flags :: _master :: _ping :: _pong :: _epoch :: _link
      :: ranges ->
        List.exists
          (fun r ->
            match String.split_on_char '-' r with
            | [ a ] ->
                (try int_of_string a = slot with _ -> false)
            | [ a; b ] ->
                (try
                   let lo = int_of_string a and hi = int_of_string b in
                   lo <= slot && slot <= hi
                 with _ -> false)
            | _ -> false)
          ranges
    | _ -> false

  (* Open a transient connection to the first seed and pull the
     CLUSTER NODES reply. Each call opens its own [Eio.Switch.run]
     so the helper can be invoked from anywhere. *)
  let text env =
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let clock = Eio.Stdenv.clock env in
    let h, p = List.hd seeds in
    let conn =
      Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
        ~host:h ~port:p ()
    in
    let r = Conn.request conn [| "CLUSTER"; "NODES" |] in
    Conn.close conn;
    match r with
    | Ok (R.Bulk_string s | R.Simple_string s
          | R.Verbatim_string { data = s; _ }) -> s
    | Ok v -> Alcotest.failf "CLUSTER NODES: unexpected %a" R.pp v
    | Error e ->
        Alcotest.failf "CLUSTER NODES: %a" Conn.Error.pp e

  let parse text =
    String.split_on_char '\n' text
    |> List.filter (fun l -> String.length l > 0)
    |> List.map (fun l -> String.split_on_char ' ' l)

  (* Returns the master-id of the primary owning [slot], if any. *)
  let primary_owning_slot env ~slot =
    let parsed = parse (text env) in
    List.find_map
      (fun fields ->
        match fields with
        | id :: _addr :: flags :: _ ->
            if line_is_master flags && line_owns_slot fields slot
            then Some id else None
        | _ -> None)
      parsed

  (* Returns any master-id that is NOT [not_id]. *)
  let other_primary env ~not_id =
    let parsed = parse (text env) in
    List.find_map
      (fun fields ->
        match fields with
        | id :: _addr :: flags :: _ ->
            if line_is_master flags && id <> not_id
            then Some id else None
        | _ -> None)
      parsed

  (* Returns the (host, port) for [node_id], if known. *)
  let addr_of_id env ~node_id =
    let parsed = parse (text env) in
    List.find_map
      (fun fields ->
        match fields with
        | id :: addr :: _ when id = node_id -> parse_addr addr
        | _ -> None)
      parsed

  (* Pick the first replica of the master owning [slot]. *)
  let replica_of_slot_owner env ~slot =
    let parsed = parse (text env) in
    let owner_id =
      List.find_map
        (fun fields ->
          match fields with
          | id :: _addr :: flags :: _ ->
              if line_is_master flags && line_owns_slot fields slot
              then Some id else None
          | _ -> None)
        parsed
    in
    match owner_id with
    | None -> None
    | Some owner_id ->
        List.find_map
          (fun fields ->
            match fields with
            | _id :: addr :: flags :: master :: _ ->
                if line_is_replica flags && master = owner_id
                then parse_addr addr else None
            | _ -> None)
          parsed
end
