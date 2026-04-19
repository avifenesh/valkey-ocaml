type selection =
  | Agreed of Topology.t
  | Agreed_fallback of Topology.t
  | No_agreement

let select ~agreement_ratio ~min_nodes_for_quorum ~queried ~views =
  let topos = List.filter_map (fun x -> x) views in
  match topos with
  | [] -> No_agreement
  | _ when queried < min_nodes_for_quorum ->
      Agreed (List.hd topos)
  | _ ->
      (* Group by SHA, find the group with the most members. Reject ties. *)
      let by_sha = Hashtbl.create 4 in
      List.iter
        (fun t ->
          let k = Topology.sha t in
          match Hashtbl.find_opt by_sha k with
          | None -> Hashtbl.add by_sha k (1, t)
          | Some (n, _) -> Hashtbl.replace by_sha k (n + 1, t))
        topos;
      let entries =
        Hashtbl.fold (fun _ v acc -> v :: acc) by_sha []
      in
      let sorted =
        List.sort (fun (a, _) (b, _) -> compare b a) entries
      in
      (match sorted with
       | [] -> No_agreement
       | [ (_, t) ] ->
           (* Only one distinct view. Check agreement ratio. *)
           let ratio = float_of_int (List.length topos)
                       /. float_of_int queried in
           if ratio >= agreement_ratio then Agreed t
           else Agreed_fallback t
       | (n1, t1) :: (n2, _) :: _ ->
           if n1 = n2 then No_agreement
           else
             let ratio = float_of_int n1 /. float_of_int queried in
             if ratio >= agreement_ratio then Agreed t1
             else Agreed_fallback t1)

let query_seed ~sw ~net ~clock ?domain_mgr ?connection_config (host, port) =
  let conn =
    try
      Some
        (Connection.connect ~sw ~net ~clock ?domain_mgr
           ?config:connection_config ~host ~port ())
    with _ -> None
  in
  match conn with
  | None -> None
  | Some c ->
      let topo =
        match Connection.request c [| "CLUSTER"; "SHARDS" |] with
        | Ok reply ->
            (match Topology.of_cluster_shards reply with
             | Ok t -> Some t
             | Error _ -> None)
        | Error _ -> None
      in
      (try Connection.close c with _ -> ());
      topo

let discover_from_seeds ~sw ~net ~clock ?domain_mgr ?connection_config
    ?(agreement_ratio = 0.2) ?(min_nodes_for_quorum = 3) ~seeds () =
  let views =
    Eio.Fiber.List.map
      (query_seed ~sw ~net ~clock ?domain_mgr ?connection_config)
      seeds
  in
  let queried = List.length seeds in
  match select ~agreement_ratio ~min_nodes_for_quorum ~queried ~views with
  | Agreed t | Agreed_fallback t -> Ok t
  | No_agreement -> Error "no topology agreement across seeds"
