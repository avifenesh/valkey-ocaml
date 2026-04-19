type result =
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
