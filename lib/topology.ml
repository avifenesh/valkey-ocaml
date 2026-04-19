module Node = struct
  type role = Primary | Replica

  type health = Online | Fail | Loading

  type t = {
    id : string;
    endpoint : string option;
    ip : string option;
    hostname : string option;
    port : int option;
    tls_port : int option;
    role : role;
    health : health;
    replication_offset : int64;
    availability_zone : string option;
  }

  let role_to_string = function Primary -> "primary" | Replica -> "replica"
  let health_to_string = function
    | Online -> "online" | Fail -> "fail" | Loading -> "loading"
end

module Shard = struct
  type slot_range = { start_ : int; end_ : int }

  type t = {
    id : string option;
    slots : slot_range list;
    primary : Node.t;
    replicas : Node.t list;
  }
end

type t = {
  shards : Shard.t list;
  slot_map : Shard.t option array;   (* size 16384 *)
  sha_cache : string;
}

let shards t = t.shards

let shard_for_slot t slot =
  if slot < 0 || slot >= Slot.slot_count then None
  else t.slot_map.(slot)

let node_for_slot t slot =
  match shard_for_slot t slot with
  | Some s -> s.primary
  | None ->
      invalid_arg (Printf.sprintf "no shard for slot %d" slot)

let primaries t = List.map (fun (s : Shard.t) -> s.primary) t.shards

let replicas t =
  List.concat_map (fun (s : Shard.t) -> s.replicas) t.shards

let all_nodes t = primaries t @ replicas t

(* ---------- parser ---------- *)

(* Robust map accessor: RESP3 Map or RESP2 flat Array. Entries can be
   Bulk_string or Simple_string keys. *)
let map_entries = function
  | Resp3.Map kvs -> Ok kvs
  | Resp3.Array items ->
      let rec pair acc = function
        | [] -> Ok (List.rev acc)
        | [ _ ] -> Error "odd number of entries in map-as-array"
        | k :: v :: rest -> pair ((k, v) :: acc) rest
      in
      pair [] items
  | other ->
      Error
        (Format.asprintf "expected map, got %a" Resp3.pp other)

let key_name = function
  | Resp3.Bulk_string s | Resp3.Simple_string s -> Some s
  | _ -> None

let find_field kvs name =
  List.find_opt
    (fun (k, _) -> match key_name k with Some n -> n = name | None -> false)
    kvs
  |> Option.map snd

let str_opt = function
  | Resp3.Bulk_string s | Resp3.Simple_string s -> Some s
  | _ -> None

let int_opt = function
  | Resp3.Integer n -> Some (Int64.to_int n)
  | Resp3.Bulk_string s -> (try Some (int_of_string s) with _ -> None)
  | _ -> None

let int64_opt = function
  | Resp3.Integer n -> Some n
  | Resp3.Bulk_string s -> (try Some (Int64.of_string s) with _ -> None)
  | _ -> None

let parse_role = function
  | "master" | "primary" -> Ok Node.Primary
  | "replica" | "slave" -> Ok Node.Replica
  | other -> Error (Printf.sprintf "unknown role %S" other)

let parse_health = function
  | "online" -> Ok Node.Online
  | "fail" -> Ok Node.Fail
  | "loading" -> Ok Node.Loading
  | other -> Error (Printf.sprintf "unknown health %S" other)

let parse_node (v : Resp3.t) : (Node.t, string) result =
  match map_entries v with
  | Error e -> Error e
  | Ok kvs ->
      let field name = find_field kvs name in
      let get_str name = Option.bind (field name) str_opt in
      let get_int name = Option.bind (field name) int_opt in
      let get_int64 name = Option.bind (field name) int64_opt in
      let id = get_str "id" in
      let role_s = get_str "role" in
      let health_s = get_str "health" in
      (match id, role_s, health_s with
       | None, _, _ -> Error "node missing id"
       | _, None, _ -> Error "node missing role"
       | _, _, None -> Error "node missing health"
       | Some id, Some role_s, Some health_s ->
           (match parse_role role_s, parse_health health_s with
            | Error e, _ | _, Error e -> Error e
            | Ok role, Ok health ->
                Ok
                  { Node.
                    id;
                    endpoint = get_str "endpoint";
                    ip = get_str "ip";
                    hostname = get_str "hostname";
                    port = get_int "port";
                    tls_port = get_int "tls-port";
                    role;
                    health;
                    replication_offset =
                      (match get_int64 "replication-offset" with
                       | Some n -> n | None -> 0L);
                    availability_zone = get_str "availability-zone";
                  }))

let parse_slot_ranges = function
  | Resp3.Array items ->
      let rec pair acc = function
        | [] -> Ok (List.rev acc)
        | [ _ ] -> Error "odd number of slot range endpoints"
        | a :: b :: rest ->
            (match int_opt a, int_opt b with
             | Some start_, Some end_ ->
                 pair ({ Shard.start_; end_ } :: acc) rest
             | _, _ ->
                 Error
                   (Format.asprintf "slot range endpoint not integer: %a / %a"
                      Resp3.pp a Resp3.pp b))
      in
      pair [] items
  | other ->
      Error (Format.asprintf "slots: expected array, got %a" Resp3.pp other)

let parse_shard (v : Resp3.t) : (Shard.t, string) result =
  match map_entries v with
  | Error e -> Error e
  | Ok kvs ->
      let field name = find_field kvs name in
      let id =
        match field "id" with Some v -> str_opt v | None -> None
      in
      let slots_r =
        match field "slots" with
        | None -> Error "shard missing slots"
        | Some v -> parse_slot_ranges v
      in
      let nodes_r =
        match field "nodes" with
        | None -> Error "shard missing nodes"
        | Some (Resp3.Array items) ->
            let rec loop acc = function
              | [] -> Ok (List.rev acc)
              | n :: rest ->
                  (match parse_node n with
                   | Ok node -> loop (node :: acc) rest
                   | Error e -> Error e)
            in
            loop [] items
        | Some other ->
            Error
              (Format.asprintf "shard nodes: expected array, got %a"
                 Resp3.pp other)
      in
      (match slots_r, nodes_r with
       | Error e, _ | _, Error e -> Error e
       | Ok slots, Ok nodes ->
           let primary, replicas =
             List.partition
               (fun (n : Node.t) -> n.role = Node.Primary)
               nodes
           in
           (match primary with
            | [] -> Error "shard has no primary"
            | p :: _extra ->
                (* Valkey normally has exactly one primary per shard; if the
                   server reports more, pick the first (matches GLIDE). *)
                Ok { Shard.id; slots; primary = p; replicas }))

let build_slot_map shards =
  let m = Array.make Slot.slot_count None in
  List.iter
    (fun (s : Shard.t) ->
      List.iter
        (fun { Shard.start_; end_ } ->
          for i = start_ to end_ do
            if i >= 0 && i < Slot.slot_count then m.(i) <- Some s
          done)
        s.slots)
    shards;
  m

let serialize_node (n : Node.t) =
  Printf.sprintf
    "id=%s;role=%s;health=%s;ip=%s;host=%s;port=%s;tls=%s;ep=%s;az=%s;ro=%Ld"
    n.id
    (Node.role_to_string n.role)
    (Node.health_to_string n.health)
    (Option.value n.ip ~default:"")
    (Option.value n.hostname ~default:"")
    (Option.fold n.port ~none:"" ~some:string_of_int)
    (Option.fold n.tls_port ~none:"" ~some:string_of_int)
    (Option.value n.endpoint ~default:"")
    (Option.value n.availability_zone ~default:"")
    n.replication_offset

let serialize_shard (s : Shard.t) =
  let slots =
    List.map (fun { Shard.start_; end_ } ->
        Printf.sprintf "%d-%d" start_ end_) s.slots
    |> List.sort compare
    |> String.concat ","
  in
  let primary_s = serialize_node s.primary in
  let replicas_s =
    List.map serialize_node s.replicas
    |> List.sort compare
    |> String.concat "|"
  in
  Printf.sprintf "shard[id=%s;slots=%s;primary=%s;replicas=%s]"
    (Option.value s.id ~default:"")
    slots primary_s replicas_s

let build_sha shards =
  let canonical =
    List.map serialize_shard shards
    |> List.sort compare
    |> String.concat "\n"
  in
  Digestif.SHA1.digest_string canonical |> Digestif.SHA1.to_hex

let of_cluster_shards (v : Resp3.t) =
  match v with
  | Resp3.Array shard_items ->
      let rec loop acc = function
        | [] -> Ok (List.rev acc)
        | s :: rest ->
            (match parse_shard s with
             | Ok sh -> loop (sh :: acc) rest
             | Error e -> Error e)
      in
      (match loop [] shard_items with
       | Error e -> Error e
       | Ok shards ->
           Ok { shards;
                slot_map = build_slot_map shards;
                sha_cache = build_sha shards })
  | other ->
      Error (Format.asprintf "CLUSTER SHARDS: expected array, got %a"
               Resp3.pp other)

let serialize t =
  List.map serialize_shard t.shards
  |> List.sort compare
  |> String.concat "\n"

let sha t = t.sha_cache
