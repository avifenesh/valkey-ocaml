module Read_from = struct
  type t =
    | Primary
    | Prefer_replica
    | Az_affinity of { az : string }
    | Az_affinity_replicas_and_primary of { az : string }

  let default = Primary
end

module Target = struct
  type t =
    | Random
    | By_slot of int
    | By_node of string
    | By_channel of string
end

module Fan_target = struct
  type t =
    | All_nodes
    | All_primaries
    | All_replicas
end

type exec_fn =
  ?timeout:float -> Target.t -> Read_from.t -> string array ->
  (Resp3.t, Connection.Error.t) result

type exec_multi_fn =
  ?timeout:float -> Fan_target.t -> string array ->
  (string * (Resp3.t, Connection.Error.t) result) list

type connection_for_slot_fn = int -> Connection.t option
type endpoint_for_slot_fn = int -> (string * string * int) option

type t = {
  exec : exec_fn;
  exec_multi : exec_multi_fn;
  close : unit -> unit;
  primary : unit -> Connection.t option;
  connection_for_slot : connection_for_slot_fn;
  endpoint_for_slot : endpoint_for_slot_fn;
}
[@@warning "-69"]

let make ~exec ~exec_multi ~close ~primary ~connection_for_slot
    ~endpoint_for_slot =
  { exec; exec_multi; close; primary;
    connection_for_slot; endpoint_for_slot }

let standalone (conn : Connection.t) : t =
  let exec ?timeout _target _read_from args =
    Connection.request ?timeout conn args
  in
  let exec_multi ?timeout _fan_target args =
    [ Topology.standalone_node_id, Connection.request ?timeout conn args ]
  in
  (* Standalone has no topology of its own; [Cluster_pubsub] uses
     [endpoint_for_slot] and is cluster-only, so [None] here is
     appropriate — callers must not reach for endpoint info on a
     standalone router. *)
  { exec; exec_multi;
    close = (fun () -> Connection.close conn);
    primary = (fun () -> Some conn);
    connection_for_slot = (fun _ -> Some conn);
    endpoint_for_slot = (fun _ -> None);
  }

let exec ?timeout t target rf args = t.exec ?timeout target rf args
let exec_multi ?timeout t fan args = t.exec_multi ?timeout fan args
let close t = t.close ()
let primary_connection t = t.primary ()
let connection_for_slot t slot = t.connection_for_slot slot
let endpoint_for_slot t slot = t.endpoint_for_slot slot
