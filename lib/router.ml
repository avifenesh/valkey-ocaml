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

type pair_fn =
  ?timeout:float -> Target.t ->
  string array -> string array ->
  ( (Resp3.t, Connection.Error.t) result
    * (Resp3.t, Connection.Error.t) result
  , Connection.Error.t) result

type connection_for_slot_fn = int -> Connection.t option
type endpoint_for_slot_fn = int -> (string * string * int) option

(* Per-slot mutex used to serialize atomic operations (MULTI/EXEC
   blocks from [Batch ~atomic:true] and [Transaction]) on the
   shared primary connection for that slot. Non-atomic traffic
   bypasses the mutex and continues to multiplex normally. See
   docs/batch.md for the underlying reason. *)
type atomic_lock_for_slot_fn = int -> Eio.Mutex.t

type t = {
  exec : exec_fn;
  exec_multi : exec_multi_fn;
  pair : pair_fn;
  close : unit -> unit;
  primary : unit -> Connection.t option;
  connection_for_slot : connection_for_slot_fn;
  endpoint_for_slot : endpoint_for_slot_fn;
  is_standalone : bool;
  atomic_lock_for_slot : atomic_lock_for_slot_fn;
}
[@@warning "-69"]

let make ~exec ~exec_multi ~pair ~close ~primary ~connection_for_slot
    ~endpoint_for_slot ~is_standalone ~atomic_lock_for_slot =
  { exec; exec_multi; pair; close; primary;
    connection_for_slot; endpoint_for_slot;
    is_standalone;
    atomic_lock_for_slot }

let standalone (conn : Connection.t) : t =
  let exec ?timeout _target _read_from args =
    Connection.request ?timeout conn args
  in
  let exec_multi ?timeout _fan_target args =
    [ Topology.standalone_node_id, Connection.request ?timeout conn args ]
  in
  let pair ?timeout _target args1 args2 =
    Connection.request_pair ?timeout conn args1 args2
  in
  (* One connection → one mutex shared by every slot. *)
  let atomic_mutex = Eio.Mutex.create () in
  { exec; exec_multi; pair;
    close = (fun () -> Connection.close conn);
    primary = (fun () -> Some conn);
    connection_for_slot = (fun _ -> Some conn);
    endpoint_for_slot = (fun _ -> None);
    is_standalone = true;
    atomic_lock_for_slot = (fun _ -> atomic_mutex);
  }

let exec ?timeout t target rf args = t.exec ?timeout target rf args
let exec_multi ?timeout t fan args = t.exec_multi ?timeout fan args
let pair ?timeout t target args1 args2 =
  t.pair ?timeout target args1 args2
let close t = t.close ()
let primary_connection t = t.primary ()
let connection_for_slot t slot = t.connection_for_slot slot
let endpoint_for_slot t slot = t.endpoint_for_slot slot
let is_standalone t = t.is_standalone
let atomic_lock_for_slot t slot = t.atomic_lock_for_slot slot
