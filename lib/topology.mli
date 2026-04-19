(** Valkey cluster topology: typed view of [CLUSTER SHARDS] + a slot map
    plus a SHA for change detection across refreshes. *)

module Node : sig
  type role = Primary | Replica

  type health = Online | Fail | Loading

  type t = {
    id : string;
    endpoint : string option;       (** Preferred dial target reported by the
                                        server: ["?"], empty, or a real value. *)
    ip : string option;             (** Server's IP. *)
    hostname : string option;       (** Announced hostname (if any). *)
    port : int option;              (** Plain TCP port. *)
    tls_port : int option;          (** TLS port. *)
    role : role;
    health : health;
    replication_offset : int64;
    availability_zone : string option;  (** Valkey 9.1+. *)
  }

  val role_to_string : role -> string
  val health_to_string : health -> string
end

module Shard : sig
  type slot_range = { start_ : int; end_ : int }   (** inclusive *)

  type t = {
    id : string option;              (** 40-char shard id (Valkey 9.1+). *)
    slots : slot_range list;
    primary : Node.t;
    replicas : Node.t list;
  }
end

type t

val of_cluster_shards : Resp3.t -> (t, string) result
(** Parse the RESP3 reply of [CLUSTER SHARDS]. Returns a human-readable
    error on malformed input. *)

val shards : t -> Shard.t list

val node_for_slot : t -> int -> Node.t
(** The primary that owns [slot]. Raises [Invalid_argument] if [slot] is
    out of range or unowned (e.g. during resharding). *)

val shard_for_slot : t -> int -> Shard.t option

val primaries : t -> Node.t list

val replicas : t -> Node.t list

val all_nodes : t -> Node.t list

val serialize : t -> string
(** Deterministic canonical form (sorted, fixed field order) used for
    change detection. *)

val sha : t -> string
(** SHA1 of [serialize]. Comparing SHAs of two topologies reliably
    detects change. *)
