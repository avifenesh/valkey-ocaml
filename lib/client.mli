(** High-level Valkey client. Standalone in v1; cluster router lands later
    without changing this surface. *)

module Read_from : sig
  type t =
    | Primary
    | Prefer_replica
    | Az_affinity of { az : string }
    | Az_affinity_replicas_and_primary of { az : string }

  val default : t
  (** [Primary] — all reads to primary. *)
end

module Target : sig
  type t =
    | Random
    | All_nodes
    | All_primaries
    | By_slot of int
    | By_node of string
    | By_channel of string
end

module Config : sig
  type t = {
    connection : Connection.Config.t;
    client_az : string option;
    read_from : Read_from.t;
  }
  val default : t
end

type t

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?config:Config.t ->
  host:string ->
  port:int ->
  unit ->
  t

val close : t -> unit

val raw_connection : t -> Connection.t
(** Escape hatch for observability / advanced use. Do not send on this
    directly under normal use — go through [exec] or the typed commands. *)

val exec :
  ?timeout:float ->
  ?target:Target.t ->
  ?read_from:Read_from.t ->
  t ->
  string array ->
  (Resp3.t, Connection.Error.t) result
(** Send a raw RESP command. Routing hints are accepted but ignored in
    standalone mode — they matter once a cluster router is wired. *)

(** {1 Typed commands (Tier 1 — being filled in progressively)} *)

val get :
  ?timeout:float ->
  ?read_from:Read_from.t ->
  t -> string -> (string option, Connection.Error.t) result

val set :
  ?timeout:float ->
  ?ex_seconds:float ->
  ?nx:bool ->
  ?xx:bool ->
  t -> string -> string -> (bool, Connection.Error.t) result
(** [true] on success; [false] when [NX]/[XX] condition prevented the set. *)

val del :
  ?timeout:float ->
  t -> string list -> (int, Connection.Error.t) result
(** Returns the number of keys that were removed. *)
