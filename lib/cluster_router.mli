(** Cluster-aware [Router.t] implementation.

    Discovers topology from [seeds] via quorum, opens a [Connection.t]
    per node, and returns a [Client.Router.t] that dispatches by slot
    with [Read_from] honoured. MOVED/ASK retry and periodic refresh
    land in follow-up commits. *)

module Config : sig
  type t = {
    seeds : (string * int) list;
    connection : Connection.Config.t;
    agreement_ratio : float;
    min_nodes_for_quorum : int;
    max_redirects : int;          (** MOVED/ASK retries per command *)
    prefer_hostname : bool;       (** Use [Node.hostname] when set. *)
  }
  val default : seeds:(string * int) list -> t
end

val create :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  config:Config.t ->
  unit ->
  (Client.Router.t, string) result
