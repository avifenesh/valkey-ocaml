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
    refresh_interval : float;     (** Base interval (s) between periodic
                                      topology refreshes. *)
    refresh_jitter : float;       (** Additional random wait [0, jitter] s. *)
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
  (Router.t, string) result

val from_pool_and_topology :
  ?max_redirects:int ->
  clock:_ Eio.Time.clock ->
  pool:Node_pool.t ->
  topology:Topology.t ->
  unit ->
  Router.t
(** Wrap an existing pool + topology as a Router. Used internally for
    the standalone-as-one-shard-cluster code path, where the pool
    already contains the single connection and the topology is
    synthetic. *)

module For_testing : sig
  (** Not part of the public API. Exposed so the retry state machine
      can be driven from unit tests without standing up a real pool. *)

  val handle_retries :
    pool:Node_pool.t ->
    topology_ref:Topology.t ref ->
    clock:_ Eio.Time.clock ->
    max_redirects:int ->
    trigger_refresh:(unit -> unit) ->
    ?sync_ref:(unit -> unit) ->
    ?timeout:float ->
    dispatch:(unit -> (Resp3.t, Connection.Error.t) result) ->
    string array ->
    (Resp3.t, Connection.Error.t) result

  val clusterdown_backoff_for_attempt : int -> float
  val tryagain_backoff : float
  val conn_lost_backoff : float
end
