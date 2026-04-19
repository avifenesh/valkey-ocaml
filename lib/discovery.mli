(** Quorum-based topology discovery.

    Queries a set of nodes for [CLUSTER SHARDS], groups the responses by
    canonical SHA, and selects a winner only when the agreement is
    strong enough. Defends against acting on a stale or isolated view. *)

type selection =
  | Agreed of Topology.t
      (** A topology won by quorum. Use it. *)
  | Agreed_fallback of Topology.t
      (** No quorum but some node answered. Use with suspicion; schedule
          a re-discovery soon. *)
  | No_agreement
      (** All queries failed, or views tied and the caller should retry. *)

val select :
  agreement_ratio:float ->
  min_nodes_for_quorum:int ->
  queried:int ->
  views:(Topology.t option) list ->
  selection
(** Pure quorum decision.

    - [agreement_ratio]: winner must have ≥ this fraction of [queried]
      (typical: 0.2).
    - [min_nodes_for_quorum]: if [queried] is below this, skip quorum
      and accept any successful view as [Agreed].
    - [views] has one entry per queried node: [Some t] on successful
      parse, [None] on error / timeout. *)

val discover_from_seeds :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  clock:_ Eio.Time.clock ->
  ?domain_mgr:_ Eio.Domain_manager.t ->
  ?connection_config:Connection.Config.t ->
  ?agreement_ratio:float ->
  ?min_nodes_for_quorum:int ->
  seeds:(string * int) list ->
  unit ->
  (Topology.t, string) result
(** Open a transient [Connection.t] to each seed in parallel, run
    [CLUSTER SHARDS], parse to [Topology.t], and apply [select].

    All transient connections are closed before return. The selected
    topology is what the caller should feed to the cluster router.

    Defaults: [agreement_ratio = 0.2], [min_nodes_for_quorum = 3]. *)
