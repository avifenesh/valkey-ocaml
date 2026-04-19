(** Quorum-based topology discovery.

    Queries a set of nodes for [CLUSTER SHARDS], groups the responses by
    canonical SHA, and selects a winner only when the agreement is
    strong enough. Defends against acting on a stale or isolated view. *)

type result =
  | Agreed of Topology.t
      (** A topology won by quorum (or fell through to fallback — see
          [Agreed_fallback]). Use it. *)
  | Agreed_fallback of Topology.t
      (** No quorum achieved but retries are exhausted. This is the first
          successfully-parsed topology from a responsive node. Use it with
          caution — flag for re-discovery soon. *)
  | No_agreement
      (** Either all queries failed, or multiple topologies tied and the
          caller should retry. *)

val select :
  agreement_ratio:float ->
  min_nodes_for_quorum:int ->
  queried:int ->
  views:(Topology.t option) list ->
  result
(** Pure quorum decision.

    - [agreement_ratio]: winner must have ≥ this fraction of [queried]
      (typical: 0.2).
    - [min_nodes_for_quorum]: if [queried] is below this, skip quorum
      and accept any successful view as [Agreed].
    - [views] has one entry per queried node: [Some t] on successful
      parse, [None] on error / timeout. *)
