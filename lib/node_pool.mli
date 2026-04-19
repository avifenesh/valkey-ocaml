(** Pool of per-node [Connection.t]s keyed by node id. The connection
    manager's essence applied to a multi-node world: every node has its
    own recovery machinery, and the pool coordinates adding / removing
    nodes as topology evolves.

    Thread-safe across domains (internally guarded by a mutex). *)

type t

val create : unit -> t

val add : t -> string -> Connection.t -> unit
(** Associate a [Connection.t] with [node_id]. If an entry already
    exists it is replaced (caller is responsible for closing the old one). *)

val get : t -> string -> Connection.t option

val remove : t -> string -> Connection.t option
(** Removes and returns the entry, so the caller can close it. *)

val node_ids : t -> string list

val connections : t -> Connection.t list

val close_all : t -> unit
(** Closes every Connection in the pool and clears the table. *)

val size : t -> int
