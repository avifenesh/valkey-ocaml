(** Per-command default routing.

    A cluster-aware client needs to know, for each command, where it
    belongs: which argument is the key (so we can compute its slot),
    whether it is safe to serve from a replica, and whether it must
    fan out to every primary. This module is the single source of
    truth for those facts.

    The table covers the commands this library exposes as typed
    wrappers. Unknown commands receive a conservative default
    ([Keyless_random]); callers who know better can bypass the
    default by passing an explicit target to [Client.exec]. *)

type t =
  | Single_key of { key_index : int; readonly : bool }
      (** Classic one-key command. Slot is computed from
          [args.(key_index)]. *)
  | Multi_key of { first_key_index : int; readonly : bool }
      (** Multi-key command where the caller is expected to have
          co-located the keys via hashtags. Slot is computed from the
          first key; any MOVED that results means the hashtag
          assumption was violated. *)
  | Keyless_random of { readonly : bool }
      (** No key (PING, TIME, CLIENT ID, …). Any node will do. *)
  | Fan_primaries
      (** Must hit every primary (FLUSHALL, SCRIPT LOAD, WAIT).
          Dispatch via [Router.exec_multi], not [exec]. *)
  | Fan_all_nodes
      (** Must hit every node (CLIENT LIST aggregation, CLUSTER NODES).
          Dispatch via [Router.exec_multi], not [exec]. *)

val lookup : string array -> t
(** Resolve the default spec for a command. Inspects [args.(0)] and,
    for two-word commands (SCRIPT LOAD, XINFO STREAM, CONFIG SET),
    [args.(1)] as well. Case-insensitive.

    Returns [Keyless_random { readonly = false }] for anything not in
    the table. *)

val readonly : t -> bool
(** Whether a command is safe to serve from a replica. Writes
    ([readonly = false]) must be forced to the primary regardless of
    the user's [Read_from] preference. *)

val target_and_rf :
  Router.Read_from.t ->
  string array ->
  (Router.Target.t * Router.Read_from.t) option
(** High-level convenience: return [Some (target, effective_read_from)]
    for a command dispatchable through [Router.exec], or [None] if the
    command should go through [Router.exec_multi] (fan-out).

    - Writes override the caller's [Read_from] to [Primary].
    - Single-key commands map to [By_slot (slot_of key)].
    - Unknown / keyless commands map to [Random] with the caller's
      [Read_from] preserved. *)

val command_count : unit -> int
(** Number of commands + sub-commands in the routing table. Useful
    for debug / sanity logs. *)
