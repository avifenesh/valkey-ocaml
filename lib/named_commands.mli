(** User-registered commands and transactions.

    Register a template once, execute by name later with different
    argument values. Two flavours:

    - {b Single command}. A template like [[| "HGET"; "$1"; "$2" |]]
      is stored; [run_command t ~name ~args:["user:42"; "email"]]
      substitutes [$1 -> "user:42"], [$2 -> "email"] and dispatches
      via {!Client.custom}. Optional explicit [target] /
      [read_from] pin the routing; otherwise [Command_spec] picks
      the default from the command name (after substitution).

    - {b Transaction}. A sequence of command templates wrapped in
      [MULTI ... EXEC]. The same placeholder substitution applies
      to every command in the set. One of the substituted arguments
      is picked via [hint_key_placeholder] to decide which slot the
      transaction pins to in cluster mode.

    Placeholders: a token is treated as a placeholder iff it is
    exactly [$N] for some decimal integer N >= 1. Any other use of
    [$] is a literal character — [$1foo] is the literal string
    [$1foo]. To get a literal token [$1], register [[| "..."; "$$1" |]]
    and {!run_command} substitutes [$$1] -> [$1] as a single
    pass-through (not implemented — just register the literal). *)

type t

val create : Client.t -> t

val register_command :
  t ->
  name:string ->
  template:string array ->
  ?target:Router.Target.t ->
  ?read_from:Router.Read_from.t ->
  unit ->
  unit

val register_transaction :
  t ->
  name:string ->
  commands:string array list ->
  ?hint_key_placeholder:int ->
  unit ->
  unit
(** [hint_key_placeholder] defaults to [1] (first substituted
    argument). Irrelevant in standalone; used in cluster mode to
    pin the transaction's connection to the right primary. *)

val unregister : t -> string -> unit

val has : t -> string -> bool

val run_command :
  ?timeout:float ->
  t ->
  name:string ->
  args:string list ->
  (Resp3.t, Connection.Error.t) result
(** Errors with [Terminal] if [name] is not registered or is
    registered as a transaction (use {!run_transaction}). *)

val run_transaction :
  ?timeout:float ->
  t ->
  name:string ->
  args:string list ->
  (Resp3.t list option, Connection.Error.t) result
(** Returns [Ok (Some replies)] on successful EXEC, [Ok None] on
    WATCH abort (if the registered transaction includes a WATCH),
    [Error _] otherwise.

    Errors with [Terminal] if [name] is not registered or is a
    single-command entry (use {!run_command}). *)
