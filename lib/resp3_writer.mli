(** RESP3 command serialiser.

    Two shapes are provided:

    - [command_to_string] builds a fresh string. Convenient for
      tests and handshake wiring where the command is tiny.
    - [command_to_cstruct] allocates one [Cstruct.t] of the exact
      final size and fills it via a single copy of each argument.
      Used on the hot path (per-request) to avoid the extra copy
      that [Buffer.contents] incurs on large values. *)

val write_command : Buffer.t -> string array -> unit

val command_to_string : string array -> string

val command_to_cstruct : string array -> Cstruct.t
(** One allocation of the exact wire size; each argument is
    [blit]ed in once. No intermediate buffer / string. *)
