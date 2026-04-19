(** Valkey cluster slot computation.

    Slots are the mapping unit between keys and nodes in a Valkey cluster.
    The cluster has exactly [slot_count] slots; each slot is owned by one
    primary (which may have zero or more replicas).

    A key's slot is:
    - If the key contains a non-empty substring [{tag}], only the tag is
      hashed — lets callers force two keys into the same slot for
      multi-key commands.
    - Otherwise, the whole key is hashed.

    The hash is CRC16-XMODEM mod [slot_count]. *)

val slot_count : int
(** 16384. *)

val crc16_xmodem : string -> int
(** Raw CRC16-XMODEM (0x1021 polynomial, initial 0, no reflection).
    Exposed for tests; most callers want [of_key]. *)

val of_key : string -> int
(** The slot that a given key belongs to. Handles hashtag extraction. *)

val tag_of_key : string -> string
(** The substring actually hashed for slot computation. Equal to [key]
    when no hashtag is present. *)
