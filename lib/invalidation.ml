(* Parser for RESP3 server-assisted client-side-caching invalidation
   pushes. See docs/client-side-caching.md and
   https://valkey.io/topics/client-side-caching/.

   Wire shape:
     > 2
     $10 invalidate
     *N $<len><key>  …    (invalidate these N keys)
   or
     > 2
     $10 invalidate
     _                    (null → flush ALL cached keys for this client;
                           sent on FLUSHDB / FLUSHALL on the server)

   This module is pure — it translates a parsed [Resp3.t] into an
   [Invalidation.t option]. The wire bytes have already been
   consumed by [Resp3_parser] upstream and handed to us as a typed
   value (via the push stream on [Connection.t]). *)

type t =
  | Keys of string list
  | Flush_all

(* Accept only bulk-string keys. Anything else is a spec-violating
   reply and we drop the whole push rather than partially evict —
   the invalidator fiber logs and continues. *)
let rec keys_of_array = function
  | [] -> Some []
  | Resp3.Bulk_string k :: rest ->
      (match keys_of_array rest with
       | None -> None
       | Some ks -> Some (k :: ks))
  | _ -> None

let of_push (v : Resp3.t) : t option =
  match v with
  | Resp3.Push [ Resp3.Bulk_string "invalidate"; Resp3.Null ] ->
      Some Flush_all
  | Resp3.Push [ Resp3.Bulk_string "invalidate"; Resp3.Array keys ] ->
      (match keys_of_array keys with
       | Some ks -> Some (Keys ks)
       | None -> None)
  | _ -> None

(* Apply an invalidation to the CSC state. Two mutations happen
   per key (or all keys for Flush_all):
     1. Flip the in-flight entry's dirty flag (if any). An
        in-flight fetch for this key is already stale; the owner
        must not cache what comes back.
     2. Evict the current cache entry.
   Order matters: mark_dirty first, then evict. Reversing would
   let a GET complete between the two and write a stale entry
   after the evict — same bug, reordered.
   Pulling this out of the fiber body keeps the effect
   unit-testable without an Eio runtime. *)
let apply cache inflight = function
  | Flush_all ->
      (* Flush-all means every key is now stale. Order matters for
         the same reason as the Keys branch: mark every in-flight
         fetch dirty FIRST so completions after this point don't
         cache now-stale values, then clear the cache itself. *)
      Inflight.mark_all_dirty inflight;
      Cache.clear cache
  | Keys ks ->
      List.iter
        (fun k ->
          Inflight.mark_dirty inflight k;
          Cache.evict cache k)
        ks
