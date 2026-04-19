let crlf = "\r\n"

let write_bulk_string buf s =
  Buffer.add_char buf '$';
  Buffer.add_string buf (string_of_int (String.length s));
  Buffer.add_string buf crlf;
  Buffer.add_string buf s;
  Buffer.add_string buf crlf

let write_command buf args =
  Buffer.add_char buf '*';
  Buffer.add_string buf (string_of_int (Array.length args));
  Buffer.add_string buf crlf;
  Array.iter (write_bulk_string buf) args

let command_to_string args =
  let buf = Buffer.create 64 in
  write_command buf args;
  Buffer.contents buf

(* Exact encoded size of [args] in RESP3 array-of-bulk-strings form:
     *N\r\n  ( $L\r\n <payload> \r\n )^N
   L includes the decimal string of each bulk-string length. *)
let encoded_size args =
  let n = Array.length args in
  (* *N\r\n *)
  let header_size =
    1 + String.length (string_of_int n) + 2
  in
  let per_arg s =
    (* $L\r\n + payload + \r\n *)
    1 + String.length (string_of_int (String.length s)) + 2
    + String.length s + 2
  in
  Array.fold_left (fun acc s -> acc + per_arg s) header_size args

let command_to_cstruct args =
  let total = encoded_size args in
  let cs = Cstruct.create_unsafe total in
  let pos = ref 0 in
  let put_char c =
    Cstruct.set_char cs !pos c;
    incr pos
  in
  let put_string s =
    let len = String.length s in
    Cstruct.blit_from_string s 0 cs !pos len;
    pos := !pos + len
  in
  put_char '*';
  put_string (string_of_int (Array.length args));
  put_string crlf;
  Array.iter
    (fun arg ->
      put_char '$';
      put_string (string_of_int (String.length arg));
      put_string crlf;
      put_string arg;
      put_string crlf)
    args;
  (* Sanity: we sized it exactly; this should never fire. *)
  assert (!pos = total);
  cs
