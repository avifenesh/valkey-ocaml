exception End_of_stream

type t = {
  stream : Cstruct.t Eio.Stream.t;
  mutable buf : Bytes.t;
  mutable off : int;
  mutable len : int;
  mutable closed : bool;
}

let create ?(initial_size = 4096) stream =
  { stream; buf = Bytes.create initial_size; off = 0; len = 0; closed = false }

let close t = t.closed <- true

let grow_or_compact t extra =
  let needed = t.len + extra in
  if t.off + needed <= Bytes.length t.buf then ()
  else if needed <= Bytes.length t.buf then (
    Bytes.blit t.buf t.off t.buf 0 t.len;
    t.off <- 0)
  else (
    let new_cap = max (Bytes.length t.buf * 2) needed in
    let new_buf = Bytes.create new_cap in
    Bytes.blit t.buf t.off new_buf 0 t.len;
    t.buf <- new_buf;
    t.off <- 0)

let pull_one_chunk t =
  if t.closed && Eio.Stream.length t.stream = 0 then raise End_of_stream;
  let chunk =
    match Eio.Stream.take_nonblocking t.stream with
    | Some c -> c
    | None ->
        if t.closed then raise End_of_stream
        else Eio.Stream.take t.stream
  in
  let clen = Cstruct.length chunk in
  if clen = 0 then raise End_of_stream;
  grow_or_compact t clen;
  Cstruct.blit_to_bytes chunk 0 t.buf (t.off + t.len) clen;
  t.len <- t.len + clen

let ensure t n =
  while t.len < n do pull_one_chunk t done

let any_char t =
  ensure t 1;
  let c = Bytes.get t.buf t.off in
  t.off <- t.off + 1;
  t.len <- t.len - 1;
  c

let take t n =
  ensure t n;
  let s = Bytes.sub_string t.buf t.off n in
  t.off <- t.off + n;
  t.len <- t.len - n;
  s

let scan_crlf t =
  let rec loop i =
    if i >= t.len - 1 then -1
    else if Bytes.get t.buf (t.off + i) = '\r'
         && Bytes.get t.buf (t.off + i + 1) = '\n'
    then i
    else loop (i + 1)
  in
  loop 0

let line t =
  let rec loop () =
    match scan_crlf t with
    | i when i >= 0 ->
        let s = Bytes.sub_string t.buf t.off i in
        t.off <- t.off + i + 2;
        t.len <- t.len - i - 2;
        s
    | _ ->
        pull_one_chunk t;
        loop ()
  in
  loop ()

let to_byte_source t : Resp3_parser.byte_source =
  { any_char = (fun () -> any_char t);
    line = (fun () -> line t);
    take = (fun n -> take t n); }
