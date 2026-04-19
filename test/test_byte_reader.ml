module B = Valkey.Byte_reader
module P = Valkey.Resp3_parser
module R = Valkey.Resp3

let cs_of_string s = Cstruct.of_string s

let make_reader chunks =
  let stream = Eio.Stream.create (List.length chunks + 1) in
  List.iter (fun s -> Eio.Stream.add stream (cs_of_string s)) chunks;
  let r = B.create stream in
  B.close r;
  r

let test_simple () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "hello\r\nworld\r\n" ] in
  let src = B.to_byte_source r in
  Alcotest.(check string) "first line" "hello" (src.line ());
  Alcotest.(check string) "second line" "world" (src.line ())

let test_split_chunks () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "hel"; "lo\r\nwor"; "ld\r\n" ] in
  let src = B.to_byte_source r in
  Alcotest.(check string) "line across 2 chunks" "hello" (src.line ());
  Alcotest.(check string) "line across another split" "world" (src.line ())

let test_take_across_chunks () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "ab"; "cd"; "ef" ] in
  let src = B.to_byte_source r in
  Alcotest.(check string) "5 bytes across 3 chunks" "abcde" (src.take 5);
  Alcotest.(check char) "last char" 'f' (src.any_char ())

let test_any_char () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "xyz" ] in
  let src = B.to_byte_source r in
  Alcotest.(check char) "x" 'x' (src.any_char ());
  Alcotest.(check char) "y" 'y' (src.any_char ());
  Alcotest.(check char) "z" 'z' (src.any_char ())

let test_parser_via_byte_reader () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "$5\r\nhel"; "lo\r\n" ] in
  let src = B.to_byte_source r in
  let v = P.read src in
  Alcotest.check (Alcotest.testable R.pp R.equal)
    "bulk string split across chunks"
    (R.Bulk_string "hello") v

let test_end_of_stream () =
  Eio_main.run @@ fun _env ->
  let r = make_reader [ "abc" ] in
  let src = B.to_byte_source r in
  let _ = src.take 3 in
  Alcotest.check_raises "eof raises"
    B.End_of_stream
    (fun () -> let _ = src.any_char () in ())

let tests =
  [ Alcotest.test_case "simple" `Quick test_simple;
    Alcotest.test_case "line across chunks" `Quick test_split_chunks;
    Alcotest.test_case "take across chunks" `Quick test_take_across_chunks;
    Alcotest.test_case "any_char sequence" `Quick test_any_char;
    Alcotest.test_case "parser over byte_reader" `Quick
      test_parser_via_byte_reader;
    Alcotest.test_case "end of stream" `Quick test_end_of_stream;
  ]
