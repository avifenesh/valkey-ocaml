module S = Valkey.Slot

(* Reference values taken from the Valkey / Redis test suite. *)

let test_crc16 () =
  Alcotest.(check int) "empty" 0 (S.crc16_xmodem "");
  Alcotest.(check int) "123456789" 0x31C3 (S.crc16_xmodem "123456789");
  Alcotest.(check int) "A" 0x58E5 (S.crc16_xmodem "A")

let test_of_key_plain () =
  (* Well-known slots from Redis cluster docs and test suites. *)
  Alcotest.(check int) "foo" 12182 (S.of_key "foo");
  Alcotest.(check int) "bar" 5061 (S.of_key "bar");
  Alcotest.(check int) "baz" 4813 (S.of_key "baz")

let test_of_key_with_tag () =
  (* When a hashtag {tag} is present, only the tag is hashed. Two keys
     sharing a tag land on the same slot. *)
  let s1 = S.of_key "{same}.foo" in
  let s2 = S.of_key "{same}.bar" in
  Alcotest.(check int) "tagged keys same slot" s1 s2;
  (* The hashed tag should equal [S.of_key "same"]. *)
  Alcotest.(check int) "tag = plain of tag content"
    (S.of_key "same") s1

let test_tag_extraction () =
  Alcotest.(check string) "no tag" "plain" (S.tag_of_key "plain");
  Alcotest.(check string) "tag mid-key" "x" (S.tag_of_key "aa{x}bb");
  Alcotest.(check string) "empty braces => whole key"
    "aa{}bb" (S.tag_of_key "aa{}bb");
  Alcotest.(check string) "lone open => whole key"
    "aa{bb" (S.tag_of_key "aa{bb");
  Alcotest.(check string) "close before open => whole key"
    "a}b{c" (S.tag_of_key "a}b{c");
  Alcotest.(check string) "first tag wins"
    "x" (S.tag_of_key "{x}{y}z")

let test_slot_range () =
  let within_range s = s >= 0 && s < S.slot_count in
  for i = 0 to 999 do
    let k = Printf.sprintf "k-%d" i in
    Alcotest.(check bool)
      (Printf.sprintf "slot in range for %S" k)
      true (within_range (S.of_key k))
  done

let tests =
  [ Alcotest.test_case "CRC16-XMODEM reference values" `Quick test_crc16;
    Alcotest.test_case "of_key (no tag)" `Quick test_of_key_plain;
    Alcotest.test_case "of_key (with hashtag)" `Quick test_of_key_with_tag;
    Alcotest.test_case "tag_of_key edge cases" `Quick test_tag_extraction;
    Alcotest.test_case "slot always in [0, 16384)" `Quick test_slot_range;
  ]
