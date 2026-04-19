let slot_count = 16384

let crc16_xmodem s =
  let crc = ref 0 in
  for i = 0 to String.length s - 1 do
    let byte = Char.code (String.unsafe_get s i) in
    crc := !crc lxor (byte lsl 8);
    for _ = 0 to 7 do
      if !crc land 0x8000 <> 0 then
        crc := ((!crc lsl 1) lxor 0x1021) land 0xffff
      else
        crc := (!crc lsl 1) land 0xffff
    done
  done;
  !crc

let tag_of_key key =
  match String.index_opt key '{' with
  | None -> key
  | Some i ->
      (match String.index_from_opt key (i + 1) '}' with
       | Some j when j > i + 1 -> String.sub key (i + 1) (j - i - 1)
       | _ -> key)

let of_key key = crc16_xmodem (tag_of_key key) mod slot_count
