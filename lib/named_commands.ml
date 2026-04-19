type entry =
  | Cmd of {
      template : string array;
      target : Router.Target.t option;
      read_from : Router.Read_from.t option;
    }
  | Tx of {
      commands : string array list;
      hint_key_placeholder : int;
    }

type t = {
  client : Client.t;
  table : (string, entry) Hashtbl.t;
  mutex : Mutex.t;
}

let create client =
  { client; table = Hashtbl.create 16; mutex = Mutex.create () }

let with_mutex t f =
  Mutex.lock t.mutex;
  let r = try Ok (f ()) with e -> Error e in
  Mutex.unlock t.mutex;
  match r with Ok v -> v | Error e -> raise e

let register_command t ~name ~template ?target ?read_from () =
  with_mutex t (fun () ->
      Hashtbl.replace t.table name (Cmd { template; target; read_from }))

let register_transaction
    t ~name ~commands ?(hint_key_placeholder = 1) () =
  with_mutex t (fun () ->
      Hashtbl.replace t.table name
        (Tx { commands; hint_key_placeholder }))

let unregister t name =
  with_mutex t (fun () -> Hashtbl.remove t.table name)

let has t name =
  with_mutex t (fun () -> Hashtbl.mem t.table name)

let lookup t name =
  with_mutex t (fun () -> Hashtbl.find_opt t.table name)

(* A token is a placeholder iff it is exactly "$N" for some decimal
   N >= 1. "$1foo" is literal. *)
let placeholder_index tok =
  let len = String.length tok in
  if len < 2 || tok.[0] <> '$' then None
  else
    let rest = String.sub tok 1 (len - 1) in
    match int_of_string_opt rest with
    | Some n when n >= 1 -> Some n
    | _ -> None

let substitute ~args template =
  let args_arr = Array.of_list args in
  let n_args = Array.length args_arr in
  Array.map
    (fun tok ->
      match placeholder_index tok with
      | Some i when i <= n_args -> args_arr.(i - 1)
      | Some _ | None -> tok)
    template

let terminal msg = Error (Connection.Error.Terminal msg)

let run_command ?timeout t ~name ~args =
  match lookup t name with
  | None ->
      terminal (Printf.sprintf "Named_commands.run_command: %S not registered" name)
  | Some (Tx _) ->
      terminal
        (Printf.sprintf
           "Named_commands.run_command: %S is a transaction; \
            use run_transaction" name)
  | Some (Cmd { template; target; read_from }) ->
      let cmd = substitute ~args template in
      Client.custom ?timeout ?target ?read_from t.client cmd

let arg_at ~args i =
  let arr = Array.of_list args in
  if i >= 1 && i <= Array.length arr then Some arr.(i - 1)
  else None

let run_transaction ?timeout:_ t ~name ~args =
  match lookup t name with
  | None ->
      terminal
        (Printf.sprintf "Named_commands.run_transaction: %S not registered"
           name)
  | Some (Cmd _) ->
      terminal
        (Printf.sprintf
           "Named_commands.run_transaction: %S is a single command; \
            use run_command" name)
  | Some (Tx { commands; hint_key_placeholder }) ->
      let hint_key = arg_at ~args hint_key_placeholder in
      let queue_result : (unit, Connection.Error.t) result ref =
        ref (Ok ())
      in
      let outcome =
        Transaction.with_transaction ?hint_key t.client (fun tx ->
            List.iter
              (fun tmpl ->
                let cmd = substitute ~args tmpl in
                match !queue_result with
                | Error _ -> ()   (* stop queueing after first error *)
                | Ok () ->
                    (match Transaction.queue tx cmd with
                     | Ok () -> ()
                     | Error e -> queue_result := Error e))
              commands)
      in
      match !queue_result with
      | Error e -> Error e
      | Ok () -> outcome
