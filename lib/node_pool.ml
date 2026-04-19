type t = {
  conns : (string, Connection.t) Hashtbl.t;
  mutex : Eio.Mutex.t;
}

let create () =
  { conns = Hashtbl.create 16;
    mutex = Eio.Mutex.create () }

let add t id conn =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      Hashtbl.replace t.conns id conn)

let get t id =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      Hashtbl.find_opt t.conns id)

let remove t id =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      match Hashtbl.find_opt t.conns id with
      | None -> None
      | Some c ->
          Hashtbl.remove t.conns id;
          Some c)

let node_ids t =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      Hashtbl.fold (fun k _ acc -> k :: acc) t.conns [])

let connections t =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      Hashtbl.fold (fun _ v acc -> v :: acc) t.conns [])

let close_all t =
  let to_close =
    Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
        let l = Hashtbl.fold (fun _ v acc -> v :: acc) t.conns [] in
        Hashtbl.clear t.conns;
        l)
  in
  List.iter
    (fun c -> try Connection.close c with _ -> ())
    to_close

let size t =
  Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
      Hashtbl.length t.conns)
