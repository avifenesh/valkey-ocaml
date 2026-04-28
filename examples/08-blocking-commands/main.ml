(* Blocking commands.

   BLPOP / BRPOP let a worker wait on a list with a timeout. The
   worker fiber is suspended until either:
     - someone LPUSHes onto the watched list
     - the block_seconds timeout fires (server-side)
     - the per-call ?timeout fires (client-side, raises Timeout)

   This client routes blocking commands through
   [Client.Config.blocking_pool] so one Client.t can serve both
   blocking workers and regular traffic concurrently — the pool
   leases a dedicated connection for each blocking call while
   the multiplexed conn keeps serving every other fiber. Opt
   in with [blocking_pool.max_per_node >= 1]; the default is
   disabled and blocking commands return
   [Error (Pool Pool_not_configured)]. *)

module C = Valkey.Client
module BP = Valkey.Blocking_pool
module E = Valkey.Connection.Error

let queue = "demo:blocking"

let producer ~client ~clock =
  for i = 0 to 4 do
    Eio.Time.sleep clock 0.5;
    let job = Printf.sprintf "job-%d" i in
    (match C.lpush client queue [ job ] with
     | Ok n -> Printf.printf "[producer] pushed %s (queue len now %d)\n%!" job n
     | Error e -> Format.eprintf "LPUSH: %a@." E.pp e)
  done

let worker ~client =
  let rec loop seen =
    if seen >= 5 then begin
      Printf.printf "[worker] processed 5 jobs, exiting\n%!";
    end
    else
      match
        C.brpop client ~keys:[ queue ] ~block_seconds:1.0
      with
      | Ok (Some (_q, job)) ->
          Printf.printf "[worker] got %s\n%!" job;
          loop (seen + 1)
      | Ok None ->
          Printf.printf "[worker] BRPOP server-side timeout, retrying\n%!";
          loop seen
      | Error e ->
          Format.eprintf "[worker] BRPOP: %a@." C.pp_blocking_error e
  in
  loop 0

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config = {
    C.Config.default with
    blocking_pool = {
      BP.Config.default with
      (* One blocking connection is enough for this demo; raise
         if you run many concurrent workers. *)
      max_per_node = 2;
      on_exhaustion = `Block;
      borrow_timeout = Some 5.0;
    };
  } in
  let client =
    C.connect ~sw ~net ~clock ~config ~host:"localhost" ~port:6379 ()
  in
  let _ = C.del client [ queue ] in
  Eio.Fiber.both
    (fun () -> producer ~client ~clock)
    (fun () -> worker ~client);
  C.close client
