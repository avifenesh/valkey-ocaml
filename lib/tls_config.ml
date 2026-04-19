let () = Mirage_crypto_rng_unix.use_default ()

type t = {
  authenticator : X509.Authenticator.t;
  server_name : [ `host ] Domain_name.t option;
}

let null_auth ?ip:_ ~host:_ _ = Ok None

let insecure () =
  { authenticator = null_auth; server_name = None }

let parse_host s =
  match Domain_name.of_string s with
  | Error _ -> None
  | Ok d -> (match Domain_name.host d with Ok h -> Some h | Error _ -> None)

let with_ca_cert ?server_name ~ca_pem () =
  let cas =
    match X509.Certificate.decode_pem_multiple ca_pem with
    | Ok certs -> certs
    | Error (`Msg m) -> invalid_arg ("Tls_config.with_ca_cert: " ^ m)
  in
  let auth = X509.Authenticator.chain_of_trust ~time:(fun () -> None) cas in
  let sn = Option.bind server_name parse_host in
  { authenticator = auth; server_name = sn }

let with_system_cas ?server_name () =
  match Ca_certs.authenticator () with
  | Error (`Msg m) -> Error m
  | Ok auth ->
      let sn = Option.bind server_name parse_host in
      Ok { authenticator = auth; server_name = sn }

let authenticator t = t.authenticator

let server_name t = t.server_name
