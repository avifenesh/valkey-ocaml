(** TLS configuration for a Valkey connection. *)

type t

val insecure : unit -> t
(** No certificate verification. Testing only. *)

val with_ca_cert : ?server_name:string -> ca_pem:string -> unit -> t
(** Verify server certificate against the given CA PEM content (not file
    path; caller reads the file). [server_name] supplies SNI and peer
    hostname verification. *)

(**/**)

(** Internal — used by [Connection] to construct the TLS client config. *)
val authenticator : t -> X509.Authenticator.t

val server_name : t -> [ `host ] Domain_name.t option
