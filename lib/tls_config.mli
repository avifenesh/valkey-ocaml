(** TLS configuration for a Valkey connection. *)

type t

val insecure : unit -> t
(** No certificate verification. Testing only. *)

val with_ca_cert : ?server_name:string -> ca_pem:string -> unit -> t
(** Verify server certificate against the given CA PEM content (not file
    path; caller reads the file). [server_name] supplies SNI and peer
    hostname verification. *)

val with_system_cas : ?server_name:string -> unit -> (t, string) result
(** Verify using the host's system CA bundle (via [ca-certs]). Use this for
    managed services like AWS ElastiCache / MemoryDB or any Valkey server
    with a cert signed by a public CA. Returns an error if the OS CA bundle
    cannot be located. *)

(**/**)

(** Internal — used by [Connection] to construct the TLS client config. *)
val authenticator : t -> X509.Authenticator.t

val server_name : t -> [ `host ] Domain_name.t option
