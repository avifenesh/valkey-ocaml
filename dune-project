(lang dune 3.16)

(name valkey)

(generate_opam_files true)

(source
 (github avifenesh/valkey-ocaml))

(authors "Avi Fenesh")

(maintainers "Avi Fenesh")

(license MIT)

(documentation "https://github.com/avifenesh/valkey-ocaml")

(package
 (name valkey)
 (synopsis "Modern Valkey client for OCaml 5 + Eio (RESP3-only)")
 (description
  "Eio-native Valkey client targeting OCaml 5.3+ and Valkey 7.2+ (including Valkey 8.1 / 9 additions: SET IFEQ, DELIFEQ, hash field TTL). RESP3 only; no Lwt/Async/legacy fallback. Connection layer has auto-reconnect, byte-budget backpressure, circuit breaker, keepalive, TLS, and optional domain split so the parser can't stall socket I/O.")
 (depends
  (ocaml (>= 5.3.0))
  (dune (>= 3.16))
  (eio (>= 1.3))
  (eio_main (>= 1.3))
  cstruct
  tls
  tls-eio
  x509
  ca-certs
  domain-name
  mirage-crypto-rng
  digestif
  (alcotest :with-test)
  (bisect_ppx (and :with-dev-setup (>= 2.8.0)))))
