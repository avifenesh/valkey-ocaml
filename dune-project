(lang dune 3.16)

(name valkey)

(generate_opam_files true)

(source
 (github avifenesh/ocaml-valkey))

(authors "Avi Fenesh <aviarchi1994@gmail.com>")

(maintainers "Avi Fenesh <aviarchi1994@gmail.com>")

(license MIT)

(documentation "https://github.com/avifenesh/ocaml-valkey")

(package
 (name valkey)
 (version 0.3.1)
 (synopsis "Modern Valkey client for OCaml 5 + Eio (RESP3-only)")
 (description
  "Eio-native Valkey client targeting OCaml 5.3+ and Valkey 7.2+ (including Valkey 8.1 / 9 additions: SET IFEQ, DELIFEQ, hash field TTL). RESP3 only; no Lwt/Async/legacy fallback. Connection layer has auto-reconnect, byte-budget backpressure, circuit breaker, keepalive, TLS, and optional domain split so the parser can't stall socket I/O. Cluster router with quorum-based topology discovery, MOVED/ASK/CLUSTERDOWN/TRYAGAIN retry, periodic refresh, and Read_from-aware replica routing (incl. AZ affinity).")
 (tags ("valkey" "redis" "eio" "client" "resp3" "cluster"))
 (depends
  (ocaml (>= 5.3.0))
  (eio (>= 1.3))
  (eio_main (>= 1.3))
  (cstruct (>= 6.2.0))
  (tls (>= 2.0.0))
  (tls-eio (>= 2.0.0))
  (x509 (>= 1.0.0))
  (ca-certs (>= 1.0.0))
  (domain-name (>= 0.4.0))
  (mirage-crypto-rng (>= 2.0.0))
  (digestif (>= 1.2.0))
  (opentelemetry (>= 0.90))
  (alcotest (and :with-test (>= 1.7.0)))
  (bisect_ppx (and :with-dev-setup (>= 2.8.0)))))
