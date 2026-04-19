# Contributing to ocaml-valkey

Thanks for the interest. This library is small, opinionated, and
modern — we try to keep the surface tight and the internals clean.
Here's everything you need to land a change.

## Build & test

Requires OCaml ≥ 5.3, dune ≥ 3.16, and Docker (for integration
tests). All development happens inside an opam switch:

```bash
opam switch create 5.3.0 --yes
eval $(opam env)
opam install . --deps-only --with-test --with-dev-setup --yes
```

Standalone tests only need a single Valkey:

```bash
docker compose up -d
dune build && dune runtest
```

Cluster integration tests need the 6-node compose cluster:

```bash
sudo bash scripts/cluster-hosts-setup.sh   # once per machine
docker compose -f docker-compose.cluster.yml up -d
dune build && dune runtest --force
```

Filter to a subset during iteration:

```bash
dune exec test/run_tests.exe -- test 'bitmap'
dune exec test/run_tests.exe -- test 'retry state machine'
```

## Fuzzers

Two separate fuzzers; both should pass strict with no unexpected
exceptions.

- **RESP3 parser** — pure logic, ~140k inputs/s:

  ```bash
  dune exec bin/fuzz_parser/fuzz_parser.exe -- --iterations 1000000 --strict
  ```

- **Stability** — end-to-end with real connections + optional
  docker-restart chaos. Use before opening any PR that touches
  `connection.ml`, `cluster_router.ml`, or the send/receive path:

  ```bash
  dune exec bin/fuzz/fuzz.exe -- --seconds 60 --workers 16 --max-errors 0
  ```

## Bench

Local bench against the standalone server (`--host`/`--port`) or
the cluster (`--seeds host:port,...`). Full matrix is ~5 min:

```bash
dune exec bin/bench/bench.exe -- --ops 50000
```

On PRs, CI runs a 5000-op trim and posts a delta table vs. `main`;
scenarios regressing more than 10 % fail the build.

## Coverage

```bash
dune runtest --instrument-with bisect_ppx --force
bisect-ppx-report summary
bisect-ppx-report html -o coverage-html --source-path lib
xdg-open coverage-html/index.html
```

The CI floor is 60 %; the goal is to ratchet up as cluster paths
get integration coverage. Don't lower the floor in a PR — raise
it or hold it.

## Pre-push gate

`scripts/install-git-hooks.sh` installs a pre-push hook that runs
build + tests + parser fuzz (100k strict) + 30 s standalone and
30 s cluster stability fuzz. Zero-error threshold. Strongly
recommended for anyone touching the wire code.

## Style

- **Documentation-first per command.** Before writing OCaml for a
  new Valkey command, fetch the current `valkey.io/commands/<name>/`
  page, quote the syntax, every argument, and every documented
  reply type in the `.mli` doc comment. Two real type-design bugs
  in Phase 1 were caught only by this discipline — see
  `AUDIT.md` and `feedback_docs_first_per_command` in the project
  memory.
- **Typed over raw.** Commands return semantically-typed values
  (`ping_reply`, `hello`, `client_tracking`, …), not raw `Resp3.t`.
  A raw escape hatch (`Client.custom`) exists for power users.
- **Domain-typed errors.** Command errors surface as
  `valkey_error` records with a `code` field; callers match on
  `code = "WRONGTYPE"`, not on pattern-matching a raw `Error`
  variant nested inside `Resp3.t`.
- **Abstract connections.** The `Connection.t` type is opaque;
  internals are free to change. Don't leak concrete representations
  into the public API.
- **Connection-setup errors are exceptions, not `result`.** TCP
  refused / `HELLO` rejected / AUTH wrong = exception during
  `connect`. Per-command failures = `result`.
- **No `ignore (_ : _ result)`.** Either handle the error or
  promote it. `AUDIT.md` tracks every silent `with _ -> ()` site
  and its justification; new ones need one too.

## Commit messages

Short imperative subject, blank line, longer body explaining the
**why** not the **what**. One logical change per commit. Reference
`AUDIT.md` items or `ROADMAP.md` phases when applicable.

Example:

```
Phase 2.4: internal audit + first fixes

AUDIT.md records every Obj.magic / try _ with _ -> () / mutable /
Atomic.* site with its disposition.

Fixes this commit:
  - cluster_pubsub: delete dead watchdog function (contained an
    Obj.magic landmine; correct version is inlined in create).
  - connection.close, cluster_router.close: guard with
    Atomic.exchange so repeat calls are true no-ops.
```

## PR checklist

Before opening a PR:

- [ ] `dune build` clean, no warnings-as-errors.
- [ ] `dune runtest` green against both standalone and cluster.
- [ ] If you touched the wire code: 60 s stability fuzz passes with
      `--max-errors 0`.
- [ ] If you touched the parser: 1 M strict parser-fuzz iterations
      clean.
- [ ] If you touched a Valkey command: the `.mli` doc comment
      quotes the valkey.io syntax.
- [ ] `CHANGELOG.md` updated under `## [Unreleased]`.
- [ ] If you added a public symbol: it has a doc comment.

## Reporting bugs

Open an issue with:
- Valkey server version + deployment mode (standalone / cluster).
- OCaml version + Eio version.
- Minimal reproducer (`dune exec --profile=dev ...`).
- Expected vs observed.

For parser bugs, the nightly fuzzer's shrinker prints a minimal
reproducer in the failing run's log artifact; paste that directly.
