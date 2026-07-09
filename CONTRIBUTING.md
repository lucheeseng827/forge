# Contributing to forge

Thanks for helping. Two rules keep contributions easy to accept:

## 1. Sign your work (DCO)

Contributions are accepted under the
[Developer Certificate of Origin](https://developercertificate.org/). Add a
`Signed-off-by` line to every commit (`git commit -s`):

```
Signed-off-by: Your Name <you@example.com>
```

Inbound = outbound: your contributions are licensed under the same
[Apache-2.0](./LICENSE) license as the project.

## 2. Run the gate before you push

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --locked
```

CI runs the same checks; a PR that fails them will not be reviewed.

## Design doctrine (read before adding a feature)

forge is a **single homogeneous fan-out of independent items across
interchangeable workers**. It deliberately has **no** task graph, **no**
`depends_on`/fan-in, **no** cron or triggers, **no** workflow topology, and
**no** fleet provisioning — and the CI doctrine guard enforces it. If your
feature needs inter-item edges, it belongs in a workflow engine
(e.g. [dagron](https://github.com/lucheeseng827/dagron)), not here. Heavy
optional dependencies (object stores, Parquet, rate limiting) go behind
off-by-default features so the lean static binary stays lean — CI asserts
their absence from the default build.

## Reporting bugs / proposing features

Open a GitHub issue with reproduction steps (for bugs: the exact command, the
input shape, and the observed vs expected behavior). For security issues, see
[SECURITY.md](./SECURITY.md) — do not open a public issue.
