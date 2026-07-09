# forge-store — the result sink, keyed by `custom_id`

`forge-store` is the **output side** of the forge batch-inference stack: an
order-independent, idempotent sink for completed `ItemResult`s. It implements
`forge_core::ResultStore` so the coordinator can write results and dead-letters
without caring about ordering or retries. The default is a local append-only JSONL file
plus a sibling dead-letter JSONL; an `object_store` (S3/GCS/Azure) backend and a
columnar Parquet writer sit behind off-by-default features.

## What it does

- **`JsonlStore`** — an append-only local `ResultStore`. Results go to `out_path`,
  dead-letters to the sibling `<out_path>.dead.jsonl`. Exposes `put`, `dead_letter`,
  and `emitted_ids`.
- **Idempotent, dedup-on-resume writes** — an in-memory emitted-id set (seeded by
  scanning both existing files on first use) makes a re-run after an expired lease a
  no-op, turning at-least-once execution into an exactly-once *effect*. A `custom_id`
  is terminal exactly once — as a success **or** a dead-letter, never both — so one set
  guards both sinks.
- **Crash-safe append** — before opening a sink it checks whether the file ends
  mid-line (a SIGKILL'd torn write) and terminates that fragment first, so a new record
  can never be glued onto a half-written one. The fragment stays as its own unparseable,
  skipped line.
- **`sum_usage`** — streams a results JSONL and folds each line's `usage` into a
  `UsageTotals` (the input to the cost-arbitrage report, `forge cost`). Missing file =
  empty total; never holds the file in RAM.
- **`object_store` backend** (feature) — `ObjStore` plus `objstore_from_out` /
  `objstore_open_run`, the same `ResultStore` trait over S3/GCS/Azure.
- **`parquet` output** (feature) — `ParquetStore`, `jsonl_to_parquet`, `forge_schema`,
  `ParquetOpts` for columnar results.

## Quickstart

```rust
use forge_store::JsonlStore;
use forge_core::ResultStore;

// Results append to results.jsonl; failures to results.jsonl.dead.jsonl.
let store = JsonlStore::new("results.jsonl");

store.put(&item_result).await?;          // success (idempotent on custom_id)
store.dead_letter(&failed_result).await?; // terminal failure

// Roll captured token usage up for the cost report.
let totals = forge_store::sum_usage("results.jsonl").await?;
```

The coordinator holds the store behind the `ResultStore` trait, so swapping in the
`object_store` or `parquet` backend needs no call-site changes.

## Config

Cargo features (all off by default, so the lean `forge-cli` musl binary never compiles
the cloud/columnar SDKs — CI asserts their absence):

| Feature | Effect |
|---------|--------|
| `object_store` | Enables the `ObjStore` S3/GCS/Azure backend (pulls `object_store`, `url`, `bytes`, `futures-util`). |
| `parquet` | Enables the `ParquetStore` columnar writer (pulls `parquet`, `arrow-array`, `arrow-schema`). |
