# forge-shard — streaming JSONL ingest

`forge-shard` is the **input side** of the forge batch-inference stack. It reads a
(potentially 50M-line) request file **line by line**, parses each into an `Item`
keyed by `custom_id`, validates the id, and hydrates a `Queue` in batches — the file
never enters RAM whole. It also carries the **off-ramp** from the closed Batch APIs
(`import_batch`) and two bounded-memory preprocessing/QA passes (prefix bucketing and
completeness verification).

## What it does

- **Streaming ingest + hydrate** — `ingest_jsonl` / `ingest_jsonl_with_rejects`
  stream the file, `parse_line` each into a `forge_core::Item`, and `enqueue` in
  batches of `HYDRATE_BATCH` (1000). Idempotent: the queue dedups on `custom_id`, so
  a re-run inserts nothing new. Returns `IngestStats { lines_read, hydrated, rejected }`.
- **Seek-resume via a byte-offset index** — after each committed batch it records a
  `forge_core::Shard` (byte range + line range). A re-run reads `Queue::hydrated_through`
  and **seeks** to the first un-hydrated offset instead of rescanning. A checkpoint past
  the current EOF (input replaced/truncated shorter) falls back to a full rescan.
- **Parse-error dead-letter sidecar** — rejected lines (malformed JSON or invalid
  `custom_id`) are counted and skipped rather than aborting the job, and optionally
  preserved to a sidecar as `{"line_no","error","input"}` records for repair. The file
  is created only when the first reject appears.
- **`custom_id` validation** — `valid_custom_id` adopts Anthropic's charset
  `^[a-zA-Z0-9_-]{1,64}$` verbatim, so a file from a closed batch API ingests unchanged.
- **Batch-API import (`import_batch`)** — normalizes an OpenAI / Anthropic / Bedrock
  batch JSONL into clean forge-native `{custom_id, method, url, body}` (sniffing
  `custom_id`/`recordId`, `body`/`params`/`modelInput`, explicit-or-inferred `url`),
  dropping duplicate ids to the reject sidecar. Returns `ImportStats`.
- **Prefix bucketing (`bucket::bucket_jsonl`)** — a single-pass, bounded-RAM reorder
  that groups lines by `(model, system-prompt prefix)` so the downstream fan-out keeps
  an engine's automatic prefix cache hot. Only `buckets` temp writers are open at once.
- **Completeness verify (`verify::verify_completeness`)** — an exact external
  sort-merge set-difference (not a Bloom filter) that confirms every input `custom_id`
  has a terminal result-or-dead-letter entry, reporting `missing`/`extra`/`duplicate`
  in `O(run_capacity)` RAM. Report-only: it never re-queues.

## Quickstart

```rust
use forge_shard::{ingest_jsonl_with_rejects, import_batch};
use forge_core::EndpointKind;

// Optional off-ramp: normalize a vendor batch file into forge-native JSONL.
import_batch("openai-batch.jsonl", "forge.jsonl",
             Some("forge.jsonl.reject.jsonl"), EndpointKind::Chat).await?;

// Stream the input into any `Queue` (e.g. forge-queue's SqliteQueue), preserving
// rejected lines for repair. Re-running after an interruption seeks past the
// already-hydrated prefix.
let stats = ingest_jsonl_with_rejects(&queue, "forge.jsonl",
                                       Some("forge.jsonl.reject.jsonl")).await?;
println!("read {} hydrated {} rejected {}",
         stats.lines_read, stats.hydrated, stats.rejected);
```

`Queue` is provided by `forge-core` / `forge-queue`; forge-shard only fills it.
