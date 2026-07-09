# forge quickstart — see the loop work with **zero GPUs**

forge is BYO-endpoints: it drives engines you already run. To try it without a GPU,
this folder ships a tiny **mock** OpenAI-compatible engine (Python stdlib only, no
pip installs) so you can watch the whole fan-out → checkpoint → resume loop end to
end. The marquee "hundreds of spot GPUs" moment lives one layer down (your provisioner /
fleet); this shows the *coordinator* doing its job.

## 1. Build forge

```sh
cargo build -p forge-cli            # produces target/debug/forge
alias forge=target/debug/forge
```

## 2. Start the mock engine

```sh
python3 examples/mock_engine.py &   # a fake vLLM on http://127.0.0.1:8000
```

It answers `GET /health`, `POST /v1/chat/completions`, `/v1/completions`, and
`/v1/embeddings`, each with a canned body and a real `usage` object (so token
accounting works). Any request mentioning `FORCE_500` is failed on purpose — that
is the `poison-1` line in `prompts.jsonl`, there to show the dead-letter path.

## 3. Run a batch

```sh
forge run \
  --input examples/prompts.jsonl \
  --workers http://127.0.0.1:8000 \
  --out results.jsonl \
  --checkpoint .forge/state.db \
  --max-attempts 3
```

You'll get:

- `results.jsonl` — one line per successful `custom_id` (order-independent).
- `results.jsonl.dead.jsonl` — the `poison-1` item, after retries are exhausted.
- `.forge/state.db` — the durable checkpoint (the queue *is* the resume state).

## 3b. Guard output *quality* with `--require` (the result-validation hook)

A 2xx response can still be garbage — an empty generation, or prose when you asked
for JSON. forge can treat that as a **soft failure**: retry it (re-sampling may fix
it) and, if it still fails, dead-letter it with a clear reason **instead of silently
writing bad data**. No closed Batch API does this.

```sh
# Require the model's output text to be valid JSON (the structured-output guard):
forge run --input examples/prompts.jsonl --workers http://127.0.0.1:8000 \
  --out results.jsonl --checkpoint .forge/state.db --max-attempts 2 --require json
```

With the bundled mock, the prompts that *don't* ask for JSON (req-001/002/003) emit
prose, so `--require json` quarantines them to `results.jsonl.dead.jsonl` with
`validation failed: output is not valid JSON`; the embedding (req-004) and the
JSON-asking prompt (req-005) pass. Modes: `--require any` (default, no check),
`nonempty` (output must be non-blank / embedding vector present), `json` (output
text must parse as JSON; embeddings degrade to `nonempty`). Validation is applied
per *item* endpoint, so a mixed chat+embeddings JSONL is checked correctly.

## 4. Inspect status (with the retry/observability metrics)

```sh
forge status --checkpoint .forge/state.db
# pending=0 leased=0 done=5 dead=1 total=6 success_rate=83.3% retried=0 failures[server_error=1]

forge status --checkpoint .forge/state.db --json
# {"pending":0,"leased":0,"done":5,"dead":1,"total":6,"success_rate":0.8333333333333334,
#  "retried":0,"attempts_histogram":{"1":6},"failure_reasons":{"server_error":1}}
```

(`retried` counts items that needed more than one *lease* — the poison item's three
HTTP attempts all happen inside one lease, in the worker's own retry loop, so it
dead-letters with lease-attempt 1. A lease is retaken only after a crash or an
expired visibility timeout.)

## 5. Prove kill-and-resume (the headline guarantee)

Kill the run partway (Ctrl-C, or `kill -9` the process) and re-run:

```sh
forge resume --checkpoint .forge/state.db --workers http://127.0.0.1:8000
```

`resume` reopens the checkpoint, expires stale leases, skips already-emitted
`custom_id`s, and finishes with **zero duplicate and zero missing** results. On a
clean run it's an idempotent no-op.

## 6. Try a malformed input (the parse-error sidecar)

Append a broken line and re-run `forge run` with a fresh checkpoint:

```sh
echo '{ this is not valid json' >> examples/prompts.jsonl
```

The bad line is counted, skipped (never aborts the job), and preserved for repair in
`results.jsonl.reject.jsonl` as `{"line_no","error","input"}`.

---

When you're ready for real hardware, swap the mock URL for your vLLM/SGLang/llama.cpp
endpoints (comma-separated) and set `--concurrency` to each engine's
`--max-num-seqs` / `--max-running-requests` / `--parallel`. Nothing else changes.
