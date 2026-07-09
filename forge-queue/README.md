# forge-queue — the durable, single-writer lease queue

`forge-queue` is forge's embedded, crash-durable work queue: the concrete backend
behind the [`forge_core::Queue`](../forge-core) seam. It gives the fan-out loop
single-writer lease/visibility-timeout semantics, an expired-lease reaper, and a
dead-letter transition — the entire spot-survival mechanism on the coordinator
side, with **no heartbeat**. There is exactly one writer, which is what makes the
lease transaction safe without row locks.

## What it does

- **`SqliteQueue`** (default `sqlite` feature) — a `rusqlite`-WAL backend. `open`
  builds the WAL schema; `enqueue` / `lease` / `ack` / `reap` / `dead_letter` /
  `counts` run their SQLite work on `tokio::task::spawn_blocking` over a single
  `Arc<Mutex<_>>` connection.
- **`RedbQueue`** (feature `redb`) — a pure-Rust, zero-C alternative (items table +
  a FIFO pending index + a leased index for the reaper + maintained counters) for
  the cleanest static cross-compile. `sled` is deliberately avoided.
- **Lease as a single-writer transaction** — `lease` is an `UPDATE … RETURNING`
  emulating `SELECT … FOR UPDATE SKIP LOCKED`: it flips `Pending`/expired-`Leased`
  rows to `Leased`, stamps `leased_until`, and bumps `attempts`.
- **Lease fencing** — `ack` and `dead_letter` apply only while the row still carries
  the leasing attempt, so a stale worker whose lease already expired cannot close
  or poison a row that was re-leased to a newer attempt.
- **Reaper** — `reap` moves every `Leased` row past its `leased_until` back to
  `Pending`, re-queuing work dropped by a dead/spot-killed worker.

Both backends implement the same `Queue` contract; the default `sqlite` build is
the mature primary, `redb` the zero-C option (`--no-default-features --features redb`).

## Quickstart

`SqliteQueue::open` is the queue seam every forge entry point wires into `BatchRun`
(and the CLI hydrates via `forge_shard::ingest_jsonl_with_rejects`):

```rust
use forge_queue::SqliteQueue;

let queue = SqliteQueue::open("ckpt.db")?;   // durable checkpoint / resume state
// hand `queue` to forge_core::BatchRun::new(queue, workers, store) …
let counts = queue.counts().await?;          // pending / leased / done / dead
```

The same `ckpt.db` a run writes is what `forge resume` / `forge sweep` reopen.
