//! forge-queue — the durable, single-writer lease queue.
//!
//! Primary backend: **`rusqlite`-WAL** ([`SqliteQueue`]). A pure-Rust **`redb`**
//! backend lives behind the `redb` feature for a zero-C static binary. `sled` is
//! deliberately avoided (prolonged beta).
//!
//! ## Status
//!
//! Both backends are **implemented** (ROADMAP P1.2) behind the same [`Queue`] trait.
//! [`SqliteQueue`] (default `sqlite` feature): `open` builds the WAL schema, and
//! `enqueue` / `lease` / `ack` / `reap` / `dead_letter` / `counts` run their SQLite
//! work on `tokio::task::spawn_blocking` over the single `Arc<Mutex<_>>` connection —
//! the lease is a single-writer `UPDATE … RETURNING` emulating `SELECT … FOR UPDATE
//! SKIP LOCKED`. [`RedbQueue`] (feature `redb`): the pure-Rust, zero-C alternative
//! for the cleanest static cross-compile — same contract over redb tables (items +
//! a FIFO pending index + a leased index for the reaper + maintained counters). The
//! default `sqlite` build is the mature primary; `redb` is the zero-C option.

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteQueue;

#[cfg(feature = "redb")]
mod redb_backend;
#[cfg(feature = "redb")]
pub use redb_backend::RedbQueue;
