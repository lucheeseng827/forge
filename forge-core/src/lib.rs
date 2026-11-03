//! forge-core — the brain of the batch-inference coordinator.
//!
//! forge is a **single homogeneous fan-out of independent items across
//! interchangeable workers**. Arbitrary task graphs with inter-task dependencies
//! are [dagron](https://github.com/lucheeseng827/dagron)'s job, NOT forge's. The boundary is
//! enforced *in the type system*: there is no `depends_on`, no successor edge, no
//! fan-in, and no topological sort anywhere in this crate. If a change here starts
//! to look like a scheduler, it is the wrong change.
//!
//! This crate owns the domain types ([`Item`], [`ItemResult`], [`WorkerSpec`], …),
//! the three-state item machine ([`ItemState`]), the [`Queue`] / [`Worker`] /
//! [`ResultStore`] traits, the retry/backoff policy ([`RetryPolicy`]), and the
//! embeddable fan-out loop ([`BatchRun`]). It has **no I/O backend dependency** —
//! the concrete SQLite queue, HTTP worker, and JSONL store live in sibling crates
//! and are driven exclusively through the traits.

// Public async traits are used only through generic bounds (never `dyn`), so the
// `async fn` desugaring is fine; silence the forward-compat lint deliberately.
#![allow(async_fn_in_trait)]

pub mod build;
mod cost;
mod error;
mod failure;
pub mod obs;
mod retry;
mod run;
mod state;
mod traits;
mod types;
mod validate;

pub use build::{identity as build_identity, init_logging, LogFormat};
pub use cost::{compute_cost, CostInputs, CostReport, UsageTotals};
pub use error::ForgeError;
pub use failure::{classify_failure, FailureKind};
pub use obs::{
    accept_request_id, current_request_id, internal_error_body, mint_request_id, scope_request_id,
};
pub use retry::RetryPolicy;
pub use run::{runtime, BatchRun, RunConfig, MAX_BLOCKING_THREADS};
pub use state::ItemState;
pub use traits::{Queue, QueueCounts, ResultStore, Worker};
pub use types::{
    valid_item_url, EndpointKind, EngineHint, Item, ItemError, ItemResponse, ItemResult, Job,
    JobStatus, JobTotals, Shard, TokenUsage, WorkerSpec, MAX_ITEM_URL_LEN,
};
pub use validate::ResponseCheck;

/// Wall-clock epoch milliseconds. Used for `leased_until` / `completed_at`.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
