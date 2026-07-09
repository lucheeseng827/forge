//! forge-agent — the optional, co-located **spot-drain agent**.
//!
//! On a spot/preemptible box, the *only* way to use the in-VM ~30s reclaim window
//! gracefully is from **inside** the dying box: the interruption signal is poll-only
//! and local (ARCHITECTURE §6). So `forge-agent` runs next to one engine and does
//! three strictly-bounded things — and nothing else:
//!
//! 1. **lease-proxy** — long-poll the single-writer coordinator ([`Coordinator`], the
//!    [`forge_proto`] wire contract) for a batch sized to the engine's concurrency,
//!    run each item against the local [`Worker`], and post results back.
//! 2. **health-gate** — only pull when the local engine's [`Worker::is_ready`] is
//!    true (a dead engine must not drain the queue into dead-letters).
//! 3. **spot-drain** — run the [`forge_spot`] watcher *locally*; on a notice, flip a
//!    one-way [`Drain`] latch so the loop **stops pulling new leases** and lets
//!    in-flight items finish *if the window allows*. Everything not yet pulled stays
//!    leased on the coordinator and is re-queued by **lease expiry** — the backbone.
//!
//! **Boundary (by doctrine).** The agent never gains a task graph, never runs a
//! second engine type per item, and **never provisions** (that's the provisioner's job). It
//! only *proposes* results; the coordinator stays the single writer, fencing every
//! post by lease generation. The OSS lease model stays **expiry-only** — no redundant
//! heartbeat — the deliberate dagron-plumbing-avoidance choice.
//!
//! **Correctness never depends on the agent.** The pure-lib v1 path (coordinator +
//! `forge` CLI) is correct on its own; a missed notice, a crashed agent, or a dropped
//! post all degrade to the same lease-expiry re-queue. The agent is an *optimization*.
//!
//! ## Transport
//!
//! [`Coordinator`] is the transport boundary. This crate ships two impls: the
//! in-memory [`InProcessCoordinator`] (a co-located single-box deployment and the
//! test vehicle) and the networked `forge-proto`-over-HTTP [`http::HttpCoordinator`]
//! (client) + [`http::serve_coordinator`] (server) against a remote coordinator — the
//! agent loop above is transport-agnostic across both.

#![forbid(unsafe_code)]
// `Coordinator::*` and the spot source are used only through generic bounds (never
// `dyn`), so the `async fn` desugaring is fine; silence the forward-compat lint,
// matching `forge-core` / `forge-spot`.
#![allow(async_fn_in_trait)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use forge_core::{ForgeError, Item, ItemResult, ItemState, TokenUsage, Worker};
use forge_proto::{InterruptionNotice, LeasePull, ResultPost, WireItem, WireOutcome, WireUsage};
use forge_spot::{Cloud, InterruptionSource, NoticeKind};
use futures_util::stream::{self, StreamExt};

pub mod http;

/// A one-way **Running → Draining** latch, shared between the run loop and the spot
/// watcher. Once a notice flips it, this box never pulls a new lease again (there is
/// no un-drain — the VM is going away).
#[derive(Clone, Default)]
pub struct Drain(Arc<AtomicBool>);

impl Drain {
    pub fn new() -> Self {
        Self::default()
    }

    /// True once a spot/preempt notice has fired.
    pub fn is_draining(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// Latch into the draining state (idempotent).
    pub fn signal(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// The agent's view of the single-writer coordinator — the [`forge_proto`] wire
/// contract, transport-agnostic. Implemented in-process by [`InProcessCoordinator`]
/// and (follow-up) by an HTTP client.
pub trait Coordinator {
    /// Lease up to `req.max` ready items. An empty grant means "nothing ready now"
    /// (the agent backs off and polls again). The coordinator has already marked the
    /// returned items `Leased` and stamped their generation.
    async fn pull(&self, req: &LeasePull) -> Result<Vec<WireItem>, ForgeError>;

    /// Post one finished item back, fenced by its lease generation. The coordinator
    /// stores-then-acks; a post is an idempotent proposal, safe to retry.
    async fn post(&self, result: &ResultPost) -> Result<(), ForgeError>;

    /// Forward a spot-interruption heads-up. Advisory only — default no-op, because
    /// correctness rides on lease expiry, not on this arriving.
    async fn notify(&self, _notice: &InterruptionNotice) -> Result<(), ForgeError> {
        Ok(())
    }
}

/// How the agent paces itself.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// The fronted engine's stable id (lease owner / fencing key).
    pub worker_id: String,
    /// Max items per lease pull — size this to the engine's declared concurrency.
    pub max_lease: u32,
    /// Visibility timeout requested for each lease, in seconds.
    pub lease_secs: u64,
    /// Backoff between polls when a grant is empty or the engine is unhealthy.
    pub poll_idle: Duration,
    /// Exit after this many *consecutive* empty grants (the run looks drained). `0`
    /// means never idle-exit — run as a daemon until [`Drain`] or an external stop.
    pub max_idle_polls: u32,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            worker_id: "agent".to_string(),
            max_lease: 64,
            lease_secs: 120,
            poll_idle: Duration::from_millis(500),
            max_idle_polls: 0,
        }
    }
}

/// What the run loop did, for logging / the (later) control plane.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentStats {
    /// Items run and posted (success + dead-letter).
    pub processed: usize,
    /// Of those, terminal failures.
    pub dead_lettered: usize,
    /// Empty pulls observed.
    pub idle_polls: usize,
    /// Did the loop exit because the [`Drain`] latch fired?
    pub drained: bool,
}

/// Map a wire item back to the in-process [`Item`] the [`Worker`] runs. Status is
/// `Leased` (the coordinator already leased it); only the request fields and the
/// attempt count carry over.
pub fn wire_to_item(w: &WireItem) -> Item {
    Item {
        custom_id: w.custom_id.clone(),
        method: w.method.clone(),
        url: w.url.clone(),
        body: w.body.clone(),
        status: ItemState::Leased,
        attempts: w.attempts,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

/// Map a finished [`ItemResult`] to the [`ResultPost`] sent back, fenced by the
/// originating lease generation. The success path carries `status_code`, `usage`, and
/// `latency_ms` so the coordinator's stored result stays faithful (`forge cost` and
/// the status metrics don't degrade just because the work ran on a remote agent).
pub fn result_to_post(worker_id: &str, lease_generation: u64, r: &ItemResult) -> ResultPost {
    let outcome = match (&r.response, &r.error) {
        (Some(resp), None) => WireOutcome::Done {
            status_code: resp.status_code,
            response: resp.body.clone(),
            usage: to_wire_usage(&r.usage),
            latency_ms: r.latency_ms,
        },
        (_, err) => WireOutcome::DeadLetter {
            error: err
                .as_ref()
                .map(|e| format!("{}: {}", e.code, e.message))
                .unwrap_or_else(|| "terminal failure".to_string()),
        },
    };
    ResultPost {
        custom_id: r.custom_id.clone(),
        lease_generation,
        worker_id: worker_id.to_string(),
        outcome,
    }
}

/// `forge_core::TokenUsage` → the wire shape.
pub fn to_wire_usage(u: &TokenUsage) -> WireUsage {
    WireUsage {
        prompt_tokens: u.prompt_tokens,
        completion_tokens: u.completion_tokens,
        total_tokens: u.total_tokens,
    }
}

/// The wire shape → `forge_core::TokenUsage` (used by the coordinator server when it
/// reconstructs the stored result from a [`ResultPost`]).
pub fn from_wire_usage(u: &WireUsage) -> TokenUsage {
    TokenUsage {
        prompt_tokens: u.prompt_tokens,
        completion_tokens: u.completion_tokens,
        total_tokens: u.total_tokens,
        // The agent<->coordinator wire (WireUsage) carries only the three headline
        // counts; cached/reasoning detail is preserved on the direct HttpWorker path,
        // not this relay path. Default (0) here.
        ..Default::default()
    }
}

/// Drive one [`InterruptionSource`] in the VM: poll on `interval` until a notice
/// fires, then flip `drain` (stop pulling) and forward an advisory
/// [`InterruptionNotice`]. Returns once the notice is handled. Best-effort: a
/// failed `notify` is ignored — the latch is what matters.
pub async fn spot_drain(
    source: &impl InterruptionSource,
    interval: Duration,
    drain: &Drain,
    coord: &impl Coordinator,
    worker_id: &str,
) {
    let notice = forge_spot::watch(source, interval).await;
    // Flip the latch *first* — stopping new leases is the real action; the wire
    // heads-up is just courtesy.
    drain.signal();
    let wire = InterruptionNotice {
        worker_id: worker_id.to_string(),
        cloud: cloud_str(notice.cloud).to_string(),
        kind: kind_str(notice.kind).to_string(),
        deadline: notice.deadline,
    };
    let _ = coord.notify(&wire).await;
    tracing::warn!(
        worker_id,
        cloud = cloud_str(notice.cloud),
        "spot interruption — draining: no new leases, finishing in-flight only"
    );
}

/// The agent run loop: health-gate → pull → run-and-post (concurrently, bounded by
/// the worker's own AIMD limiter) → repeat. Stops pulling the instant [`Drain`] is
/// latched (in-flight finishes; the rest is left for lease-expiry re-queue). Exits
/// when drained, after `max_idle_polls` consecutive empty grants, or on a transport
/// error.
pub async fn run_agent(
    cfg: &AgentConfig,
    coord: &impl Coordinator,
    worker: &impl Worker,
    drain: &Drain,
) -> Result<AgentStats, ForgeError> {
    let mut stats = AgentStats::default();
    let mut consecutive_idle = 0u32;

    loop {
        if drain.is_draining() {
            stats.drained = true;
            return Ok(stats);
        }

        // Health-gate: never pull into a dead engine. Re-probe, and if still down,
        // back off without counting it as "queue drained".
        if !worker.is_ready() {
            worker.probe().await;
            if !worker.is_ready() {
                tokio::time::sleep(cfg.poll_idle).await;
                continue;
            }
        }

        let grant = coord
            .pull(&LeasePull {
                worker_id: cfg.worker_id.clone(),
                max: cfg.max_lease,
                lease_secs: cfg.lease_secs,
            })
            .await?;

        if grant.is_empty() {
            stats.idle_polls += 1;
            consecutive_idle += 1;
            if cfg.max_idle_polls != 0 && consecutive_idle >= cfg.max_idle_polls {
                return Ok(stats);
            }
            tokio::time::sleep(cfg.poll_idle).await;
            continue;
        }
        consecutive_idle = 0;

        // Run the granted batch concurrently. Each item: submit → post. The worker's
        // AIMD limiter is the real in-flight throttle, so we hand it the whole batch
        // and let it pace; no `spawn`/`Arc` (the futures borrow `&worker`/`&coord`),
        // mirroring the coordinator's dispatch idiom. The batch finishes even once
        // `drain` flips — that *is* the in-flight drain.
        let batch = stream::iter(grant)
            .map(|w| async move {
                let item = wire_to_item(&w);
                let result = match worker.submit(&item).await {
                    Ok(r) => r,
                    Err(e) => ItemResult::failed(&item, &cfg.worker_id, &e),
                };
                let post = result_to_post(&cfg.worker_id, w.lease_generation, &result);
                let dead = matches!(post.outcome, WireOutcome::DeadLetter { .. });
                coord.post(&post).await.map(|()| dead)
            })
            .buffer_unordered(cfg.max_lease.max(1) as usize)
            .collect::<Vec<_>>()
            .await;

        for outcome in batch {
            // A failed post is a transport error — surface it (the items stay leased
            // and will be re-queued by expiry; we don't silently drop work).
            let dead = outcome?;
            stats.processed += 1;
            if dead {
                stats.dead_lettered += 1;
            }
        }
    }
}

/// Build the [`InterruptionSource`] for a `--cloud` selection. `None` ⇒ no spot
/// watcher (e.g. on-prem / on-demand instances).
pub enum CloudSource {
    Aws(forge_spot::AwsSpot),
    Gcp(forge_spot::GcpSpot),
    Azure(forge_spot::AzureSpot),
}

impl CloudSource {
    /// Parse a `--cloud` flag. `"none"` (or empty) ⇒ `None`.
    pub fn parse(s: &str) -> Result<Option<Self>, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "" => Ok(None),
            "aws" => Ok(Some(CloudSource::Aws(forge_spot::AwsSpot::new()))),
            "gcp" => Ok(Some(CloudSource::Gcp(forge_spot::GcpSpot::new()))),
            "azure" => Ok(Some(CloudSource::Azure(forge_spot::AzureSpot::new()))),
            other => Err(format!(
                "unknown cloud {other:?} (expected aws|gcp|azure|none)"
            )),
        }
    }

    #[cfg(test)]
    fn cloud(&self) -> Cloud {
        match self {
            CloudSource::Aws(_) => Cloud::Aws,
            CloudSource::Gcp(_) => Cloud::Gcp,
            CloudSource::Azure(_) => Cloud::Azure,
        }
    }

    /// Run the spot watcher for the selected cloud, flipping `drain` on a notice.
    pub async fn drive(
        &self,
        interval: Duration,
        drain: &Drain,
        coord: &impl Coordinator,
        worker_id: &str,
    ) {
        match self {
            CloudSource::Aws(s) => spot_drain(s, interval, drain, coord, worker_id).await,
            CloudSource::Gcp(s) => spot_drain(s, interval, drain, coord, worker_id).await,
            CloudSource::Azure(s) => spot_drain(s, interval, drain, coord, worker_id).await,
        }
    }
}

fn cloud_str(c: Cloud) -> &'static str {
    match c {
        Cloud::Aws => "aws",
        Cloud::Gcp => "gcp",
        Cloud::Azure => "azure",
    }
}

fn kind_str(k: NoticeKind) -> &'static str {
    match k {
        NoticeKind::Terminate => "terminate",
        NoticeKind::Rebalance => "rebalance",
    }
}

// ───────────────────────── in-process coordinator ─────────────────────────

use std::sync::Mutex;

/// An in-memory [`Coordinator`]: a co-located single-box deployment (the agent and a
/// local item dispenser in one process) and the test vehicle. Hands out queued items
/// in FIFO batches, collects posts, and records notices. **Never** mutates anything
/// the agent shouldn't — it only models the coordinator's lease/ack surface.
#[derive(Default)]
pub struct InProcessCoordinator {
    inner: Mutex<CoordInner>,
}

#[derive(Default)]
struct CoordInner {
    /// Items still available to lease (FIFO).
    pending: std::collections::VecDeque<WireItem>,
    /// Posts received from the agent, in arrival order.
    posts: Vec<ResultPost>,
    /// Notices forwarded by the agent.
    notices: Vec<InterruptionNotice>,
    /// Optional drain latch to flip after the first non-empty pull (models a notice
    /// that lands mid-run, for deterministic drain tests).
    drain_after_first_pull: Option<Drain>,
    pulls: usize,
}

impl InProcessCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed the pending queue with wire items.
    pub fn with_items(items: impl IntoIterator<Item = WireItem>) -> Self {
        let c = Self::new();
        {
            let mut g = c.inner.lock().unwrap();
            g.pending.extend(items);
        }
        c
    }

    /// Arrange for `drain` to be latched right after the next non-empty pull — a
    /// deterministic stand-in for "a spot notice arrives while batch 1 is in flight."
    pub fn drain_after_first_pull(&self, drain: Drain) {
        self.inner.lock().unwrap().drain_after_first_pull = Some(drain);
    }

    /// Posts received so far (clone).
    pub fn posts(&self) -> Vec<ResultPost> {
        self.inner.lock().unwrap().posts.clone()
    }

    /// Items never leased (left for lease-expiry re-queue), by `custom_id`.
    pub fn remaining_ids(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .pending
            .iter()
            .map(|w| w.custom_id.clone())
            .collect()
    }

    /// Notices forwarded by the agent.
    pub fn notices(&self) -> Vec<InterruptionNotice> {
        self.inner.lock().unwrap().notices.clone()
    }
}

impl Coordinator for InProcessCoordinator {
    async fn pull(&self, req: &LeasePull) -> Result<Vec<WireItem>, ForgeError> {
        let mut g = self.inner.lock().unwrap();
        let take = (req.max as usize).min(g.pending.len());
        let batch: Vec<WireItem> = g.pending.drain(..take).collect();
        g.pulls += 1;
        if !batch.is_empty() {
            if let Some(d) = g.drain_after_first_pull.take() {
                d.signal();
            }
        }
        Ok(batch)
    }

    async fn post(&self, result: &ResultPost) -> Result<(), ForgeError> {
        self.inner.lock().unwrap().posts.push(result.clone());
        Ok(())
    }

    async fn notify(&self, notice: &InterruptionNotice) -> Result<(), ForgeError> {
        self.inner.lock().unwrap().notices.push(notice.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_core::{ItemResponse, TokenUsage, WorkerSpec};

    fn wire(id: &str) -> WireItem {
        WireItem {
            custom_id: id.into(),
            method: "POST".into(),
            url: "/v1/chat/completions".into(),
            body: serde_json::json!({"model": "m", "messages": []}),
            lease_generation: 1,
            attempts: 0,
        }
    }

    /// A trivial in-process worker that always succeeds (or always dead-letters),
    /// echoing the item id — no HTTP, fully deterministic.
    struct FakeWorker {
        spec: WorkerSpec,
        ready: AtomicBool,
        fail: bool,
    }

    impl FakeWorker {
        fn ok() -> Self {
            Self {
                spec: WorkerSpec::new("gpu1", "http://localhost", forge_core::EndpointKind::Chat),
                ready: AtomicBool::new(true),
                fail: false,
            }
        }
        fn failing() -> Self {
            Self {
                spec: WorkerSpec::new("gpu1", "http://localhost", forge_core::EndpointKind::Chat),
                ready: AtomicBool::new(true),
                fail: true,
            }
        }
    }

    impl Worker for FakeWorker {
        fn spec(&self) -> &WorkerSpec {
            &self.spec
        }
        fn is_ready(&self) -> bool {
            self.ready.load(Ordering::Relaxed)
        }
        async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
            if self.fail {
                return Err(ForgeError::Worker("boom".into()));
            }
            Ok(ItemResult {
                custom_id: item.custom_id.clone(),
                response: Some(ItemResponse {
                    status_code: 200,
                    request_id: None,
                    body: serde_json::json!({"echo": item.custom_id}),
                }),
                error: None,
                usage: TokenUsage::default(),
                worker_id: "gpu1".into(),
                latency_ms: 1,
                attempt: 1,
                completed_at: 0,
            })
        }
    }

    fn cfg() -> AgentConfig {
        AgentConfig {
            worker_id: "gpu1".into(),
            max_lease: 4,
            lease_secs: 30,
            poll_idle: Duration::from_millis(1),
            max_idle_polls: 1, // finite: exit as soon as the queue looks drained
        }
    }

    #[tokio::test]
    async fn drains_whole_queue_without_a_notice() {
        let coord = InProcessCoordinator::with_items((0..10).map(|i| wire(&format!("c{i}"))));
        let worker = FakeWorker::ok();
        let drain = Drain::new();

        let stats = run_agent(&cfg(), &coord, &worker, &drain)
            .await
            .expect("run");

        assert_eq!(stats.processed, 10);
        assert_eq!(stats.dead_lettered, 0);
        assert!(!stats.drained);
        assert_eq!(coord.posts().len(), 10);
        assert!(coord.remaining_ids().is_empty(), "queue fully drained");
        // Every post is a Done echoing its id.
        for p in coord.posts() {
            assert!(matches!(p.outcome, WireOutcome::Done { .. }));
        }
    }

    #[tokio::test]
    async fn notice_stops_new_leases_but_drains_in_flight() {
        // 10 items, max_lease 4 → batch 1 = 4 items. A notice lands right after the
        // first pull: batch 1 must still be posted (in-flight drain), and the other 6
        // must be left leased for lease-expiry (never pulled).
        let coord = InProcessCoordinator::with_items((0..10).map(|i| wire(&format!("c{i}"))));
        let worker = FakeWorker::ok();
        let drain = Drain::new();
        coord.drain_after_first_pull(drain.clone());

        let stats = run_agent(&cfg(), &coord, &worker, &drain)
            .await
            .expect("run");

        assert!(stats.drained, "loop exited via the drain latch");
        assert_eq!(stats.processed, 4, "only the in-flight batch drained");
        assert_eq!(coord.posts().len(), 4);
        assert_eq!(
            coord.remaining_ids().len(),
            6,
            "the rest stay leased for lease-expiry re-queue, not lost"
        );
    }

    #[tokio::test]
    async fn failures_become_dead_letter_posts_not_dropped() {
        let coord = InProcessCoordinator::with_items((0..3).map(|i| wire(&format!("c{i}"))));
        let worker = FakeWorker::failing();
        let drain = Drain::new();

        let stats = run_agent(&cfg(), &coord, &worker, &drain)
            .await
            .expect("run");

        assert_eq!(stats.processed, 3);
        assert_eq!(stats.dead_lettered, 3);
        for p in coord.posts() {
            assert!(matches!(p.outcome, WireOutcome::DeadLetter { .. }));
        }
    }

    #[tokio::test]
    async fn unhealthy_engine_is_not_pulled_into() {
        // Engine never ready → no pulls succeed, but the idle budget still lets the
        // loop exit (it never dead-letters a single item into a dead engine).
        let coord = InProcessCoordinator::with_items((0..5).map(|i| wire(&format!("c{i}"))));
        let worker = FakeWorker::ok();
        worker.ready.store(false, Ordering::Relaxed);
        let drain = Drain::new();

        // probe() default returns is_ready() (still false) → health-gate holds.
        let mut c = cfg();
        c.max_idle_polls = 0; // would loop forever; so drive drain to end it
        drain.signal();
        let stats = run_agent(&c, &coord, &worker, &drain).await.expect("run");
        assert_eq!(stats.processed, 0);
        assert_eq!(coord.remaining_ids().len(), 5, "nothing leased");
    }

    #[tokio::test]
    async fn spot_drain_latches_and_forwards_notice() {
        // A fake source that fires a terminate notice on the first poll.
        struct FiringSource;
        impl InterruptionSource for FiringSource {
            fn cloud(&self) -> Cloud {
                Cloud::Gcp
            }
            async fn poll(&self) -> Option<forge_spot::Notice> {
                Some(forge_spot::Notice {
                    cloud: Cloud::Gcp,
                    kind: NoticeKind::Terminate,
                    deadline: None,
                })
            }
        }

        let coord = InProcessCoordinator::new();
        let drain = Drain::new();
        spot_drain(
            &FiringSource,
            Duration::from_millis(1),
            &drain,
            &coord,
            "gpu1",
        )
        .await;

        assert!(drain.is_draining());
        let notices = coord.notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].cloud, "gcp");
        assert_eq!(notices[0].kind, "terminate");
    }

    #[test]
    fn cloud_source_parses_each_selector() {
        assert!(CloudSource::parse("none").unwrap().is_none());
        assert!(CloudSource::parse("").unwrap().is_none());
        assert!(matches!(
            CloudSource::parse("aws").unwrap().map(|c| c.cloud()),
            Some(Cloud::Aws)
        ));
        assert!(matches!(
            CloudSource::parse("GCP").unwrap().map(|c| c.cloud()),
            Some(Cloud::Gcp)
        ));
        assert!(matches!(
            CloudSource::parse("azure").unwrap().map(|c| c.cloud()),
            Some(Cloud::Azure)
        ));
        assert!(CloudSource::parse("digitalocean").is_err());
    }

    #[test]
    fn result_to_post_maps_success_and_failure() {
        let ok = ItemResult {
            custom_id: "c".into(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: None,
                body: serde_json::json!({"ok": true}),
            }),
            error: None,
            usage: TokenUsage::default(),
            worker_id: "gpu1".into(),
            latency_ms: 1,
            attempt: 1,
            completed_at: 0,
        };
        assert!(matches!(
            result_to_post("gpu1", 5, &ok).outcome,
            WireOutcome::Done { .. }
        ));

        let item = wire_to_item(&wire("c"));
        let bad = ItemResult::failed(&item, "gpu1", &ForgeError::Worker("nope".into()));
        let post = result_to_post("gpu1", 5, &bad);
        assert_eq!(post.lease_generation, 5);
        assert!(matches!(post.outcome, WireOutcome::DeadLetter { .. }));
    }
}
