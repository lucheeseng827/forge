//! Networked `forge-proto` transport — the remote lease-proxy.
//!
//! Two ends of one small HTTP/1.1 + JSON protocol:
//!
//! - [`HttpCoordinator`] (agent side): a [`Coordinator`] backed by `reqwest`. The
//!   agent on a spot box points it at the central coordinator's base URL.
//! - [`serve_coordinator`] (coordinator side): a `tiny_http` server that maps the
//!   three routes onto a [`Queue`] + [`ResultStore`]. Deliberately a *minimal*
//!   blocking server (no async-framework stack) — it runs on its own OS threads and
//!   bridges into the async backends via the current Tokio runtime handle.
//!
//! **Single-writer is preserved across the wire.** The agent only ever *proposes*
//! results; the **server** is the one that writes — and it does so in the canonical
//! **store-then-ack** order (durable result first, then the lease-fenced queue
//! transition), exactly as the in-process coordinator loop does. A stale post (lease
//! already expired and re-leased) is a harmless no-op: the store write de-duplicates
//! and the fenced `ack`/`dead_letter` returns `false`.
//!
//! Routes (all `POST`, JSON in/out):
//!
//! | path | request | response |
//! |---|---|---|
//! | `/v1/lease` | [`LeasePull`] | [`LeaseGrant`] |
//! | `/v1/result` | [`ResultPost`] | [`AckReply`] |
//! | `/v1/interruption` | [`InterruptionNotice`] | `{"ok":true}` |
//!
//! **Authentication.** Served with [`serve_coordinator_with`] and a
//! [`CoordinatorConfig::token`], every route requires `Authorization: Bearer <token>`,
//! checked in constant time **before** the body is read, and the agent sends it via
//! [`HttpCoordinator::with_token`]. The wire hands out raw prompts and accepts the
//! results that become the record, so an open coordinator lets anyone who can reach
//! the port read prompts or forge results. [`serve_coordinator`] (no token) remains
//! for a coordinator that is only reachable from inside its own trust boundary.

use std::io::Read;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use forge_core::{
    now_ms, ForgeError, Item, ItemError, ItemResponse, ItemResult, Queue, ResultStore,
};
use forge_proto::{
    AckReply, InterruptionNotice, LeaseGrant, LeasePull, ResultPost, WireItem, WireOutcome,
};

use crate::{from_wire_usage, Coordinator};

/// Cap on one request body. A result post carries a single item's response, so even a
/// large embedding batch sits far below this; the cap exists so that no caller can
/// make an accept thread buffer an unbounded body into memory.
const MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;

fn transport(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Worker(format!("transport: {e}"))
}

// ───────────────────────────── client ─────────────────────────────

/// The agent-side [`Coordinator`]: speaks the wire protocol to a remote coordinator
/// over HTTP. One keep-alive `reqwest::Client`; long timeouts because a lease long-poll
/// or a big result body can legitimately take a while.
pub struct HttpCoordinator {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl HttpCoordinator {
    /// Point at a coordinator base URL (e.g. `http://coordinator:8080`).
    pub fn new(base: impl Into<String>) -> Result<Self, ForgeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(transport)?;
        Ok(Self {
            client,
            base: base.into(),
            token: None,
        })
    }

    /// Send `Authorization: Bearer <token>` on every request, for a coordinator served
    /// with a [`CoordinatorConfig::token`]. A blank token is refused rather than sent:
    /// it would be rejected by the server anyway, but only after the agent had started.
    pub fn with_token(mut self, token: impl AsRef<str>) -> Result<Self, ForgeError> {
        self.token = Some(forge_core::auth::non_blank_secret(
            "the coordinator token",
            token.as_ref(),
        )?);
        Ok(self)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base.trim_end_matches('/'), path)
    }

    async fn post_json<Req, Resp>(&self, path: &str, body: &Req) -> Result<Resp, ForgeError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        // Retry a transient transport failure. reqwest won't retry a non-idempotent
        // POST itself, but every coordinator route is safe to re-send *by effect*: a lost
        // result post is fenced by lease generation + deduplicated by the store, and a
        // lost lease pull just re-leases. So a dropped keep-alive connection or a momentary
        // reactor stall on a busy box costs a retry, not the whole agent run.
        //
        // We retry on *any* `send()` error (not just connect/timeout) because the safety
        // is by-effect, not by knowing the server didn't process it — but note the cost
        // when it *did*: an errored-but-processed `pull` (e.g. the response body was lost
        // in transit) leaves the first grant leased to nobody until its lease **expires**,
        // and the retry leases a *different* batch. That is bounded, self-healing waste
        // (one lease TTL of idle capacity), never lost or double-run work. A failure while
        // *decoding* a 2xx response body is surfaced as-is (not retried) — re-running it
        // would be equally safe but the loop keeps the retry to the connection attempt.
        // Bound total wall-clock across retries: each `send()` can burn the full request
        // timeout before erroring, so 5 naive retries could stack to ~10 min on a degraded
        // link. Stop retrying once this budget is spent (a full attempt in flight still
        // finishes — we just don't start another), keeping the hot pull/post path from
        // sitting stuck far longer than a single attempt would.
        const RETRY_BUDGET: Duration = Duration::from_secs(150);
        let url = self.url(path);
        let deadline = Instant::now() + RETRY_BUDGET;
        let mut attempt = 0u32;
        loop {
            let mut request = self.client.post(url.as_str()).json(body);
            if let Some(token) = &self.token {
                request = request.bearer_auth(token);
            }
            match request.send().await {
                Ok(resp) => {
                    // A 401 is never transient, so it is not retried, and it gets a
                    // message that names the fix rather than a bare status code.
                    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
                        return Err(transport(format!(
                            "{path} → HTTP 401: the coordinator rejected this agent's \
                             credentials. It requires the shared coordinator token \
                             (FORGE_COORDINATOR_TOKEN for the forge-agent CLI), and this \
                             agent sent {}.",
                            if self.token.is_some() {
                                "a token that does not match"
                            } else {
                                "none"
                            }
                        )));
                    }
                    if !resp.status().is_success() {
                        return Err(transport(format!("{} → HTTP {}", path, resp.status())));
                    }
                    return resp.json::<Resp>().await.map_err(transport);
                }
                Err(_) if attempt < 4 && Instant::now() < deadline => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                }
                Err(e) => return Err(transport(e)),
            }
        }
    }
}

impl Coordinator for HttpCoordinator {
    async fn pull(&self, req: &LeasePull) -> Result<LeaseGrant, ForgeError> {
        self.post_json("/v1/lease", req).await
    }

    async fn post(&self, result: &ResultPost) -> Result<(), ForgeError> {
        // The agent treats acked / stale-no-op identically; it just needs the post to
        // have been accepted (so the item won't silently vanish on a transport error).
        let _: AckReply = self.post_json("/v1/result", result).await?;
        Ok(())
    }

    async fn notify(&self, notice: &InterruptionNotice) -> Result<(), ForgeError> {
        // Advisory: ignore the body, only care that it was delivered.
        let _: serde_json::Value = self.post_json("/v1/interruption", notice).await?;
        Ok(())
    }
}

// ───────────────────────────── server ─────────────────────────────

/// A running coordinator server. Drop-free: call [`shutdown`](CoordinatorServer::shutdown)
/// to stop the accept threads and join them.
pub struct CoordinatorServer {
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    threads: Vec<JoinHandle<()>>,
}

impl CoordinatorServer {
    /// The bound address (useful when binding to port 0 in tests).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A base URL a [`HttpCoordinator`] can point at.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Signal the accept threads to stop and join them.
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.threads {
            let _ = t.join();
        }
    }
}

/// How [`serve_coordinator_with`] runs a coordinator.
#[derive(Clone)]
pub struct CoordinatorConfig {
    /// How long a lease is granted when the pull doesn't ask for a specific TTL.
    pub default_lease: Duration,
    /// Accept threads sharing the listener (the queue's atomic lease keeps concurrent
    /// pulls disjoint).
    pub threads: usize,
    /// Shared bearer token. `Some` → every route requires `Authorization: Bearer
    /// <token>`; `None` → open, for a coordinator reachable only from inside its own
    /// trust boundary.
    pub token: Option<String>,
}

impl CoordinatorConfig {
    /// An open coordinator with the given lease default and accept threads.
    pub fn new(default_lease: Duration, threads: usize) -> Self {
        Self {
            default_lease,
            threads,
            token: None,
        }
    }

    /// Require `token` on every route.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
}

// Hand-written so that logging a config can never print the token.
impl std::fmt::Debug for CoordinatorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorConfig")
            .field("default_lease", &self.default_lease)
            .field("threads", &self.threads)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Serve the coordinator side of the wire protocol over `bind`, **open** (no token).
///
/// Equivalent to [`serve_coordinator_with`] with [`CoordinatorConfig::new`]. Only
/// appropriate when the bind is reachable solely from your own workers' network; use
/// [`serve_coordinator_with`] and a token for anything else.
pub fn serve_coordinator<Q, S>(
    bind: &str,
    queue: Arc<Q>,
    store: Arc<S>,
    default_lease: Duration,
    threads: usize,
) -> Result<CoordinatorServer, ForgeError>
where
    Q: Queue + Send + Sync + 'static,
    S: ResultStore + Send + Sync + 'static,
{
    serve_coordinator_with(
        bind,
        queue,
        store,
        CoordinatorConfig::new(default_lease, threads),
    )
}

/// Serve the coordinator side of the wire protocol over `bind` (e.g. `0.0.0.0:8080`
/// or `127.0.0.1:0` for an ephemeral test port), backed by `queue` + `store`.
///
/// With a [`CoordinatorConfig::token`], a request without the matching bearer token is
/// answered `401` before its body is read. A blank token is refused here rather than
/// served, because it would accept `Bearer ` with nothing after it.
///
/// Must be called from within a Tokio runtime — the blocking accept threads bridge
/// into the async `Queue`/`ResultStore` via [`tokio::runtime::Handle::block_on`].
pub fn serve_coordinator_with<Q, S>(
    bind: &str,
    queue: Arc<Q>,
    store: Arc<S>,
    cfg: CoordinatorConfig,
) -> Result<CoordinatorServer, ForgeError>
where
    Q: Queue + Send + Sync + 'static,
    S: ResultStore + Send + Sync + 'static,
{
    let token: Option<Arc<str>> = cfg
        .token
        .as_deref()
        .map(|t| forge_core::auth::non_blank_secret("the coordinator token", t))
        .transpose()?
        .map(Arc::from);
    let default_lease = cfg.default_lease;
    let server = Arc::new(tiny_http::Server::http(bind).map_err(transport)?);
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| transport("server bound to a non-IP address"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let handle = tokio::runtime::Handle::current();

    let mut handles = Vec::new();
    for _ in 0..cfg.threads.max(1) {
        let server = Arc::clone(&server);
        let stop = Arc::clone(&stop);
        let queue = Arc::clone(&queue);
        let store = Arc::clone(&store);
        let token = token.clone();
        let rt = handle.clone();
        handles.push(std::thread::spawn(move || {
            // Poll with a timeout so the `stop` flag is observed promptly.
            while !stop.load(Ordering::SeqCst) {
                match server.recv_timeout(Duration::from_millis(200)) {
                    Ok(Some(req)) => {
                        handle_request(&rt, req, &queue, &store, default_lease, token.as_deref());
                    }
                    Ok(None) => continue, // idle timeout — re-check `stop`
                    Err(_) => break,
                }
            }
        }));
    }

    Ok(CoordinatorServer {
        stop,
        addr,
        threads: handles,
    })
}

fn handle_request<Q, S>(
    rt: &tokio::runtime::Handle,
    mut req: tiny_http::Request,
    queue: &Arc<Q>,
    store: &Arc<S>,
    default_lease: Duration,
    token: Option<&str>,
) where
    Q: Queue + Send + Sync + 'static,
    S: ResultStore + Send + Sync + 'static,
{
    // Authenticate before touching the body, so an unauthenticated caller never gets
    // a body buffered or parsed, let alone reaching the queue. The body stays unread on
    // the wire, and the patched HTTP layer closes the connection after the 401 instead
    // of draining it. Every failure is the same 401: missing, wrong scheme and wrong
    // token are indistinguishable from outside.
    if let Some(expected) = token {
        let auth = req
            .headers()
            .iter()
            .find(|h| h.field.equiv("Authorization"))
            .map(|h| h.value.as_str().to_string());
        if !forge_core::auth::bearer_matches(auth.as_deref(), expected) {
            let challenge = tiny_http::Header::from_bytes(&b"WWW-Authenticate"[..], &b"Bearer"[..])
                .expect("static header");
            let _ = req.respond(text_status(401, "unauthorized").with_header(challenge));
            return;
        }
    }

    let method = req.method().clone();
    let path = req.url().to_string();
    let mut raw = Vec::new();
    if req
        .as_reader()
        .take(MAX_BODY_BYTES + 1)
        .read_to_end(&mut raw)
        .is_err()
    {
        let _ = req.respond(text_status(400, "unreadable body"));
        return;
    }
    if raw.len() as u64 > MAX_BODY_BYTES {
        let _ = req.respond(text_status(413, "request body exceeds the size limit"));
        return;
    }
    let body = match String::from_utf8(raw) {
        Ok(b) => b,
        Err(_) => {
            let _ = req.respond(text_status(400, "request body is not UTF-8"));
            return;
        }
    };

    let outcome = route(rt, &method, &path, &body, queue, store, default_lease);
    let response = match outcome {
        Ok(json) => tiny_http::Response::from_string(json)
            .with_header(json_header())
            .with_status_code(200),
        Err((code, msg)) => text_status(code, &msg),
    };
    let _ = req.respond(response);
}

#[allow(clippy::type_complexity)]
fn route<Q, S>(
    rt: &tokio::runtime::Handle,
    method: &tiny_http::Method,
    path: &str,
    body: &str,
    queue: &Arc<Q>,
    store: &Arc<S>,
    default_lease: Duration,
) -> Result<String, (u16, String)>
where
    Q: Queue + Send + Sync + 'static,
    S: ResultStore + Send + Sync + 'static,
{
    // Every route is a POST.
    if *method != tiny_http::Method::Post {
        return Err((405, format!("{method} not allowed (POST only)")));
    }
    match path {
        "/v1/lease" => {
            let pull: LeasePull = parse(body)?;
            let lease_for = if pull.lease_secs == 0 {
                default_lease
            } else {
                Duration::from_secs(pull.lease_secs)
            };
            let items = rt
                .block_on(queue.lease(pull.max as usize, lease_for))
                .map_err(internal)?;
            // Only asked when the grant is empty, because that is the only case an
            // agent cannot answer for itself — and it costs a `counts()` round-trip,
            // which is not worth paying on every busy lease.
            let drained =
                items.is_empty() && rt.block_on(queue.counts()).map_err(internal)?.is_drained();
            let grant = LeaseGrant {
                items: items.iter().map(item_to_wire).collect(),
                drained,
            };
            serde_json::to_string(&grant).map_err(internal)
        }
        "/v1/result" => {
            let post: ResultPost = parse(body)?;
            let acked = rt
                .block_on(apply_result(queue, store, &post))
                .map_err(internal)?;
            serde_json::to_string(&AckReply { acked }).map_err(internal)
        }
        "/v1/interruption" => {
            let notice: InterruptionNotice = parse(body)?;
            tracing::warn!(
                worker_id = %notice.worker_id,
                cloud = %notice.cloud,
                kind = %notice.kind,
                "agent reported a spot interruption (advisory) — relying on lease expiry"
            );
            Ok("{\"ok\":true}".to_string())
        }
        _ => Err((404, format!("no route for {path}"))),
    }
}

/// The coordinator's store-then-ack, the exactly-once-*effect* ordering. Writes the
/// result durably **first**, then applies the lease-fenced queue transition. This is
/// the single place a worker/agent result mutates the queue — and it's on the
/// coordinator, so the single-writer rule holds.
async fn apply_result<Q, S>(
    queue: &Arc<Q>,
    store: &Arc<S>,
    post: &ResultPost,
) -> Result<bool, ForgeError>
where
    Q: Queue,
    S: ResultStore,
{
    match &post.outcome {
        WireOutcome::Done {
            status_code,
            response,
            usage,
            latency_ms,
        } => {
            let result = ItemResult {
                custom_id: post.custom_id.clone(),
                response: Some(ItemResponse {
                    status_code: *status_code,
                    request_id: None,
                    body: response.clone(),
                }),
                error: None,
                usage: from_wire_usage(usage),
                worker_id: post.worker_id.clone(),
                latency_ms: *latency_ms,
                attempt: post.lease_generation as u32,
                completed_at: now_ms(),
            };
            store.put(&result).await?; // durable FIRST
            queue
                .ack(&post.custom_id, post.lease_generation as u32)
                .await // then fenced ack
        }
        WireOutcome::DeadLetter { error } => {
            let result = ItemResult {
                custom_id: post.custom_id.clone(),
                response: None,
                error: Some(ItemError {
                    code: "worker_error".to_string(),
                    message: error.clone(),
                }),
                usage: Default::default(),
                worker_id: post.worker_id.clone(),
                latency_ms: 0,
                attempt: post.lease_generation as u32,
                completed_at: now_ms(),
            };
            store.dead_letter(&result).await?; // durable FIRST
            queue
                .dead_letter(&post.custom_id, post.lease_generation as u32, error)
                .await
        }
    }
}

/// Map a leased [`Item`] to its wire form. The post-increment `attempts` the queue
/// stamped on lease *is* the fencing generation echoed back in the result.
fn item_to_wire(it: &Item) -> WireItem {
    WireItem {
        custom_id: it.custom_id.clone(),
        method: it.method.clone(),
        url: it.url.clone(),
        body: it.body.clone(),
        lease_generation: it.attempts as u64,
        attempts: it.attempts,
    }
}

fn parse<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, (u16, String)> {
    serde_json::from_str(body).map_err(|e| (400, format!("bad request body: {e}")))
}

fn internal(e: impl std::fmt::Display) -> (u16, String) {
    (500, e.to_string())
}

fn json_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
        .expect("static header")
}

fn text_status(code: u16, msg: &str) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    tiny_http::Response::from_string(msg.to_string()).with_status_code(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_agent, AgentConfig, Drain};
    use forge_core::{EndpointKind, ItemState, TokenUsage, Worker, WorkerSpec};
    use forge_queue::SqliteQueue;
    use forge_store::JsonlStore;

    // The two socket tests each drive a real reqwest client against a `tiny_http`
    // coordinator. On a CPU-constrained box (this CI runner has 4 cores) running both
    // at once oversubscribes the scheduler badly enough that the client's reactor
    // thread can be starved for a minute, stalling a request until it errors. They
    // exercise independent state, so serialize them — each then runs like it does in
    // isolation (where it is rock-solid). Kept small and few-threaded for the same
    // reason: a real deployment never co-locates the agent and coordinator like this.
    static SOCKET_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Serve the coordinator on its OWN dedicated runtime so its `block_on`-bridged
    /// async work (Queue/ResultStore I/O) has an isolated reactor + blocking pool and
    /// never competes with the client for the (current-thread) test runtime — the
    /// isolation a real deployment gets from separate agent/coordinator processes.
    /// Keep the returned runtime alive until after `server.shutdown()`, then
    /// `shutdown_background()` it (dropping a runtime on an async worker thread panics).
    fn serve_on_dedicated_runtime<Q, S>(
        queue: Arc<Q>,
        store: Arc<S>,
        threads: usize,
    ) -> (tokio::runtime::Runtime, CoordinatorServer)
    where
        Q: Queue + Send + Sync + 'static,
        S: ResultStore + Send + Sync + 'static,
    {
        serve_with_on_dedicated_runtime(
            queue,
            store,
            CoordinatorConfig::new(Duration::from_secs(30), threads),
        )
    }

    /// [`serve_on_dedicated_runtime`] with a full [`CoordinatorConfig`] (e.g. a token).
    fn serve_with_on_dedicated_runtime<Q, S>(
        queue: Arc<Q>,
        store: Arc<S>,
        cfg: CoordinatorConfig,
    ) -> (tokio::runtime::Runtime, CoordinatorServer)
    where
        Q: Queue + Send + Sync + 'static,
        S: ResultStore + Send + Sync + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("server runtime");
        let server = {
            let _enter = rt.enter(); // so serve_coordinator captures THIS runtime's handle
            serve_coordinator_with("127.0.0.1:0", queue, store, cfg).expect("serve")
        };
        (rt, server)
    }

    fn item(id: &str) -> Item {
        Item {
            custom_id: id.into(),
            method: "POST".into(),
            url: "/v1/chat/completions".into(),
            body: serde_json::json!({"model": "m", "messages": []}),
            status: ItemState::Pending,
            attempts: 0,
            leased_until: None,
            leased_by: None,
            last_error: None,
        }
    }

    /// A worker that echoes the id and reports a fixed token usage, so we can prove
    /// usage survives the network hop into the store.
    struct EchoWorker {
        spec: WorkerSpec,
        ready: AtomicBool,
    }
    impl EchoWorker {
        fn new() -> Self {
            Self {
                spec: WorkerSpec::new("gpu1", "http://localhost", EndpointKind::Chat),
                ready: AtomicBool::new(true),
            }
        }
    }
    impl Worker for EchoWorker {
        fn spec(&self) -> &WorkerSpec {
            &self.spec
        }
        fn is_ready(&self) -> bool {
            self.ready.load(Ordering::Relaxed)
        }
        async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
            Ok(ItemResult {
                custom_id: item.custom_id.clone(),
                response: Some(ItemResponse {
                    status_code: 200,
                    request_id: None,
                    body: serde_json::json!({"echo": item.custom_id, "usage": {"total_tokens": 7}}),
                }),
                error: None,
                usage: TokenUsage {
                    prompt_tokens: 3,
                    completion_tokens: 4,
                    total_tokens: 7,
                    ..Default::default()
                },
                worker_id: "gpu1".into(),
                latency_ms: 5,
                attempt: 1,
                completed_at: 0,
            })
        }
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "forge-agent-http-{}-{name}.jsonl",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn end_to_end_over_a_real_socket() {
        let _serial = SOCKET_TEST.lock().await;

        // Coordinator side: a real in-memory queue hydrated with 12 items + a JSONL
        // store, served over an ephemeral localhost port.
        let queue = Arc::new(SqliteQueue::open_in_memory().expect("queue"));
        let items: Vec<Item> = (0..12).map(|i| item(&format!("c{i}"))).collect();
        queue.enqueue(&items).await.expect("hydrate");

        let out = tmp("e2e");
        let _ = std::fs::remove_file(&out);
        let store = Arc::new(JsonlStore::new(&out));

        // Serve on a dedicated runtime (see `serve_on_dedicated_runtime`); 2 accept
        // threads keep the footprint small while still overlapping the agent's posts.
        let (server_rt, server) =
            serve_on_dedicated_runtime(Arc::clone(&queue), Arc::clone(&store), 2);

        // Agent side: point an HttpCoordinator at the server and run the loop against a
        // local echo worker until the queue drains.
        let coord = HttpCoordinator::new(server.base_url()).expect("client");
        let worker = EchoWorker::new();
        let drain = Drain::new();
        let cfg = AgentConfig {
            worker_id: "gpu1".into(),
            max_lease: 5,
            lease_secs: 30,
            poll_idle: Duration::from_millis(5),
            max_idle_polls: 2,
        };

        let stats = run_agent(&cfg, &coord, &worker, &drain).await.expect("run");
        server.shutdown();
        server_rt.shutdown_background();

        assert_eq!(stats.processed, 12, "all items ran over the wire");
        assert_eq!(stats.dead_lettered, 0);

        // The coordinator wrote every result and drained the queue (store-then-ack).
        let counts = queue.counts().await.expect("counts");
        assert_eq!(counts.done, 12);
        assert_eq!(counts.pending, 0);
        assert_eq!(counts.leased, 0);

        // Usage survived the hop: the store can sum captured tokens.
        let total = forge_store::sum_usage(&out).await.expect("sum");
        assert_eq!(
            total.total_tokens(),
            12 * 7,
            "captured usage faithful over the wire"
        );
        let _ = std::fs::remove_file(&out);
    }

    #[tokio::test]
    async fn stale_post_is_a_harmless_no_op() {
        let _serial = SOCKET_TEST.lock().await;

        // Post a result for an item the queue never leased (generation it can't match)
        // → the fenced ack returns false, but the call still succeeds (no error path).
        let queue = Arc::new(SqliteQueue::open_in_memory().expect("queue"));
        queue.enqueue(&[item("c0")]).await.expect("hydrate");
        let out = tmp("stale");
        let _ = std::fs::remove_file(&out);
        let store = Arc::new(JsonlStore::new(&out));
        let (server_rt, server) =
            serve_on_dedicated_runtime(Arc::clone(&queue), Arc::clone(&store), 1);

        let coord = HttpCoordinator::new(server.base_url()).expect("client");
        let post = ResultPost {
            custom_id: "c0".into(),
            lease_generation: 999, // never issued
            worker_id: "gpu1".into(),
            outcome: WireOutcome::Done {
                status_code: 200,
                response: serde_json::json!({"echo": "c0"}),
                usage: Default::default(),
                latency_ms: 1,
            },
        };
        // The agent's `post` returns Ok even though the fenced ack is a no-op.
        coord.post(&post).await.expect("post accepted");
        server.shutdown();
        server_rt.shutdown_background();

        // The item stays Pending (never acked by the stale generation).
        let counts = queue.counts().await.expect("counts");
        assert_eq!(counts.done, 0);
        assert_eq!(counts.pending, 1);
        let _ = std::fs::remove_file(&out);
    }

    #[tokio::test]
    async fn a_tokened_coordinator_serves_only_agents_that_hold_the_token() {
        let _serial = SOCKET_TEST.lock().await;

        let queue = Arc::new(SqliteQueue::open_in_memory().expect("queue"));
        let items: Vec<Item> = (0..6).map(|i| item(&format!("t{i}"))).collect();
        queue.enqueue(&items).await.expect("hydrate");
        let out = tmp("token");
        let _ = std::fs::remove_file(&out);
        let store = Arc::new(JsonlStore::new(&out));
        let (server_rt, server) = serve_with_on_dedicated_runtime(
            Arc::clone(&queue),
            Arc::clone(&store),
            CoordinatorConfig::new(Duration::from_secs(30), 2).token("s3cret"),
        );

        let pull = LeasePull {
            worker_id: "intruder".into(),
            max: 6,
            lease_secs: 30,
        };

        // No token: refused with a message that names the fix, and nothing leased.
        let bare = HttpCoordinator::new(server.base_url()).expect("client");
        let err = bare
            .pull(&pull)
            .await
            .expect_err("no token must be refused");
        let msg = err.to_string();
        assert!(msg.contains("401"), "{msg}");
        assert!(msg.contains("FORGE_COORDINATOR_TOKEN"), "{msg}");
        assert!(msg.contains("sent none"), "{msg}");

        // Wrong token: refused, and told the token it sent does not match.
        let wrong = HttpCoordinator::new(server.base_url())
            .expect("client")
            .with_token("guess")
            .expect("token");
        let msg = wrong
            .pull(&pull)
            .await
            .expect_err("wrong token")
            .to_string();
        assert!(msg.contains("does not match"), "{msg}");

        // A forged result post without the token never reaches the store or the queue.
        let forged = ResultPost {
            custom_id: "t0".into(),
            lease_generation: 1,
            worker_id: "intruder".into(),
            outcome: WireOutcome::Done {
                status_code: 200,
                response: serde_json::json!({"forged": true}),
                usage: Default::default(),
                latency_ms: 1,
            },
        };
        assert!(bare.post(&forged).await.is_err());
        let c = queue.counts().await.expect("counts");
        assert_eq!(
            (c.pending, c.leased, c.done),
            (6, 0, 0),
            "refused calls changed nothing"
        );

        // The right token drains the queue end to end.
        let coord = HttpCoordinator::new(server.base_url())
            .expect("client")
            .with_token("s3cret")
            .expect("token");
        let worker = EchoWorker::new();
        let drain = Drain::new();
        let cfg = AgentConfig {
            worker_id: "gpu1".into(),
            max_lease: 3,
            lease_secs: 30,
            poll_idle: Duration::from_millis(5),
            max_idle_polls: 2,
        };
        let stats = run_agent(&cfg, &coord, &worker, &drain).await.expect("run");
        server.shutdown();
        server_rt.shutdown_background();

        assert_eq!(stats.processed, 6);
        assert_eq!(queue.counts().await.expect("counts").done, 6);
        let written = std::fs::read_to_string(&out).expect("results");
        assert!(
            !written.contains("forged"),
            "the forged post was never stored"
        );
        let _ = std::fs::remove_file(&out);
    }

    #[tokio::test]
    async fn a_blank_token_is_refused_rather_than_served_open() {
        let queue = Arc::new(SqliteQueue::open_in_memory().expect("queue"));
        let store = Arc::new(JsonlStore::new(tmp("blank")));
        let err = serve_coordinator_with(
            "127.0.0.1:0",
            queue,
            store,
            CoordinatorConfig::new(Duration::from_secs(30), 1).token(" \n"),
        )
        .err()
        .expect("a blank token must not start an effectively-open server");
        assert!(err.to_string().contains("empty"), "{err}");

        // The client refuses to send one too.
        assert!(HttpCoordinator::new("http://127.0.0.1:1")
            .expect("client")
            .with_token("")
            .is_err());
    }

    #[test]
    fn a_config_never_prints_its_token() {
        let cfg = CoordinatorConfig::new(Duration::from_secs(1), 1).token("s3cret");
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("redacted"), "{shown}");
    }
}
