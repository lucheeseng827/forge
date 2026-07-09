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
        })
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
            match self.client.post(url.as_str()).json(body).send().await {
                Ok(resp) => {
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
    async fn pull(&self, req: &LeasePull) -> Result<Vec<WireItem>, ForgeError> {
        let grant: LeaseGrant = self.post_json("/v1/lease", req).await?;
        Ok(grant.items)
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

/// Serve the coordinator side of the wire protocol over `bind` (e.g. `0.0.0.0:8080`
/// or `127.0.0.1:0` for an ephemeral test port), backed by `queue` + `store`.
/// `threads` accept threads share the listener (the queue's atomic lease keeps
/// concurrent pulls disjoint). `default_lease` caps how long a lease is granted when
/// the pull doesn't ask for more.
///
/// Must be called from within a Tokio runtime — the blocking accept threads bridge
/// into the async `Queue`/`ResultStore` via [`tokio::runtime::Handle::block_on`].
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
    let server = Arc::new(tiny_http::Server::http(bind).map_err(transport)?);
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| transport("server bound to a non-IP address"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let handle = tokio::runtime::Handle::current();

    let mut handles = Vec::new();
    for _ in 0..threads.max(1) {
        let server = Arc::clone(&server);
        let stop = Arc::clone(&stop);
        let queue = Arc::clone(&queue);
        let store = Arc::clone(&store);
        let rt = handle.clone();
        handles.push(std::thread::spawn(move || {
            // Poll with a timeout so the `stop` flag is observed promptly.
            while !stop.load(Ordering::SeqCst) {
                match server.recv_timeout(Duration::from_millis(200)) {
                    Ok(Some(req)) => {
                        handle_request(&rt, req, &queue, &store, default_lease);
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
) where
    Q: Queue + Send + Sync + 'static,
    S: ResultStore + Send + Sync + 'static,
{
    let method = req.method().clone();
    let path = req.url().to_string();
    let mut body = String::new();
    if req.as_reader().read_to_string(&mut body).is_err() {
        let _ = req.respond(text_status(400, "unreadable body"));
        return;
    }

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
            let grant = LeaseGrant {
                items: items.iter().map(item_to_wire).collect(),
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("server runtime");
        let server = {
            let _enter = rt.enter(); // so serve_coordinator captures THIS runtime's handle
            serve_coordinator(
                "127.0.0.1:0",
                queue,
                store,
                Duration::from_secs(30),
                threads,
            )
            .expect("serve")
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
}
