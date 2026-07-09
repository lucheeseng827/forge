//! End-to-end: drive the OpenAI Batch REST front door over a real ephemeral socket
//! against an in-process **mock worker** (the same `Worker`-trait-impl approach the
//! agent's own transport tests use — no external engine needed), through the full SDK
//! flow: upload a file → create a batch → poll to `completed` → fetch the output content
//! → assert the OpenAI output shape and that every `custom_id` is present.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use forge_batch::{serve_batch, BatchConfig, WorkerFleet};
use forge_core::{
    EndpointKind, ForgeError, Item, ItemResponse, ItemResult, RunConfig, TokenUsage, Worker,
    WorkerSpec,
};
use serde_json::Value;

// ── the mock worker + fleet ──────────────────────────────────────────────────

/// A worker that echoes each item's `custom_id` back in a chat-completion-shaped body
/// and reports fixed token usage — a deterministic stand-in for a real vLLM endpoint.
struct EchoWorker {
    spec: WorkerSpec,
    ready: AtomicBool,
}

impl EchoWorker {
    fn new(id: &str) -> Self {
        Self {
            spec: WorkerSpec::new(id, "http://mock", EndpointKind::Chat),
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
                request_id: Some(format!("echo-{}", item.custom_id)),
                body: serde_json::json!({
                    "id": "chatcmpl-x",
                    "object": "chat.completion",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": item.custom_id}}],
                }),
            }),
            error: None,
            usage: TokenUsage {
                prompt_tokens: 3,
                completion_tokens: 4,
                total_tokens: 7,
                ..Default::default()
            },
            worker_id: self.spec.worker_id.clone(),
            latency_ms: 1,
            attempt: 1,
            completed_at: 0,
        })
    }
}

struct MockFleet;
impl WorkerFleet for MockFleet {
    type W = EchoWorker;
    fn build(&self) -> Result<Vec<EchoWorker>, ForgeError> {
        Ok(vec![EchoWorker::new("mock-0")])
    }
}

/// Like [`EchoWorker`] but pauses before answering — long enough that a batch of a few
/// items is reliably still `in_progress` (not yet drained) by the time a test calls
/// `shutdown()`, so the shutdown path actually has a running supervisor thread to stop.
struct SlowEchoWorker {
    spec: WorkerSpec,
}

impl SlowEchoWorker {
    fn new(id: &str) -> Self {
        Self {
            spec: WorkerSpec::new(id, "http://mock", EndpointKind::Chat),
        }
    }
}

impl Worker for SlowEchoWorker {
    fn spec(&self) -> &WorkerSpec {
        &self.spec
    }
    fn is_ready(&self) -> bool {
        true
    }
    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(ItemResult {
            custom_id: item.custom_id.clone(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: Some(format!("echo-{}", item.custom_id)),
                body: serde_json::json!({"id": "chatcmpl-x", "object": "chat.completion", "choices": []}),
            }),
            error: None,
            usage: TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                ..Default::default()
            },
            worker_id: self.spec.worker_id.clone(),
            latency_ms: 200,
            attempt: 1,
            completed_at: 0,
        })
    }
}

struct SlowMockFleet;
impl WorkerFleet for SlowMockFleet {
    type W = SlowEchoWorker;
    fn build(&self) -> Result<Vec<SlowEchoWorker>, ForgeError> {
        Ok(vec![SlowEchoWorker::new("slow-0")])
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("forge-batch-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn config(dir: std::path::PathBuf, api_key: Option<String>) -> BatchConfig<MockFleet> {
    BatchConfig {
        bind: "127.0.0.1:0".into(),
        data_dir: dir,
        api_key,
        fleet: MockFleet,
        run_config: RunConfig {
            // Fast, tight timings for the test — the mock never actually stalls.
            lease_for: Duration::from_millis(500),
            lease_max: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            ready_grace: Duration::from_secs(2),
            ..RunConfig::default()
        },
        threads: 2,
    }
}

fn slow_config(dir: std::path::PathBuf) -> BatchConfig<SlowMockFleet> {
    BatchConfig {
        bind: "127.0.0.1:0".into(),
        data_dir: dir,
        api_key: None,
        fleet: SlowMockFleet,
        run_config: RunConfig {
            lease_for: Duration::from_secs(5),
            lease_max: Duration::from_secs(10),
            poll_interval: Duration::from_millis(10),
            ready_grace: Duration::from_secs(2),
            ..RunConfig::default()
        },
        threads: 2,
    }
}

/// The forge-native / OpenAI batch input line shape.
fn input_line(id: &str) -> String {
    serde_json::json!({
        "custom_id": id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    })
    .to_string()
}

// ── the full-flow test ───────────────────────────────────────────────────────

#[tokio::test]
async fn full_openai_batch_flow_against_a_mock_worker() {
    let dir = tmp_dir("e2e");
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    // 1) Upload the input file (raw JSONL body; MVP multipart compromise).
    let n = 6usize;
    let body = (0..n)
        .map(|i| input_line(&format!("req-{i}")))
        .collect::<Vec<_>>()
        .join("\n");
    let file: Value = client
        .post(format!("{base}/v1/files?filename=in.jsonl"))
        .body(body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(file["object"], "file");
    assert_eq!(file["purpose"], "batch");
    let input_file_id = file["id"].as_str().unwrap().to_string();
    assert!(input_file_id.starts_with("file-"));

    // 2) Create the batch.
    let created: Value = client
        .post(format!("{base}/v1/batches"))
        .json(&serde_json::json!({
            "input_file_id": input_file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["object"], "batch");
    assert_eq!(created["status"], "in_progress");
    assert!(created["output_file_id"].is_null());
    let batch_id = created["id"].as_str().unwrap().to_string();
    assert!(batch_id.starts_with("batch-"));

    // 3) Poll until completed.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut batch: Value;
    loop {
        batch = client
            .get(format!("{base}/v1/batches/{batch_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if batch["status"] == "completed" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "batch did not complete in time: {batch}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // request_counts mapped from the queue: total = all, completed = done, failed = dead.
    assert_eq!(batch["request_counts"]["total"], n as u64);
    assert_eq!(batch["request_counts"]["completed"], n as u64);
    assert_eq!(batch["request_counts"]["failed"], 0);
    assert!(!batch["completed_at"].is_null());
    let output_file_id = batch["output_file_id"].as_str().unwrap().to_string();
    assert!(output_file_id.starts_with("file-"));
    assert!(
        batch["error_file_id"].is_null(),
        "no dead items → no error file"
    );

    // 4) Fetch + assert the OpenAI output shape; every custom_id present exactly once.
    let content = client
        .get(format!("{base}/v1/files/{output_file_id}/content"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for line in content.lines().filter(|l| !l.trim().is_empty()) {
        let v: Value = serde_json::from_str(line).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("batch_req_"));
        assert!(v["error"].is_null());
        let resp = &v["response"];
        assert_eq!(resp["status_code"], 200);
        assert!(!resp["request_id"].as_str().unwrap().is_empty());
        // The echoed content is the custom_id.
        assert_eq!(
            resp["body"]["choices"][0]["message"]["content"],
            v["custom_id"]
        );
        seen.insert(v["custom_id"].as_str().unwrap().to_string());
    }
    let expected: std::collections::BTreeSet<String> = (0..n).map(|i| format!("req-{i}")).collect();
    assert_eq!(seen, expected, "every custom_id present in the output");

    // 4b) The output file is supposed to be immutable — a second GET must return
    // byte-identical content (same `id`/`request_id` per line), not freshly-minted ones.
    let content_again = client
        .get(format!("{base}/v1/files/{output_file_id}/content"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        content, content_again,
        "repeated GETs of the same output file must be byte-identical"
    );

    // 5) List envelope contains the batch.
    let list: Value = client
        .get(format!("{base}/v1/batches"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["object"], "list");
    assert!(list["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["id"] == batch_id.as_str()));

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn shutdown_joins_a_still_running_batch_supervisor() {
    // Regression: `shutdown()` used to only stop + join the HTTP accept threads — a
    // batch's per-batch supervisor thread (BatchEntry.task) was never signalled or
    // joined, so it kept running detached, holding a `tokio::runtime::Handle` that
    // could outlive its own runtime. Prove the fix: create a batch on a deliberately
    // slow worker (so it's still `in_progress`), call `shutdown()`, and require it to
    // return within a bounded deadline — if the supervisor thread were still being
    // (improperly) awaited/blocked-on forever, this would hang and the timeout fires.
    let dir = tmp_dir("shutdown");
    let server = serve_batch(slow_config(dir.clone())).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    let n = 3usize;
    let body = (0..n)
        .map(|i| input_line(&format!("req-{i}")))
        .collect::<Vec<_>>()
        .join("\n");
    let file: Value = client
        .post(format!("{base}/v1/files"))
        .body(body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let created: Value = client
        .post(format!("{base}/v1/batches"))
        .json(&serde_json::json!({
            "input_file_id": file["id"], "endpoint": "/v1/chat/completions"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["status"], "in_progress");

    // Each item takes 200ms on a single worker; give the supervisor a moment to
    // actually start dispatching before we pull the rug.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let shutdown_result = tokio::task::spawn_blocking(move || server.shutdown());
    tokio::time::timeout(Duration::from_secs(10), shutdown_result)
        .await
        .expect("shutdown() must join the per-batch supervisor thread, not hang forever")
        .expect("shutdown() must not panic");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cancel_returns_live_counts_not_a_hardcoded_zero() {
    let dir = tmp_dir("cancel");
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    let n = 4usize;
    let body = (0..n)
        .map(|i| input_line(&format!("req-{i}")))
        .collect::<Vec<_>>()
        .join("\n");
    let file: Value = client
        .post(format!("{base}/v1/files"))
        .body(body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let created: Value = client
        .post(format!("{base}/v1/batches"))
        .json(&serde_json::json!({
            "input_file_id": file["id"], "endpoint": "/v1/chat/completions"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let batch_id = created["id"].as_str().unwrap().to_string();

    // Wait for the supervisor thread to hydrate the queue (total > 0) before cancelling —
    // otherwise this races the supervisor's startup and total is legitimately still 0,
    // which isn't what this test is checking for.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let batch: Value = client
            .get(format!("{base}/v1/batches/{batch_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if batch["request_counts"]["total"] == n as u64 {
            break;
        }
        assert!(Instant::now() < deadline, "queue never hydrated: {batch}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let cancelled: Value = client
        .post(format!("{base}/v1/batches/{batch_id}/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // Regression: cancel used to always embed RequestCounts::default() (all zeros)
    // regardless of the queue's real state. `total` is set once the queue is hydrated
    // and never changes afterward, so it must reflect the real input size here, whether
    // the batch was still in flight or had already finished by the time we cancelled.
    assert_eq!(
        cancelled["request_counts"]["total"], n as u64,
        "cancel must report live counts, not a hardcoded zero: {cancelled}"
    );

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn health_is_open_but_everything_else_needs_the_key() {
    let dir = tmp_dir("auth");
    let server = serve_batch(config(dir.clone(), Some("sk-secret".into()))).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    // Health: no auth required.
    let health = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    let hv: Value = health.json().await.unwrap();
    assert_eq!(hv["status"], "ok");

    // A protected route without the bearer → 401.
    let unauth = client
        .get(format!("{base}/v1/batches"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    // With the right bearer → 200.
    let ok = client
        .get(format!("{base}/v1/batches"))
        .bearer_auth("sk-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    // With a wrong bearer → 401.
    let bad = client
        .get(format!("{base}/v1/batches"))
        .bearer_auth("sk-nope")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 401);

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn batches_survive_a_restart_and_relist() {
    let dir = tmp_dir("restart");

    // First server: run a batch to completion, then shut down.
    let batch_id;
    let output_file_id;
    {
        let server = serve_batch(config(dir.clone(), None)).expect("serve");
        let base = server.base_url();
        let client = reqwest::Client::new();

        let file: Value = client
            .post(format!("{base}/v1/files"))
            .body(input_line("only-1"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let created: Value = client
            .post(format!("{base}/v1/batches"))
            .json(&serde_json::json!({
                "input_file_id": file["id"], "endpoint": "/v1/chat/completions"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        batch_id = created["id"].as_str().unwrap().to_string();

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let b: Value = client
                .get(format!("{base}/v1/batches/{batch_id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if b["status"] == "completed" {
                output_file_id = b["output_file_id"].as_str().unwrap().to_string();
                break;
            }
            assert!(Instant::now() < deadline, "did not complete: {b}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        server.shutdown();
    }

    // Second server over the SAME data-dir: the batch + its output file re-list, and the
    // completed results are still fetchable.
    {
        let server = serve_batch(config(dir.clone(), None)).expect("re-serve");
        let base = server.base_url();
        let client = reqwest::Client::new();

        let b: Value = client
            .get(format!("{base}/v1/batches/{batch_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(b["status"], "completed", "completed batch survived restart");
        assert_eq!(b["request_counts"]["completed"], 1);

        let content = client
            .get(format!("{base}/v1/files/{output_file_id}/content"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let line: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(line["custom_id"], "only-1");
        server.shutdown();
    }

    let _ = std::fs::remove_dir_all(&dir);
}
