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
            // Concurrency 1 on purpose: with the default limit the whole batch runs in
            // parallel and finishes inside one 200ms sleep, so a test that means to
            // catch the server mid-flight catches nothing.
            spec: WorkerSpec::new(id, "http://mock", EndpointKind::Chat).concurrency(1),
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

    // The newer routes sit behind the same gate: listing files, deleting one, and
    // reading a batch's results mid-run all reveal or destroy customer data.
    for (method, path) in [
        (reqwest::Method::GET, "/v1/files"),
        (reqwest::Method::DELETE, "/v1/files/file-x"),
        (reqwest::Method::GET, "/v1/batches/batch-x/results"),
    ] {
        let r = client
            .request(method.clone(), format!("{base}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401, "{method} {path} without the key");
    }

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

/// `/metrics` is the scrape surface for a `serve-batch` deployment. Before it existed
/// the documented way to collect forge metrics was `forge status --prometheus` on a
/// timer — a CLI read of a checkpoint file — which cannot run in the shipped
/// `FROM scratch` image at all, since there is no shell.
#[tokio::test]
async fn metrics_exposes_counters_and_gauges_and_respects_auth() {
    let dir = tmp_dir("metrics");
    let server = serve_batch(config(dir.clone(), Some("sk-secret".into()))).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    // Unlike /v1/health, metrics are behind the key: they carry per-status batch and
    // item counts, which is business telemetry rather than liveness.
    let unauth = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(unauth.status(), 401, "metrics must not be world-readable");

    let resp = client
        .get(format!("{base}/metrics"))
        .bearer_auth("sk-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/plain"),
        "Prometheus requires the text exposition content type"
    );
    let body = resp.text().await.unwrap();

    // Counters carry a TYPE line naming them counters — that is what lets rate()
    // work, and it is the whole reason this is not just `status --prometheus` again.
    for counter in [
        "forge_batches_created_total",
        "forge_batches_completed_total",
        "forge_batches_cancelled_total",
        "forge_batches_failed_total",
        "forge_files_uploaded_total",
        "forge_http_requests_total",
        "forge_http_errors_total",
    ] {
        assert!(
            body.contains(&format!("# TYPE {counter} counter")),
            "{counter} must be declared a counter, not a gauge:\n{body}"
        );
    }

    // Every batch status is present even at zero, so a dashboard shows 0 rather than
    // a gap before anything has failed.
    for status in [
        "in_progress",
        "completed",
        "cancelling",
        "cancelled",
        "failed",
    ] {
        assert!(
            body.contains(&format!("forge_batches{{status=\"{status}\"}}")),
            "missing zero-valued series for {status}:\n{body}"
        );
    }

    // The unauthenticated attempt above must have been counted as an error.
    let errors = body
        .lines()
        .find(|l| l.starts_with("forge_http_errors_total "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .expect("forge_http_errors_total value");
    assert!(
        errors >= 1,
        "the 401 should have been counted, got {errors}"
    );

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── restart recovery ─────────────────────────────────────────────────────────

/// A batch that was still running when the process died must actually **run again**.
///
/// This is the 3am case, and the pre-fix behaviour was the worst possible shape:
/// `bootstrap` re-listed the batch with no supervisor and its persisted status still
/// `in_progress`, so the API kept answering `in_progress` — for a job that had stopped
/// dead. A customer's SDK polls that forever. Nothing errors, nothing alerts, and the
/// only symptom is a number that never moves.
#[tokio::test]
async fn a_batch_left_in_flight_by_a_dead_process_is_resumed_on_restart() {
    let dir = tmp_dir("restart-resume");
    let client = reqwest::Client::new();
    let n = 8usize;
    let body = (0..n)
        .map(|i| input_line(&format!("req-{i}")))
        .collect::<Vec<_>>()
        .join("\n");

    // First process: upload, create, and stop while it is genuinely mid-flight. The
    // slow worker (200ms/item, 8 items) guarantees it has not drained.
    let batch_id = {
        let server = serve_batch(slow_config(dir.clone())).expect("serve");
        let base = server.base_url();
        let file: Value = client
            .post(format!("{base}/v1/files?filename=in.jsonl"))
            .body(body.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let batch: Value = client
            .post(format!("{base}/v1/batches"))
            .json(&serde_json::json!({
                "input_file_id": file["id"],
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(batch["status"], "in_progress");
        // Let a little work land, then stop the process.
        tokio::time::sleep(Duration::from_millis(300)).await;
        server.shutdown();
        batch["id"].as_str().unwrap().to_string()
    };

    // Second process over the SAME data dir — a pod restart.
    let server = serve_batch(slow_config(dir.clone())).expect("re-serve");
    let base = server.base_url();

    // It must reach `completed` on its own. Pre-fix this loop times out with the batch
    // pinned at `in_progress` and nothing running it.
    let deadline = Instant::now() + Duration::from_secs(30);
    let final_status = loop {
        let b: Value = client
            .get(format!("{base}/v1/batches/{batch_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let st = b["status"].as_str().unwrap_or("").to_string();
        if st == "completed" {
            break b;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted server never resumed the batch: it is still {st:?} — a \
             zombie reporting progress it will never make"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Every item present exactly once: the resume re-hydrated idempotently and the
    // store deduped what the first process had already written.
    assert_eq!(final_status["request_counts"]["total"], n as u64);
    assert_eq!(final_status["request_counts"]["completed"], n as u64);
    let out_id = final_status["output_file_id"]
        .as_str()
        .expect("output file");
    let content = client
        .get(format!("{base}/v1/files/{out_id}/content"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let ids: std::collections::HashSet<String> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["custom_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(ids.len(), n, "every item exactly once after the restart");
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A truncated `files.json` must stop the server, not silently empty it.
///
/// The old loader swallowed every error and started with an EMPTY registry — and the
/// next upload persisted that empty map, so a recoverable half-written file became the
/// permanent loss of every file record. The uploaded bytes are still on disk; only the
/// index mapping ids to them is gone, which is exactly the situation a human can fix
/// and a program must not paper over.
#[tokio::test]
async fn a_corrupt_file_registry_refuses_to_boot_rather_than_starting_blank() {
    let dir = tmp_dir("corrupt-registry");
    let client = reqwest::Client::new();

    let file_id = {
        let server = serve_batch(config(dir.clone(), None)).expect("serve");
        let base = server.base_url();
        let file: Value = client
            .post(format!("{base}/v1/files?filename=in.jsonl"))
            .body(input_line("req-0"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        server.shutdown();
        file["id"].as_str().unwrap().to_string()
    };

    // Simulate the crash-mid-write: a truncated registry.
    let reg = dir.join("files.json");
    let good = std::fs::read_to_string(&reg).unwrap();
    assert!(good.contains(&file_id));
    std::fs::write(&reg, &good[..good.len() / 2]).unwrap();

    let err = match serve_batch(config(dir.clone(), None)) {
        Err(e) => e,
        Ok(_) => panic!("a truncated file registry must not boot as an empty one"),
    };
    let msg = err.to_string();
    assert!(msg.contains("unreadable"), "name it: {msg}");
    assert!(
        msg.contains("permanent"),
        "say why starting blank is worse than refusing: {msg}"
    );
    // And it did not rewrite the damaged file on the way out.
    assert_eq!(std::fs::read_to_string(&reg).unwrap().len(), good.len() / 2);

    // Restoring it brings the server back with the record intact.
    std::fs::write(&reg, &good).unwrap();
    let server = serve_batch(config(dir.clone(), None)).expect("boot after restore");
    let base = server.base_url();
    let obj: Value = client
        .get(format!("{base}/v1/files/{file_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(obj["id"], file_id);
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The registry write must be atomic: no window where the file is truncated on disk.
#[tokio::test]
async fn the_file_registry_is_never_left_half_written() {
    let dir = tmp_dir("atomic-registry");
    let client = reqwest::Client::new();
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();

    // Every upload rewrites the whole registry; read it back after each one and assert
    // it always parses. With a truncate-then-write there is a window where it does not.
    for i in 0..12 {
        client
            .post(format!("{base}/v1/files?filename=in.jsonl"))
            .body(input_line(&format!("req-{i}")))
            .send()
            .await
            .unwrap();
        let raw = std::fs::read(dir.join("files.json")).unwrap();
        serde_json::from_slice::<Value>(&raw)
            .unwrap_or_else(|e| panic!("registry not parseable after write {i}: {e}"));
    }
    // No temp files survive.
    let strays: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
        .collect();
    assert!(
        strays.is_empty(),
        "temp files must be renamed away, not left"
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── operator eyes ────────────────────────────────────────────────────────────

/// `/v1/health` must answer the two questions a probe and a support ticket actually
/// ask — is this process serving, and which build is it — and must NOT answer the one
/// it has no business answering to an unauthenticated caller.
#[tokio::test]
async fn health_reports_build_identity_and_no_longer_leaks_the_batch_count() {
    let dir = tmp_dir("health");
    let server = serve_batch(config(dir.clone(), Some("k".into()))).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    // Create a batch so there IS a count that could leak.
    let file: Value = client
        .post(format!("{base}/v1/files?filename=in.jsonl"))
        .header("authorization", "Bearer k")
        .body(input_line("req-0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    client
        .post(format!("{base}/v1/batches"))
        .header("authorization", "Bearer k")
        .json(&serde_json::json!({
            "input_file_id": file["id"], "endpoint": "/v1/chat/completions"
        }))
        .send()
        .await
        .unwrap();

    // No credential at all — this route has to stay open for a load balancer.
    let r = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let h: Value = r.json().await.unwrap();

    assert_eq!(h["status"], "ok");
    assert!(
        h["build"]["version"].is_string(),
        "which build is this? {h}"
    );
    assert!(
        h["uptime_secs"].is_u64(),
        "a resetting uptime is a crash loop: {h}"
    );
    assert!(
        h.get("batches").is_none(),
        "how much work a customer is running is business telemetry; /metrics is gated \
         on exactly that, so answering it here made the gate decorative: {h}"
    );
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every response carries a request id, and a caller's own id is echoed — but only
/// after being made safe to put in a header and a log line.
#[tokio::test]
async fn every_response_carries_a_request_id_and_a_hostile_one_is_sanitised() {
    let dir = tmp_dir("reqid");
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    // Minted when the caller supplies none — including on a success, so "quote the
    // request id" works for a response that was wrong rather than failed.
    let r = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap();
    let id = r
        .headers()
        .get("x-request-id")
        .expect("every response carries one")
        .to_str()
        .unwrap()
        .to_string();
    assert!(id.starts_with("req_"), "{id}");

    // A caller's trace id is echoed, so their logs join to forge's.
    let r = client
        .get(format!("{base}/v1/health"))
        .header("x-request-id", "0af7651916cd43dd")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-request-id"], "0af7651916cd43dd");

    // …but never verbatim. Anything outside the allowlist is stripped rather than
    // reflected into a response header and every log line for the request.
    //
    // (A raw CRLF or NUL cannot be tested through this client — `reqwest` refuses to
    // *send* one — so the response-splitting and log-forging cases are covered by the
    // unit tests in `forge_core::obs`, which call the sanitiser directly. What a real
    // client CAN send is punctuation, which is what this asserts.)
    let r = client
        .get(format!("{base}/v1/health"))
        .header("x-request-id", "trace id;with\"junk<>")
        .send()
        .await
        .unwrap();
    let echoed = r.headers()["x-request-id"].to_str().unwrap();
    assert_eq!(echoed, "traceidwithjunk");
    for c in [' ', ';', '"', '<', '>'] {
        assert!(
            !echoed.contains(c),
            "{c:?} survived into a response header: {echoed}"
        );
    }
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── files list / delete (OpenAI SDK parity) ──────────────────────────────────

/// Upload `body` as a batch input file and return its id.
async fn upload(client: &reqwest::Client, base: &str, body: String) -> String {
    let f: Value = client
        .post(format!("{base}/v1/files"))
        .body(body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    f["id"].as_str().unwrap().to_string()
}

/// Create a batch over `input_file_id` and return its id.
async fn create(client: &reqwest::Client, base: &str, input_file_id: &str) -> String {
    let b: Value = client
        .post(format!("{base}/v1/batches"))
        .json(&serde_json::json!({
            "input_file_id": input_file_id, "endpoint": "/v1/chat/completions"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    b["id"].as_str().unwrap().to_string()
}

/// Poll a batch until it reports `status`, returning the batch object.
async fn wait_for_status(client: &reqwest::Client, base: &str, id: &str, status: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let b: Value = client
            .get(format!("{base}/v1/batches/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if b["status"] == status {
            return b;
        }
        assert!(Instant::now() < deadline, "never reached {status}: {b}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn listed_ids(list: &Value) -> Vec<String> {
    assert_eq!(list["object"], "list");
    list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap().to_string())
        .collect()
}

/// `client.files.list()` and `client.files.delete()` are the first cleanup calls
/// existing SDK code makes, and both used to answer `404 no such route`.
#[tokio::test]
async fn files_list_and_delete_follow_the_openai_sdk_contract() {
    let dir = tmp_dir("files");
    let server = serve_batch(slow_config(dir.clone())).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    let busy_input = upload(
        &client,
        &base,
        (0..5)
            .map(|i| input_line(&format!("a-{i}")))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await;
    let spare = upload(&client, &base, input_line("b-0")).await;

    // Both inputs list, and a purpose filter narrows the list.
    let list: Value = client
        .get(format!("{base}/v1/files"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids = listed_ids(&list);
    assert!(ids.contains(&busy_input) && ids.contains(&spare), "{list}");
    assert!(list["data"]
        .as_array()
        .unwrap()
        .iter()
        .all(|f| f["purpose"] == "batch"));
    let outputs: Value = client
        .get(format!("{base}/v1/files?purpose=batch_output"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed_ids(&outputs).is_empty(), "{outputs}");

    // The input of an in-progress batch cannot be deleted: a restart would re-read it.
    let batch = create(&client, &base, &busy_input).await;
    let r = client
        .delete(format!("{base}/v1/files/{busy_input}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    assert!(r.text().await.unwrap().contains(&batch));
    let r = client
        .get(format!("{base}/v1/files/{busy_input}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "a refused delete leaves the file in place");

    // A file nothing is using is deleted: the SDK's FileDeleted shape, record and bytes gone.
    let deleted: Value = client
        .delete(format!("{base}/v1/files/{spare}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        deleted,
        serde_json::json!({"id": spare, "object": "file", "deleted": true})
    );
    for path in [
        format!("/v1/files/{spare}"),
        format!("/v1/files/{spare}/content"),
    ] {
        let r = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(r.status(), 404, "{path}");
    }
    let r = client
        .delete(format!("{base}/v1/files/{spare}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "a second delete finds nothing");
    assert!(
        !dir.join("files").join(format!("{spare}.jsonl")).exists(),
        "the bytes are removed, not just the record"
    );

    // Once the batch is done, both its input and its output can go — and the batch still
    // describes itself.
    let done = wait_for_status(&client, &base, &batch, "completed").await;
    let output = done["output_file_id"].as_str().unwrap().to_string();
    let outputs: Value = client
        .get(format!("{base}/v1/files?purpose=batch_output"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed_ids(&outputs), vec![output.clone()]);
    for id in [&busy_input, &output] {
        let r = client
            .delete(format!("{base}/v1/files/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{id}");
    }
    let r = client
        .get(format!("{base}/v1/files/{output}/content"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let b = wait_for_status(&client, &base, &batch, "completed").await;
    assert_eq!(b["request_counts"]["completed"], 5);
    server.shutdown();

    // Deletions are persisted: a restart does not resurrect any of the three.
    let server = serve_batch(slow_config(dir.clone())).expect("re-serve");
    let list: Value = client
        .get(format!("{}/v1/files", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed_ids(&list).is_empty(), "{list}");
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── mid-run results (forge extension) ────────────────────────────────────────

/// A GET of `/results` from `cursor`: (status header, next cursor, custom_ids served).
async fn read_results(
    client: &reqwest::Client,
    base: &str,
    batch: &str,
    query: &str,
) -> (String, u64, Vec<String>) {
    let r = client
        .get(format!("{base}/v1/batches/{batch}/results?{query}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{query}");
    assert_eq!(r.headers()["content-type"], "application/jsonl");
    let status = r.headers()["x-forge-batch-status"]
        .to_str()
        .unwrap()
        .to_string();
    let next: u64 = r.headers()["x-forge-next-after"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let ids = r
        .text()
        .await
        .unwrap()
        .lines()
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            // The same OpenAI output-line shape the finished output file uses.
            assert!(v["id"].as_str().unwrap().starts_with("batch_req_"));
            v["custom_id"].as_str().unwrap().to_string()
        })
        .collect();
    (status, next, ids)
}

/// On a large batch nothing used to be readable until the last item landed: the
/// OpenAI contract only stamps `output_file_id` at completion. `/results` serves what
/// is already written, and a cursor lets a client tail it without re-downloading.
#[tokio::test]
async fn results_can_be_tailed_mid_run_and_add_up_to_the_output_file() {
    let dir = tmp_dir("tail");
    let server = serve_batch(slow_config(dir.clone())).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    let n = 6;
    let input = upload(
        &client,
        &base,
        (0..n)
            .map(|i| input_line(&format!("t-{i}")))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await;
    let batch = create(&client, &base, &input).await;

    // Tail until the batch is terminal AND a read comes back empty — the documented
    // "you have everything" condition.
    let mut cursor = 0u64;
    let mut tailed: Vec<String> = Vec::new();
    let mut read_while_running = 0usize;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, next, ids) =
            read_results(&client, &base, &batch, &format!("after={cursor}")).await;
        assert!(next >= cursor, "the cursor never moves backwards");
        if status == "in_progress" {
            read_while_running += ids.len();
        }
        let empty = ids.is_empty();
        tailed.extend(ids);
        cursor = next;
        if status == "completed" && empty {
            break;
        }
        assert!(Instant::now() < deadline, "tail never finished: {tailed:?}");
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    assert!(
        read_while_running > 0,
        "results were readable before the batch finished"
    );

    // Every result exactly once: a cursor never re-sends a line…
    let mut unique = tailed.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        tailed.len(),
        "duplicates in the tail: {tailed:?}"
    );
    // …and the tail is the finished output file, by custom_id.
    let done = wait_for_status(&client, &base, &batch, "completed").await;
    let output = done["output_file_id"].as_str().unwrap().to_string();
    let mut final_ids: Vec<String> = client
        .get(format!("{base}/v1/files/{output}/content"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["custom_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    final_ids.sort();
    assert_eq!(unique, final_ids);
    assert_eq!(unique.len(), n);

    // A clean batch's error stream exists and is empty.
    let (_, next, ids) = read_results(&client, &base, &batch, "kind=error").await;
    assert_eq!((next, ids.len()), (0, 0));

    // Requests that could only start mid-record, or that name nothing, are refused.
    for (query, why) in [
        ("kind=bogus", "unknown kind"),
        ("after=abc", "not a number"),
        ("after=1", "mid-line"),
        ("after=99999999", "past the end"),
    ] {
        let r = client
            .get(format!("{base}/v1/batches/{batch}/results?{query}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{why}");
    }
    let r = client
        .get(format!("{base}/v1/batches/batch-nope/results"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // Deleting the output file deletes the results behind `/results` too: no copy is
    // left for the API to keep serving after the customer asked for it to be gone.
    let r = client
        .delete(format!("{base}/v1/files/{output}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let (_, next, ids) = read_results(&client, &base, &batch, "").await;
    assert_eq!((next, ids.len()), (0, 0));
    let r = client
        .get(format!("{base}/v1/batches/{batch}/results?after={cursor}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        400,
        "an old cursor now points past the (empty) end"
    );

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── review fixes: paging, cancel, the create/delete race ─────────────────────

/// GET `path` and return the JSON body.
async fn get_json(client: &reqwest::Client, url: String) -> Value {
    let r = client.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 200, "{url}");
    r.json().await.unwrap()
}

/// The OpenAI SDKs page a list by asking again with `?after=<last_id>` until
/// `has_more` is false. With no `has_more`, and `after` ignored, `for f in
/// client.files.list()` never ended: every page was the whole list again.
#[tokio::test]
async fn lists_page_the_way_the_openai_sdks_iterate_them() {
    let dir = tmp_dir("paging");
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();

    for i in 0..5 {
        upload(&client, &base, input_line(&format!("p-{i}"))).await;
    }

    // A plain GET lists everything, and says so.
    let all = get_json(&client, format!("{base}/v1/files")).await;
    let every = listed_ids(&all);
    assert_eq!(every.len(), 5);
    assert_eq!(all["has_more"], false);
    assert_eq!(all["first_id"], every[0].as_str());
    assert_eq!(all["last_id"], every[4].as_str());

    // Iterate exactly as the SDK does: same order, nothing twice, and it ends.
    let mut seen = Vec::new();
    let mut url = format!("{base}/v1/files?limit=2");
    for page_no in 1.. {
        assert!(page_no <= 3, "five files in pages of two take three pages");
        let page = get_json(&client, url).await;
        seen.extend(listed_ids(&page));
        if page["has_more"] == false {
            break;
        }
        url = format!(
            "{base}/v1/files?limit=2&after={}",
            page["last_id"].as_str().unwrap()
        );
    }
    assert_eq!(seen, every);

    // Past the last item: an empty page that ends the iteration.
    let tail = get_json(&client, format!("{base}/v1/files?after={}", every[4])).await;
    assert!(listed_ids(&tail).is_empty());
    assert_eq!(
        (tail["has_more"].clone(), tail["last_id"].clone()),
        (Value::Bool(false), Value::Null)
    );

    // A cursor naming nothing, or a limit that is not a positive integer, is refused.
    for query in ["after=file-gone", "limit=0", "limit=two"] {
        let r = client
            .get(format!("{base}/v1/files?{query}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{query}");
    }

    // Batches page the same way.
    let input = upload(&client, &base, input_line("b")).await;
    for _ in 0..3 {
        create(&client, &base, &input).await;
    }
    let first = get_json(&client, format!("{base}/v1/batches?limit=2")).await;
    assert_eq!(
        (listed_ids(&first).len(), first["has_more"].clone()),
        (2, Value::Bool(true))
    );
    let rest = get_json(
        &client,
        format!(
            "{base}/v1/batches?limit=2&after={}",
            first["last_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(
        (listed_ids(&rest).len(), rest["has_more"].clone()),
        (1, Value::Bool(false))
    );
    let all = get_json(&client, format!("{base}/v1/batches")).await;
    assert_eq!(
        (listed_ids(&all).len(), all["has_more"].clone()),
        (3, Value::Bool(false))
    );

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `/results` is the only way to read a cancelled batch's results, and its readers stop
/// once the status is terminal and a read comes back empty. Cancel used to report
/// `cancelled` at once while the run went on writing, so results landing afterwards
/// were lost to them. The batch is now `cancelling` until nothing of the run is left.
#[tokio::test]
async fn a_cancelled_batch_writes_nothing_once_it_reports_cancelled() {
    let dir = tmp_dir("cancel-tail");
    let client = reqwest::Client::new();
    let batch = {
        let server = serve_batch(slow_config(dir.clone())).expect("serve");
        let base = server.base_url();
        let input = upload(
            &client,
            &base,
            (0..10)
                .map(|i| input_line(&format!("c-{i}")))
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .await;
        let batch = create(&client, &base, &input).await;

        // Cancel mid-run, once a result has landed.
        let deadline = Instant::now() + Duration::from_secs(10);
        while read_results(&client, &base, &batch, "").await.2.is_empty() {
            assert!(Instant::now() < deadline, "no result ever landed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let cancel: Value = client
            .post(format!("{base}/v1/batches/{batch}/cancel"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            cancel["status"] == "cancelling" || cancel["status"] == "cancelled",
            "{cancel}"
        );
        let done = wait_for_status(&client, &base, &batch, "cancelled").await;
        assert!(done["cancelled_at"].is_i64(), "{done}");

        // Read everything there is, the documented way...
        let (mut cursor, mut got) = (0u64, 0usize);
        loop {
            let (status, next, ids) =
                read_results(&client, &base, &batch, &format!("after={cursor}")).await;
            assert_eq!(status, "cancelled");
            cursor = next;
            if ids.is_empty() {
                break;
            }
            got += ids.len();
        }
        assert!(
            (1..10).contains(&got),
            "a cancel mid-run keeps a part: {got}"
        );
        // ...and nothing arrives after that, even once an item's time has passed.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let (_, next, ids) = read_results(&client, &base, &batch, &format!("after={cursor}")).await;
        assert_eq!(
            (next, ids.len()),
            (cursor, 0),
            "a result landed after `cancelled`"
        );
        server.shutdown();
        batch
    };

    // A process that died while cancelling leaves `cancelling` on disk. Nothing will
    // run that batch again, so the next process finishes its cancel.
    let meta = dir.join("batches").join(&batch).join("batch.json");
    let mut record: Value = serde_json::from_slice(&std::fs::read(&meta).unwrap()).unwrap();
    record["status"] = "cancelling".into();
    record["cancelled_at"] = Value::Null;
    std::fs::write(&meta, serde_json::to_vec(&record).unwrap()).unwrap();
    let server = serve_batch(slow_config(dir.clone())).expect("re-serve");
    let b = get_json(&client, format!("{}/v1/batches/{batch}", server.base_url())).await;
    assert_eq!(b["status"], "cancelled", "{b}");
    assert!(b["cancelled_at"].is_i64(), "{b}");
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch used to be registered in a second hold of the lock after its input was
/// looked up, so a `DELETE` of the input landing in between found no in-progress batch
/// and deleted the file the run was about to hydrate from: both calls answered 200 and
/// the batch then failed.
#[tokio::test]
async fn deleting_an_input_never_races_a_batch_being_created_from_it() {
    let dir = tmp_dir("create-delete");
    let server = serve_batch(slow_config(dir.clone())).expect("serve");
    let base = server.base_url();
    let client = reqwest::Client::new();
    // Five slow items keep every batch in progress for the whole test.
    let body = (0..5)
        .map(|i| input_line(&format!("r-{i}")))
        .collect::<Vec<_>>()
        .join("\n");

    for _ in 0..25 {
        let input = upload(&client, &base, body.clone()).await;
        let (created, deleted) = tokio::join!(
            client
                .post(format!("{base}/v1/batches"))
                .json(&serde_json::json!({
                    "input_file_id": input, "endpoint": "/v1/chat/completions"
                }))
                .send(),
            client.delete(format!("{base}/v1/files/{input}")).send(),
        );
        let (created, deleted) = (created.unwrap().status(), deleted.unwrap().status());
        assert!(
            matches!(
                (created.as_u16(), deleted.as_u16()),
                (200, 409) | (404, 200)
            ),
            "create {created}, delete {deleted}: exactly one of them must win"
        );
    }

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `/results` reads a batch's raw results directly, so deleting the output file used to
/// stop it serving them only once their bytes were gone. A crash between the registry
/// write and the unlink, or a failed unlink, left them served after the customer had
/// deleted them, and across restarts. A tombstone on the batch now goes first.
#[tokio::test]
async fn deleted_results_stay_deleted_even_if_their_bytes_survive() {
    let dir = tmp_dir("delete-tombstone");
    let client = reqwest::Client::new();
    let server = serve_batch(config(dir.clone(), None)).expect("serve");
    let base = server.base_url();
    let input = upload(
        &client,
        &base,
        (0..3)
            .map(|i| input_line(&format!("d-{i}")))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await;
    let batch = create(&client, &base, &input).await;
    let done = wait_for_status(&client, &base, &batch, "completed").await;
    let output = done["output_file_id"].as_str().unwrap().to_string();
    let raw = dir.join("batches").join(&batch).join("out.jsonl");
    let bytes = std::fs::read(&raw).unwrap();

    let r = client
        .delete(format!("{base}/v1/files/{output}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // As if the unlink had failed, or the process had died before it.
    std::fs::write(&raw, &bytes).unwrap();
    let (_, next, ids) = read_results(&client, &base, &batch, "").await;
    assert_eq!((next, ids.len()), (0, 0), "deleted results were served");
    server.shutdown();

    // Across a restart too, and startup removes the bytes that were left behind.
    let server = serve_batch(config(dir.clone(), None)).expect("re-serve");
    let (_, next, ids) = read_results(&client, &server.base_url(), &batch, "").await;
    assert_eq!(
        (next, ids.len()),
        (0, 0),
        "deleted results were served after a restart"
    );
    assert!(!raw.exists(), "startup removes the deleted results' bytes");
    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
