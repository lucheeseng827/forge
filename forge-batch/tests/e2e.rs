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
