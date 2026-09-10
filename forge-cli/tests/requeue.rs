//! The dead-letter return leg, end to end against the real binary and a real engine.
//!
//! The reason this file exists as an integration test rather than a unit test: the
//! failure it guards is invisible at every individual seam. The queue transition looks
//! right, the re-run looks right, the engine really is called and really does return a
//! result — and then `JsonlStore::put` short-circuits on an emitted-id set that was
//! seeded from the dead-letter file, and the result is dropped on the floor. Nothing
//! errors. The operator sees a successful run and a results file that is still missing
//! the items they just paid to re-run.
//!
//! So the assertion that matters is the last one in
//! `a_requeued_item_actually_lands_in_the_results_file`: the id is IN `<out>` after the
//! second run. Everything else is scaffolding for it.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use forge_core::{
    BatchRun, EndpointKind, Item, ItemState, Queue, RetryPolicy, RunConfig, WorkerSpec,
};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn forge_bin() -> PathBuf {
    let mut p = std::env::current_exe().expect("current exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    p.join("forge")
}

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("forge-requeue-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).expect("mkdir");
    d
}

struct Run {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn forge(args: &[&str]) -> Run {
    let out = Command::new(forge_bin())
        .args(args)
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("running forge");
    Run {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn item(i: usize) -> Item {
    Item {
        custom_id: format!("req-{i}"),
        method: "POST".into(),
        url: "/v1/chat/completions".into(),
        body: json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
        status: ItemState::Pending,
        attempts: 0,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

fn fast_cfg() -> RunConfig {
    RunConfig {
        lease_for: Duration::from_secs(30),
        lease_max: Duration::from_secs(30),
        lease_batch: 16,
        poll_interval: Duration::from_millis(10),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy {
            max_attempts: 2,
            base: Duration::from_millis(1),
            cap: Duration::from_millis(2),
        },
        load_aware: true,
        ..RunConfig::default()
    }
}

/// An engine that fails every inference request with a retryable 500.
async fn broken_engine() -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("engine fell over"))
        .mount(&s)
        .await;
    s
}

/// The same engine after somebody fixed it.
async fn healthy_engine() -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
        })))
        .mount(&s)
        .await;
    s
}

async fn run_against(ckpt: &Path, out: &Path, uri: String, n: usize, fresh: bool) -> u64 {
    let queue = SqliteQueue::open(ckpt).unwrap();
    if fresh {
        let items: Vec<Item> = (0..n).map(item).collect();
        queue.enqueue(&items).await.unwrap();
    }
    let store = JsonlStore::new(out);
    let worker =
        HttpWorker::new(WorkerSpec::new("w0", uri, EndpointKind::Chat).concurrency(4)).unwrap();
    let totals = BatchRun::new(queue, vec![worker], store)
        .with_config(fast_cfg())
        .run()
        .await
        .unwrap();
    totals.items_done
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeued_item_actually_lands_in_the_results_file() {
    let d = tmp("roundtrip");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let dead = d.join("results.jsonl.dead.jsonl");

    // 1) The engine is down. Every item dead-letters.
    let broken = broken_engine().await;
    run_against(&ckpt, &out, broken.uri(), 3, true).await;
    drop(broken);
    assert_eq!(
        fs::read_to_string(&dead).unwrap().lines().count(),
        3,
        "all three should be dead-lettered"
    );
    assert!(!out.exists() || fs::read_to_string(&out).unwrap().trim().is_empty());

    // 2) Somebody fixes the engine and requeues.
    let r = forge(&[
        "requeue",
        "--checkpoint",
        ckpt.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--all",
    ]);
    assert!(r.ok, "requeue failed: {}{}", r.stdout, r.stderr);
    assert!(r.stdout.contains("requeued 3"), "{}", r.stdout);
    assert!(r.stdout.contains("pruned 3"), "{}", r.stdout);
    assert_eq!(
        fs::read_to_string(&dead).unwrap().trim(),
        "",
        "the dead records must be gone, or the re-run's results get discarded"
    );

    // 3) Re-run on the SAME checkpoint against the fixed engine.
    let healthy = healthy_engine().await;
    let done = run_against(&ckpt, &out, healthy.uri(), 3, false).await;
    assert_eq!(done, 3, "all three ran again");

    // THE assertion. Without the prune this file is still empty: the items re-run,
    // burn real GPU time, and `put` silently drops every result.
    let content = fs::read_to_string(&out).unwrap();
    assert_eq!(
        content.lines().count(),
        3,
        "the requeued results must be IN the results file, not silently discarded"
    );
    for i in 0..3 {
        assert!(content.contains(&format!("req-{i}")), "missing req-{i}");
    }
    let _ = fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requeue_by_reason_leaves_the_unfixable_items_quarantined() {
    // The operator's real decision: a 500 is worth retrying once the engine is fixed;
    // a 4xx is a bad request that will fail identically forever. Requeueing everything
    // would burn GPU minutes re-proving the second half.
    let d = tmp("byreason");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let dead = d.join("results.jsonl.dead.jsonl");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // req-0 is a bad request (400, terminal); the rest hit a broken engine (500).
    Mock::given(method("POST"))
        .and(wiremock::matchers::body_string_contains("\"req-0\""))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("engine fell over"))
        .mount(&server)
        .await;

    {
        let queue = SqliteQueue::open(&ckpt).unwrap();
        let mut items: Vec<Item> = (0..3).map(item).collect();
        // Put the id in the body so the mock can route on it.
        for (i, it) in items.iter_mut().enumerate() {
            it.body = json!({"model": "m", "tag": format!("req-{i}")});
        }
        queue.enqueue(&items).await.unwrap();
        let store = JsonlStore::new(&out);
        let worker =
            HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(2))
                .unwrap();
        BatchRun::new(queue, vec![worker], store)
            .with_config(fast_cfg())
            .run()
            .await
            .unwrap();
    }
    let before = fs::read_to_string(&dead).unwrap();
    assert_eq!(before.lines().count(), 3);

    // Requeue only what a fixed engine could plausibly help.
    let r = forge(&[
        "requeue",
        "--checkpoint",
        ckpt.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--reason",
        "500",
    ]);
    assert!(r.ok, "{}{}", r.stdout, r.stderr);

    let q = SqliteQueue::open(&ckpt).unwrap();
    let c = q.counts().await.unwrap();
    assert!(c.pending > 0, "the 500s came back: {c:?}");
    assert!(
        c.dead > 0,
        "the 400 must stay quarantined — retrying it cannot help: {c:?}"
    );
    // And the dead file still carries exactly the ones left dead.
    let after = fs::read_to_string(&dead).unwrap();
    assert_eq!(after.lines().count() as u64, c.dead);
    let _ = fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requeue_refuses_while_a_coordinator_holds_live_leases() {
    // A running coordinator cached the emitted-id set at startup. Pruning the file
    // underneath it reaches nothing, so every requeued item's result would still be
    // discarded — the same silent loss, made invisible by appearing to succeed.
    let d = tmp("live");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");

    let q = SqliteQueue::open(&ckpt).unwrap();
    q.enqueue(&[item(0), item(1)]).await.unwrap();
    let leased = q.lease(1, Duration::from_secs(600)).await.unwrap();
    q.dead_letter(
        &leased[0].custom_id,
        leased[0].attempts,
        "retries exhausted: HTTP 500",
    )
    .await
    .unwrap();
    // A second item is leased and STILL HELD — a live coordinator.
    let held = q.lease(1, Duration::from_secs(600)).await.unwrap();
    assert_eq!(held.len(), 1);
    drop(q);

    let r = forge(&[
        "requeue",
        "--checkpoint",
        ckpt.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--all",
    ]);
    assert!(!r.ok, "must refuse: {}", r.stdout);
    assert!(
        r.stderr.contains("coordinator is running"),
        "name the reason: {}",
        r.stderr
    );
    assert!(r.stderr.contains("live lease"), "{}", r.stderr);

    // Nothing moved.
    let q = SqliteQueue::open(&ckpt).unwrap();
    assert_eq!(
        q.counts().await.unwrap().dead,
        1,
        "the refusal changed nothing"
    );
    let _ = fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dry_run_changes_nothing() {
    let d = tmp("dryrun");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let dead = d.join("results.jsonl.dead.jsonl");

    let broken = broken_engine().await;
    run_against(&ckpt, &out, broken.uri(), 2, true).await;
    drop(broken);
    let before = fs::read_to_string(&dead).unwrap();

    let r = forge(&[
        "requeue",
        "--checkpoint",
        ckpt.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--all",
        "--dry-run",
    ]);
    assert!(r.ok, "{}{}", r.stdout, r.stderr);
    assert!(r.stdout.contains("would requeue 2"), "{}", r.stdout);
    assert_eq!(fs::read_to_string(&dead).unwrap(), before, "file untouched");

    let q = SqliteQueue::open(&ckpt).unwrap();
    assert_eq!(q.counts().await.unwrap().dead, 2, "queue untouched");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn requeue_never_guesses_at_scope() {
    // It reverses a terminal state. Defaulting to --all when the operator forgot a flag
    // would re-run a whole job's worth of unfixable 4xx items on a real GPU.
    let d = tmp("noscope");
    let r = forge(&[
        "requeue",
        "--checkpoint",
        d.join("state.db").to_str().unwrap(),
        "--out",
        d.join("results.jsonl").to_str().unwrap(),
    ]);
    assert!(!r.ok);
    assert!(r.stderr.contains("nothing selected"), "{}", r.stderr);
    let _ = fs::remove_dir_all(&d);
}
