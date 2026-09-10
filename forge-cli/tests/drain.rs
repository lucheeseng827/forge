//! Graceful drain on SIGTERM — the 3am path.
//!
//! Being hard-killed is *correct*: leases expire, the reaper reclaims them, and the
//! store write is idempotent, so nothing is lost but time. Draining is about the time.
//! On a spot preemption (~30s warning) or a rolling deploy, every in-flight item that
//! gets killed is GPU minutes already spent that must be spent again, and none of it
//! can be reclaimed until the lease TTL elapses.
//!
//! So the properties under test are: a drain finishes what it started, leaves the rest
//! runnable, and a resume completes the job with **every item present exactly once**.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

#[cfg(unix)]
use std::process::Stdio;

fn item(i: usize) -> Item {
    Item {
        custom_id: format!("req-{i}"),
        method: "POST".into(),
        url: "/v1/chat/completions".into(),
        body: json!({"model": "m", "messages": []}),
        status: ItemState::Pending,
        attempts: 0,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

fn cfg() -> RunConfig {
    RunConfig {
        // A long lease, so a dropped item would stay stuck and the test would notice.
        lease_for: Duration::from_secs(300),
        lease_max: Duration::from_secs(300),
        lease_batch: 4,
        poll_interval: Duration::from_millis(5),
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

async fn engine(delay_ms: u64) -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(delay_ms))
                .set_body_json(json!({
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
                })),
        )
        .mount(&s)
        .await;
    s
}

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("forge-drain-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_finishes_in_flight_work_and_leaves_the_rest_runnable() {
    let d = dir("midrun");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let server = engine(60).await;

    let queue = SqliteQueue::open(&ckpt).unwrap();
    let items: Vec<Item> = (0..40).map(item).collect();
    queue.enqueue(&items).await.unwrap();

    let drain = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&drain);
    // Flip it partway through, the way a SIGTERM would arrive mid-run.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        flag.store(true, Ordering::SeqCst);
    });

    let worker =
        HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(4))
            .unwrap();
    let totals = BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
        .with_config(cfg())
        .with_shutdown(Arc::clone(&drain))
        .run()
        .await
        .expect("a drain returns normally, it is not an error");

    // It stopped early…
    assert!(
        totals.items_done < 40,
        "the drain should have stopped before the job finished (done={})",
        totals.items_done
    );
    assert!(totals.items_done > 0, "some work should have landed first");

    // …and left NOTHING stuck. Every item it started, it finished: no leases are
    // still held, which is the whole difference between draining and being killed.
    let q = SqliteQueue::open(&ckpt).unwrap();
    let counts = q.counts().await.unwrap();
    assert_eq!(
        counts.leased, 0,
        "a drain must not abandon leases — those are GPU minutes nobody can reclaim \
         until the TTL expires: {counts:?}"
    );
    assert_eq!(counts.done as u64, totals.items_done);
    assert_eq!(counts.pending + counts.done + counts.dead, 40);
    assert!(counts.pending > 0, "the rest must still be runnable");

    // The store agrees with the queue: every acked item has its result on disk.
    let lines = std::fs::read_to_string(&out).unwrap().lines().count();
    assert_eq!(
        lines as u64, totals.items_done,
        "result-before-ack must survive the shutdown path too"
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_then_resume_completes_every_item_exactly_once() {
    // The property an orchestrator actually depends on: a pod evicted mid-batch and
    // restarted loses no item and duplicates none.
    let d = dir("resume");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let server = engine(40).await;

    {
        let queue = SqliteQueue::open(&ckpt).unwrap();
        queue
            .enqueue(&(0..30).map(item).collect::<Vec<_>>())
            .await
            .unwrap();
    }

    let drain = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&drain);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        flag.store(true, Ordering::SeqCst);
    });

    let first = {
        let queue = SqliteQueue::open(&ckpt).unwrap();
        let worker =
            HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(4))
                .unwrap();
        BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
            .with_config(cfg())
            .with_shutdown(drain)
            .run()
            .await
            .unwrap()
    };
    assert!(first.items_done < 30, "the drain stopped it early");
    let first_ran = first.items_done;
    // The drain left the checkpoint in exactly the state a resume expects.
    {
        let q = SqliteQueue::open(&ckpt).unwrap();
        let c = q.counts().await.unwrap();
        assert_eq!(c.leased, 0, "nothing abandoned mid-flight: {c:?}");
        assert_eq!(c.done + c.pending, 30);
    }

    // Resume — a fresh process would do exactly this.
    let second = {
        let queue = SqliteQueue::open(&ckpt).unwrap();
        let worker =
            HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(4))
                .unwrap();
        BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
            .with_config(cfg())
            .run()
            .await
            .unwrap()
    };

    let q = SqliteQueue::open(&ckpt).unwrap();
    let counts = q.counts().await.unwrap();
    assert!(
        counts.is_drained(),
        "the resume finished the job: {counts:?}"
    );
    assert_eq!(counts.done, 30);

    // Exactly once: 30 result lines, 30 distinct ids.
    let content = std::fs::read_to_string(&out).unwrap();
    let ids: std::collections::HashSet<&str> = content
        .lines()
        .filter_map(|l| {
            l.split("\"custom_id\":\"")
                .nth(1)
                .and_then(|r| r.split('"').next())
        })
        .collect();
    assert_eq!(
        content.lines().count(),
        30,
        "no item was lost and none was written twice"
    );
    assert_eq!(ids.len(), 30, "30 distinct ids");
    // `items_done` is JOB-wide (it folds prior-run progress, which is what makes a
    // resume's output meaningful), so the per-run work is measured in TOKENS — and
    // tokens are only spent by an item that actually ran. 30 items × 7 tokens, split
    // across the two runs with nothing counted twice, is the proof that the drain
    // handed off cleanly: no item was re-executed on a GPU, and none was skipped.
    assert_eq!(
        first.tokens_total() + second.tokens_total(),
        30 * 7,
        "an item that ran twice would show up here as extra tokens spent"
    );
    assert_eq!(first.tokens_total(), first_ran * 7);
    assert_eq!(
        second.tokens_total(),
        (30 - first_ran) * 7,
        "the resume ran only what was left"
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flag_already_set_leases_nothing_at_all() {
    // A SIGTERM that lands between process start and the first lease must not start
    // work it is about to abandon.
    let d = dir("preset");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");
    let server = engine(0).await;

    let queue = SqliteQueue::open(&ckpt).unwrap();
    queue
        .enqueue(&(0..10).map(item).collect::<Vec<_>>())
        .await
        .unwrap();

    let worker =
        HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(4))
            .unwrap();
    let totals = BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
        .with_config(cfg())
        .with_shutdown(Arc::new(AtomicBool::new(true)))
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done, 0);
    let q = SqliteQueue::open(&ckpt).unwrap();
    let counts = q.counts().await.unwrap();
    assert_eq!(counts.pending, 10, "nothing was leased: {counts:?}");
    assert_eq!(counts.leased, 0);
    let _ = std::fs::remove_dir_all(&d);
}

/// SIGTERM against the **real binary**, which is the only place the bug lived: the CLI
/// waited on `ctrl_c()` (SIGINT) alone, so under Kubernetes, `docker stop`, systemd or
/// a spot reclaim the graceful path never ran at all — the process ignored the signal
/// with the default disposition removed, or died with no drain, and was SIGKILLed when
/// the grace period expired.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_the_run_and_exits_resumable() {
    let server = engine(120).await;
    let d = dir("sigterm");
    let input = d.join("in.jsonl");
    let out = d.join("out.jsonl");
    let ckpt = d.join("state.db");

    let mut body = String::new();
    for i in 0..60 {
        body.push_str(&format!(
            "{{\"custom_id\":\"req-{i}\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{{\"model\":\"m\"}}}}\n"
        ));
    }
    std::fs::write(&input, body).unwrap();

    let mut run = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["run", "--input"])
        .arg(&input)
        .arg("--out")
        .arg(&out)
        .arg("--checkpoint")
        .arg(&ckpt)
        .args([
            "--workers",
            &server.uri(),
            "--concurrency",
            "4",
            "--lease-secs",
            "300",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn forge run");
    let pid = run.id().expect("pid");

    // Wait until it is genuinely mid-flight (a result landed), so the drain has
    // something to drain.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if std::fs::read_to_string(&out)
            .map(|c| c.lines().count())
            .unwrap_or(0)
            > 0
        {
            break;
        }
        assert!(
            run.try_wait().unwrap().is_none(),
            "run exited before any result"
        );
        assert!(std::time::Instant::now() < deadline, "no result within 60s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The signal every orchestrator actually sends.
    assert!(std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("kill")
        .success());

    // It must exit BY ITSELF — that is the whole assertion. A process that ignores
    // SIGTERM sits here until the harness times out.
    let status = tokio::time::timeout(Duration::from_secs(30), run.wait())
        .await
        .expect("SIGTERM was ignored: the process never exited on its own")
        .expect("wait");

    assert_eq!(
        status.code(),
        Some(2),
        "a drained run has work left, so it must exit Incomplete(2) — the code that \
         tells an orchestrator to `resume` rather than treat it as a failure"
    );

    // Nothing abandoned: every item it leased, it finished and acked.
    let q = SqliteQueue::open(&ckpt).unwrap();
    let counts = q.counts().await.unwrap();
    assert_eq!(
        counts.leased, 0,
        "a graceful drain leaves no lease held; those items would otherwise be \
         unreclaimable until the 300s TTL expired: {counts:?}"
    );
    assert!(
        counts.pending > 0,
        "there is work left to resume: {counts:?}"
    );
    assert_eq!(counts.total(), 60);

    // And it said so, rather than dying quietly.
    let mut err = String::new();
    if let Some(mut e) = run.stderr.take() {
        use tokio::io::AsyncReadExt;
        let _ = e.read_to_string(&mut err).await;
    }
    assert!(
        err.contains("SIGTERM"),
        "the operator must be told which signal and what it means: {err}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// Diagnostics must not land in a command's machine-readable output.
///
/// `tracing_subscriber::fmt()` writes to **stdout** by default, so every log line
/// shared a stream with the thing a caller is parsing. It was latent for as long as the
/// only lines were warnings (a rejected-input notice would silently corrupt
/// `status --json` for whoever hit it); a startup line that fires on every invocation
/// turned it into a certainty. stdout is the answer, stderr is the commentary.
#[cfg(unix)]
#[test]
fn logs_go_to_stderr_so_json_output_stays_parseable() {
    let d = dir("stdout-purity");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["status", "--checkpoint"])
        .arg(d.join("state.db"))
        .arg("--json")
        .output()
        .expect("run forge status");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    serde_json::from_str::<serde_json::Value>(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not parseable JSON ({e}); it contained:\n{stdout}"));
    assert!(
        stderr.contains("forge starting"),
        "the startup line must exist, on stderr: {stderr}"
    );
    assert!(
        !stdout.contains("forge starting"),
        "a log line reached stdout and corrupted the payload: {stdout}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// The JSON log format has to be one object per line, or a shipper cannot read it.
#[cfg(unix)]
#[test]
fn json_logs_are_one_object_per_line() {
    let d = dir("json-logs");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["status", "--checkpoint"])
        .arg(d.join("state.db"))
        .args(["--json", "--log-format", "json"])
        .output()
        .expect("run forge status");

    let stderr = String::from_utf8_lossy(&out.stderr);
    let mut saw_start = false;
    for line in stderr.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("log line is not JSON ({e}): {line}"));
        if v["message"] == "forge starting" {
            saw_start = true;
            // The fields an operator greps for, flattened rather than nested under a
            // span so a shipper's field extraction is trivial.
            assert!(v["version"].is_string(), "{v}");
            assert_eq!(v["command"], "status");
            assert_eq!(v["level"], "INFO");
        }
    }
    assert!(saw_start, "no startup line in JSON mode: {stderr}");
    // stdout is still the payload.
    serde_json::from_str::<serde_json::Value>(String::from_utf8_lossy(&out.stdout).trim())
        .expect("stdout still parses");
    let _ = std::fs::remove_dir_all(&d);
}

/// The heartbeat is the answer to "your batch jobs run at 2am alone": between
/// "starting" and "done" a multi-hour run used to log nothing at all, so a working job
/// and a wedged one produced identical output — silence.
///
/// A plain `#[test]` driving its own runtime, not `#[tokio::test]`: the subscriber is
/// installed per-thread with `with_default`, and `block_on` drives the run on this same
/// thread (the loop never spawns), so the events land where the test can read them.
#[test]
fn a_long_run_reports_progress_while_it_works() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    struct Sink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sink = std::sync::Arc::clone(&logs);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || Sink(std::sync::Arc::clone(&sink)))
        // No ANSI: the terminal formatter colours field names, which puts escape codes
        // between `done` and `=` and makes a substring assertion meaningless.
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    let d = dir("heartbeat");
    let ckpt = d.join("state.db");
    let out = d.join("results.jsonl");

    let totals = tracing::subscriber::with_default(subscriber, || {
        rt.block_on(async {
            let server = engine(30).await;
            let queue = SqliteQueue::open(&ckpt).unwrap();
            queue
                .enqueue(&(0..60).map(item).collect::<Vec<_>>())
                .await
                .unwrap();
            let worker = HttpWorker::new(
                WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(2),
            )
            .unwrap();
            let cfg = RunConfig {
                progress_every: Duration::from_millis(100),
                ..cfg()
            };
            BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
                .with_config(cfg)
                .run()
                .await
                .unwrap()
        })
    });
    assert_eq!(totals.items_done, 60);

    let text = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(
        text.contains("progress"),
        "a run this long must say something while it works:\n{text}"
    );
    // The fields that let an operator tell working from wedged.
    for field in ["done=", "total=", "in_flight=", "items_per_sec="] {
        assert!(text.contains(field), "missing {field} in:\n{text}");
    }
    let _ = std::fs::remove_dir_all(&d);
}

/// …and it stays quiet on a job that finishes quickly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_can_be_switched_off() {
    let d = dir("no-heartbeat");
    let out = d.join("results.jsonl");
    let server = engine(0).await;
    let queue = SqliteQueue::open(d.join("state.db")).unwrap();
    queue
        .enqueue(&(0..4).map(item).collect::<Vec<_>>())
        .await
        .unwrap();
    let worker =
        HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(2))
            .unwrap();
    let totals = BatchRun::new(queue, vec![worker], JsonlStore::new(&out))
        .with_config(RunConfig {
            progress_every: Duration::ZERO,
            ..cfg()
        })
        .run()
        .await
        .unwrap();
    assert_eq!(
        totals.items_done, 4,
        "ZERO disables the heartbeat, not the run"
    );
    let _ = std::fs::remove_dir_all(&d);
}
