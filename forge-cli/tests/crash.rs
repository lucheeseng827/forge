//! Crash/kill integration: SIGKILL the coordinator mid-run, then `forge resume`,
//! and prove the headline guarantee — **zero duplicate and zero missing** results.
//!
//! Drives the real `forge` binary as a subprocess against an in-process wiremock
//! engine (a real localhost server the subprocess reaches over HTTP). The mock
//! delays each POST so the run is genuinely mid-flight when killed.

use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const N: usize = 60;

fn write_input(path: &std::path::Path) {
    let mut s = String::new();
    for i in 0..N {
        s.push_str(&format!(
            "{{\"custom_id\":\"req-{i:04}\",\"url\":\"/v1/chat/completions\",\
             \"body\":{{\"model\":\"m\",\"messages\":[{{\"role\":\"user\",\"content\":\"hi\"}}]}}}}\n"
        ));
    }
    std::fs::write(path, s).unwrap();
}

async fn status_json(checkpoint: &std::path::Path) -> Value {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["status", "--checkpoint"])
        .arg(checkpoint)
        .arg("--json")
        .output()
        .await
        .expect("status");
    serde_json::from_slice(&out.stdout).expect("parse status json")
}

/// Count the parseable result lines in the output file so far. Reading the append-only
/// results file is the safe way to watch the run's progress mid-flight: unlike spawning
/// `forge status`, it never opens the checkpoint DB and so can't race `forge run`'s
/// concurrent SQLite setup/ingest at startup.
fn results_written(out: &std::path::Path) -> usize {
    std::fs::read_to_string(out)
        .map(|c| {
            c.lines()
                .filter(|l| serde_json::from_str::<Value>(l).is_ok())
                .count()
        })
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_sigkill_then_resume_is_exactly_once() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // Each generation takes 150ms, so 60 items across 4 in-flight ≈ 2.2s — plenty of
    // wall-clock to kill the coordinator mid-run.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}
                }))
                .set_delay(Duration::from_millis(150)),
        )
        .mount(&server)
        .await;

    let dir = std::env::temp_dir().join(format!("forge-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("in.jsonl");
    let out = dir.join("out.jsonl");
    let checkpoint = dir.join("state.db");
    write_input(&input);

    // Spawn `forge run` and SIGKILL it mid-flight.
    let mut run = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["run", "--input"])
        .arg(&input)
        .arg("--out")
        .arg(&out)
        .arg("--checkpoint")
        .arg(&checkpoint)
        .args([
            "--workers",
            &server.uri(),
            "--concurrency",
            "4",
            "--lease-secs",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn forge run");

    // Let the run make progress, then SIGKILL it while it is still in flight — a hard
    // coordinator crash with no cleanup. We watch the output file for the first result
    // rather than sleeping a fixed amount (racy on a loaded box) or polling `forge
    // status` (which would open the checkpoint DB and race the run's own SQLite setup
    // at startup — the reason an earlier version saw an empty, un-ingested queue). If
    // the run happens to drain entirely before we observe a result (rare), that is fine
    // too: `resume` then exercises the idempotent no-op path and the zero-dup /
    // zero-missing assertions below still hold.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if results_written(&out) > 0 {
            break; // genuinely mid-flight: a result landed, most items still pending
        }
        if run.try_wait().unwrap().is_some() {
            break; // the run finished on its own before we caught it (rare)
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run wrote no results within 60s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = run.kill().await; // SIGKILL — harmless no-op if it already exited
    let _ = run.wait().await;

    // Resume to completion (reuses the recorded --out). `resume` is idempotent by
    // design — the exact property this test proves — so a transient non-zero exit
    // (e.g. the in-process mock briefly starved on an oversubscribed CI box) is simply
    // retried, as an operator would. Each attempt's timeout only catches a true hang;
    // it is generous so a CPU-starved box (each item is a 150ms mock gen) making slow
    // progress doesn't false-timeout.
    // `--concurrency 8` keeps resume from blasting the in-process mock with hundreds of
    // simultaneous requests (the default is 256); a gentler fan-out drains the ~55
    // remaining items in ~1s and is far less likely to starve the mock on a busy box.
    let mut resumed = false;
    let mut last_err = String::new();
    let mut last_out = String::new();
    for _ in 0..5 {
        let output = tokio::time::timeout(
            Duration::from_secs(180),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
                .args(["resume", "--checkpoint"])
                .arg(&checkpoint)
                // Pass --out explicitly: a SIGKILL can land before `run` durably records
                // the job's output path, so resume can't always auto-recover it (it says
                // "pass --out"). It equals the recorded path, so resolve_resume_out uses
                // the same file whether or not the metadata survived.
                .arg("--out")
                .arg(&out)
                .args([
                    "--workers",
                    &server.uri(),
                    "--lease-secs",
                    "30",
                    "--concurrency",
                    "8",
                    // More retries so a momentarily CPU-starved in-process mock (on a
                    // busy CI box) is re-tried rather than dead-lettering the item.
                    "--max-attempts",
                    "25",
                ])
                .output(),
        )
        .await
        .expect("resume did not hang")
        .expect("resume ran");
        last_out = String::from_utf8_lossy(&output.stdout).into_owned();
        if output.status.success() {
            resumed = true;
            break;
        }
        last_err = String::from_utf8_lossy(&output.stderr).into_owned();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        resumed,
        "resume did not succeed within 5 attempts; last stderr:\n{last_err}"
    );

    // The queue is fully drained, all done, none dead.
    let end = status_json(&checkpoint).await;
    assert_eq!(
        end["done"].as_u64().unwrap(),
        N as u64,
        "every item done; status={end}; last resume stdout={last_out:?}"
    );
    assert_eq!(end["dead"].as_u64().unwrap(), 0, "no dead items");
    assert_eq!(end["pending"].as_u64().unwrap(), 0);
    assert_eq!(end["leased"].as_u64().unwrap(), 0);

    // The output JSONL has exactly one PARSEABLE line per custom_id — zero
    // duplicate, zero missing. (A crash-torn fragment, if any, is an unparseable
    // line that does not count and did not corrupt a real record.)
    let content = std::fs::read_to_string(&out).unwrap();
    let mut ids: Vec<String> = content
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| {
            v.get("custom_id")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .collect();
    let total = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        N,
        "every custom_id present exactly once (zero missing)"
    );
    assert_eq!(total, N, "no duplicate result lines (zero duplicate)");

    let _ = std::fs::remove_dir_all(&dir);
}
