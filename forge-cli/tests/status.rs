//! `forge status --json` contract test: prepare a checkpoint through the library,
//! then invoke the real binary and assert the machine-readable report.

use std::process::Command;
use std::time::Duration;

use forge_core::{Item, ItemState, Queue};
use forge_queue::SqliteQueue;
use serde_json::Value;

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

#[tokio::test]
async fn status_json_reports_counts_success_rate_and_retries() {
    let dir = std::env::temp_dir().join(format!("forge-status-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let checkpoint = dir.join("state.db");

    // Prepare a checkpoint: 2 done at attempt 1, 1 dead at attempt 2.
    {
        let q = SqliteQueue::open(&checkpoint).unwrap();
        q.enqueue(&[item("a"), item("b"), item("poison")])
            .await
            .unwrap();
        let first = q.lease(2, Duration::from_secs(300)).await.unwrap();
        for it in &first {
            q.ack(&it.custom_id, it.attempts).await.unwrap();
        }
        q.lease(1, Duration::from_millis(0)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        q.reap().await.unwrap();
        let again = q.lease(1, Duration::from_secs(300)).await.unwrap();
        q.dead_letter("poison", again[0].attempts, "boom")
            .await
            .unwrap();
    } // drop → WAL flushed before the binary reopens the file

    let out = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["status", "--checkpoint"])
        .arg(&checkpoint)
        .arg("--json")
        .output()
        .expect("run forge status --json");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let report: Value = serde_json::from_slice(&out.stdout).expect("parse JSON report");
    assert_eq!(report["done"], 2);
    assert_eq!(report["dead"], 1);
    assert_eq!(report["total"], 3);
    assert_eq!(report["retried"], 1, "the poison item took 2 attempts");
    assert_eq!(report["success_rate"], 2.0 / 3.0);
    assert_eq!(report["attempts_histogram"]["1"], 2);
    assert_eq!(report["attempts_histogram"]["2"], 1);
    // The poison item's "boom" error classifies as "other".
    assert_eq!(report["failure_reasons"]["other"], 1);

    // Prometheus mode emits scrapeable exposition with the same numbers.
    let prom = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["status", "--checkpoint"])
        .arg(&checkpoint)
        .arg("--prometheus")
        .output()
        .expect("run forge status --prometheus");
    let text = String::from_utf8_lossy(&prom.stdout);
    assert!(text.contains("# TYPE forge_items gauge"), "{text}");
    assert!(text.contains("forge_items{state=\"done\"} 2"), "{text}");
    assert!(
        text.contains("forge_failures{reason=\"other\"} 1"),
        "{text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
