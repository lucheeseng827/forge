//! `forge run --out file://…/` writes each result as its own object and `resume`
//! reuses the recorded object-store URL as an idempotent no-op. The whole file compiles
//! only with `--features object_store` (which also builds the binary with the S3/GCS/
//! Azure sink), so the default `cargo test` run skips it; CI runs
//! `cargo test -p forge-cli --features object_store`.
#![cfg(feature = "object_store")]

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const N: usize = 8;

#[tokio::test]
async fn run_and_resume_to_object_store() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}
        })))
        .mount(&server)
        .await;

    let dir = std::env::temp_dir().join(format!("forge-objcli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("in.jsonl");
    let checkpoint = dir.join("state.db"); // → object job id "state"
    let out_root = dir.join("obj"); // the object store's local root
    let out_url = format!("file://{}", out_root.display());

    let mut lines = String::new();
    for i in 0..N {
        lines.push_str(&format!(
            "{{\"custom_id\":\"req-{i:04}\",\"url\":\"/v1/chat/completions\",\
             \"body\":{{\"model\":\"m\",\"messages\":[{{\"role\":\"user\",\"content\":\"hi\"}}]}}}}\n"
        ));
    }
    std::fs::write(&input, lines).unwrap();

    // Fan the batch into the object store.
    let run = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["run", "--input"])
        .arg(&input)
        .arg("--out")
        .arg(&out_url)
        .arg("--checkpoint")
        .arg(&checkpoint)
        .args(["--workers", &server.uri()])
        .output()
        .await
        .expect("spawn forge run");
    assert!(
        run.status.success(),
        "run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Every result is its own object under <root>/results/<job>/done/<custom_id>.json.
    let done_dir = out_root.join("results").join("state").join("done");
    let done_count = || {
        std::fs::read_dir(&done_dir)
            .map(|rd| rd.count())
            .unwrap_or(0)
    };
    assert_eq!(done_count(), N, "one done object per custom_id");

    // `resume` reuses the recorded object-store URL (no --out) and is an idempotent
    // no-op — it re-reads the O(emitted) manifest, skips every id, and writes nothing
    // new (still exactly N objects, zero duplicates).
    let resume = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["resume", "--checkpoint"])
        .arg(&checkpoint)
        .args(["--workers", &server.uri()])
        .output()
        .await
        .expect("spawn forge resume");
    assert!(
        resume.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );
    assert_eq!(done_count(), N, "resume added no duplicate objects");

    // `forge cost --results <url>` sums usage straight from the done objects (each mock
    // response reports 4 total tokens).
    let cost = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["cost", "--results"])
        .arg(&out_url)
        .arg("--json")
        .output()
        .await
        .expect("spawn forge cost");
    assert!(
        cost.status.success(),
        "cost failed: {}",
        String::from_utf8_lossy(&cost.stderr)
    );
    let cost_json: serde_json::Value = serde_json::from_slice(&cost.stdout).expect("cost json");
    assert_eq!(cost_json["items"].as_u64(), Some(N as u64));
    assert_eq!(cost_json["total_tokens"].as_u64(), Some((N * 4) as u64));

    // `forge verify` proves every input id has a terminal object (bounded-RAM sweep over
    // the dumped manifest — done AND dead ids).
    let verify = tokio::process::Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["verify", "--input"])
        .arg(&input)
        .arg("--results")
        .arg(&out_url)
        .arg("--json")
        .output()
        .await
        .expect("spawn forge verify");
    assert!(
        verify.status.success(),
        "verify failed: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let verify_json: serde_json::Value =
        serde_json::from_slice(&verify.stdout).expect("verify json");
    assert_eq!(verify_json["missing"].as_u64(), Some(0), "nothing missing");
    assert_eq!(verify_json["emitted_ids"].as_u64(), Some(N as u64));
    assert_eq!(verify_json["complete"].as_bool(), Some(true));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Hydrating from an object-store URL must be indistinguishable from a local file —
/// same ingest, same dedup, same `Shard` bookkeeping — because it IS the same code
/// path with a different reader. If these ever diverge, the object-store input is
/// quietly a second implementation.
#[cfg(feature = "object_store")]
mod object_input {
    use forge_core::Queue;
    use forge_queue::SqliteQueue;

    fn dir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("forge-objinput-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn input_body(n: usize) -> String {
        (0..n)
            .map(|i| {
                format!(
                    r#"{{"custom_id":"req-{i}","method":"POST","url":"/v1/chat/completions","body":{{"model":"m"}}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    }

    async fn ingest_from_url(queue: &SqliteQueue, url: &str, reject: &std::path::Path) -> u64 {
        let (offset, line) = queue.hydrated_through().await.unwrap().unwrap_or((0, 0));
        let (reader, start) = forge_store::open_input_stream(url, offset).await.unwrap();
        let start_line = if start == offset { line } else { 0 };
        forge_shard::ingest_reader_with_rejects(queue, reader, start, start_line, Some(reject))
            .await
            .unwrap()
            .hydrated
    }

    #[tokio::test]
    async fn hydrates_from_a_url_and_a_second_pass_seeks_instead_of_rescanning() {
        let d = dir("seek");
        let file = d.join("in.jsonl");
        std::fs::write(&file, input_body(50)).unwrap();
        let url = format!("file://{}", file.display());
        let reject = d.join("rej.jsonl");

        let queue = SqliteQueue::open(d.join("ck.db")).unwrap();
        assert_eq!(ingest_from_url(&queue, &url, &reject).await, 50);
        assert_eq!(queue.counts().await.unwrap().total(), 50);

        // The offset bookkeeping must have advanced, or a re-run would rescan 50M lines.
        let (offset, _) = queue
            .hydrated_through()
            .await
            .unwrap()
            .expect("a recorded shard");
        assert!(offset > 0, "ingest must record how far it got");

        // Second pass over the unchanged object: nothing new, and — the point — it
        // resumes from the recorded offset rather than re-reading the whole object.
        assert_eq!(ingest_from_url(&queue, &url, &reject).await, 0);
        assert_eq!(queue.counts().await.unwrap().total(), 50);

        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn a_replaced_shorter_object_rescans_rather_than_ingesting_nothing() {
        let d = dir("replaced");
        let file = d.join("in.jsonl");
        std::fs::write(&file, input_body(40)).unwrap();
        let url = format!("file://{}", file.display());
        let reject = d.join("rej.jsonl");

        let queue = SqliteQueue::open(d.join("ck.db")).unwrap();
        assert_eq!(ingest_from_url(&queue, &url, &reject).await, 40);

        // The input is swapped for a SHORTER one carrying different ids. The recorded
        // offset now points past its end; serving an empty range there would silently
        // ingest nothing and look like a clean no-op run.
        std::fs::write(
            &file,
            r#"{"custom_id":"fresh-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m"}}"#
                .to_string()
                + "\n",
        )
        .unwrap();
        assert_eq!(
            ingest_from_url(&queue, &url, &reject).await,
            1,
            "a truncated/replaced object must rescan from the start, not read nothing"
        );

        let _ = std::fs::remove_dir_all(&d);
    }
}
