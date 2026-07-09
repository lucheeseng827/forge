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
