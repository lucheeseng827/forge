//! `forge verify` contract test: prepare an input + results (+ dead-letter sibling)
//! on disk, invoke the real binary, and assert the JSON report **and** the exit code
//! (non-zero exactly when something is missing — the CI/acceptance gate).

use std::process::Command;

use serde_json::Value;

fn input_line(id: &str) -> String {
    format!(r#"{{"custom_id":"{id}","method":"POST","url":"","body":{{}}}}"#)
}
fn result_line(id: &str) -> String {
    format!(r#"{{"custom_id":"{id}","response":{{"status_code":200,"body":{{}}}}}}"#)
}

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("forge-verify-cli-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn verify_incomplete_exits_nonzero_with_json() {
    let d = dir("incomplete");
    let input = d.join("in.jsonl");
    let results = d.join("out.jsonl");
    let dead = d.join("out.jsonl.dead.jsonl");
    // input a,b,c ; results a ; dead b → c missing.
    std::fs::write(
        &input,
        format!(
            "{}\n{}\n{}\n",
            input_line("a"),
            input_line("b"),
            input_line("c")
        ),
    )
    .unwrap();
    std::fs::write(&results, format!("{}\n", result_line("a"))).unwrap();
    std::fs::write(&dead, format!("{}\n", result_line("b"))).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["verify", "--input"])
        .arg(&input)
        .arg("--results")
        .arg(&results)
        .arg("--json")
        .output()
        .expect("run forge verify --json");

    assert!(!out.status.success(), "missing ids must exit non-zero");
    let report: Value = serde_json::from_slice(&out.stdout).expect("parse JSON report");
    assert_eq!(report["input_ids"], 3);
    assert_eq!(report["emitted_ids"], 2);
    assert_eq!(report["missing"], 1);
    assert_eq!(report["extra"], 0);
    assert_eq!(report["complete"], false);

    // The default missing-id sidecar lists the gap.
    let missing = std::fs::read_to_string(d.join("out.jsonl.missing.txt")).unwrap();
    assert_eq!(missing.trim(), "c");

    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn verify_complete_exits_zero() {
    let d = dir("complete");
    let input = d.join("in.jsonl");
    let results = d.join("out.jsonl");
    let dead = d.join("out.jsonl.dead.jsonl");
    // a,b done; c dead-lettered → all terminal.
    std::fs::write(
        &input,
        format!(
            "{}\n{}\n{}\n",
            input_line("a"),
            input_line("b"),
            input_line("c")
        ),
    )
    .unwrap();
    std::fs::write(
        &results,
        format!("{}\n{}\n", result_line("a"), result_line("b")),
    )
    .unwrap();
    std::fs::write(&dead, format!("{}\n", result_line("c"))).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["verify", "--input"])
        .arg(&input)
        .arg("--results")
        .arg(&results)
        .arg("--json")
        .output()
        .expect("run forge verify --json");

    assert!(
        out.status.success(),
        "a complete run must exit zero; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).expect("parse JSON report");
    assert_eq!(report["missing"], 0);
    assert_eq!(report["complete"], true);
    // No sidecar written for a clean run.
    assert!(!d.join("out.jsonl.missing.txt").exists());

    let _ = std::fs::remove_dir_all(&d);
}
