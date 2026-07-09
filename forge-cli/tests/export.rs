//! `forge export` (JSONL → Parquet) smoke test. The whole file compiles only with
//! `--features parquet` (which also builds the binary with the subcommand), so the
//! default `cargo test` run skips it; CI runs `cargo test -p forge-cli --features
//! parquet`.
#![cfg(feature = "parquet")]

use std::process::Command;

#[test]
fn export_writes_a_readable_parquet() {
    let dir = std::env::temp_dir().join(format!("forge-export-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let results = dir.join("results.jsonl");
    let out = dir.join("results.parquet");

    std::fs::write(
        &results,
        "{\"custom_id\":\"a\",\"response\":{\"status_code\":200,\"body\":{}},\
          \"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2},\
          \"worker_id\":\"w\",\"latency_ms\":1,\"attempt\":1,\"completed_at\":0}\n\
         {\"custom_id\":\"b\",\"response\":{\"status_code\":200,\"body\":{}},\
          \"usage\":{\"prompt_tokens\":2,\"completion_tokens\":2,\"total_tokens\":4},\
          \"worker_id\":\"w\",\"latency_ms\":1,\"attempt\":1,\"completed_at\":0}\n",
    )
    .unwrap();

    let out_text = Command::new(env!("CARGO_BIN_EXE_forge"))
        .args(["export", "--results"])
        .arg(&results)
        .arg("--out")
        .arg(&out)
        .output()
        .expect("run forge export");
    assert!(
        out_text.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out_text.stderr)
    );
    assert!(String::from_utf8_lossy(&out_text.stdout).contains("exported 2 row"));

    // A valid Parquet file is framed by the `PAR1` magic at both ends — assert it
    // without pulling the parquet crate into forge-cli.
    let bytes = std::fs::read(&out).unwrap();
    assert!(bytes.len() > 8);
    assert_eq!(&bytes[..4], b"PAR1", "leading Parquet magic");
    assert_eq!(
        &bytes[bytes.len() - 4..],
        b"PAR1",
        "trailing Parquet magic (footer)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
