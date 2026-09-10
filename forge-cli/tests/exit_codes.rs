//! The exit-code contract, exercised against the real binary.
//!
//! These codes are a published interface: orchestrators (Airflow, Dagster, Argo, a
//! K8s CronJob) branch on them, and the retry decision depends on telling `Incomplete`
//! (resume — cheap and correct) apart from `Usage` (a human must change something;
//! retrying burns the same GPU minutes to fail identically). A change here is a
//! breaking change for every pipeline wrapping forge, so it is pinned by test.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Path to the `forge` binary under test, as cargo built it for this integration test.
pub fn forge_bin() -> PathBuf {
    // The macro cargo provides for exactly this. Walking `current_exe()` by hand breaks
    // under a custom CARGO_TARGET_DIR or a non-default profile; `drain.rs` already uses
    // the macro, so this file was the odd one out.
    PathBuf::from(env!("CARGO_BIN_EXE_forge"))
}

pub fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("forge-exit-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).expect("mkdir");
    d
}

pub fn write(p: &Path, s: &str) {
    fs::write(p, s).expect("write");
}

pub fn code(args: &[&str]) -> i32 {
    Command::new(forge_bin())
        .args(args)
        .output()
        .expect("running forge")
        .status
        .code()
        .expect("exit code (not a signal)")
}

pub const INPUT_2: &str = concat!(
    r#"{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m"}}"#,
    "\n",
    r#"{"custom_id":"b","method":"POST","url":"/v1/chat/completions","body":{"model":"m"}}"#,
    "\n"
);

const RESULT_A: &str = concat!(
    r#"{"custom_id":"a","response":{"status_code":200,"body":{}},"usage":{},"#,
    r#""worker_id":"w","latency_ms":1,"attempt":1,"completed_at":0}"#,
    "\n"
);

#[test]
fn verify_exits_0_when_complete_and_2_when_work_remains() {
    let d = tmp("verify");
    let one = d.join("one.jsonl");
    let two = d.join("two.jsonl");
    let res = d.join("res.jsonl");
    write(&one, INPUT_2.lines().next().unwrap());
    write(&two, INPUT_2);
    write(&res, RESULT_A);

    // Every input id has a terminal result → Complete.
    assert_eq!(
        code(&[
            "verify",
            "--input",
            one.to_str().unwrap(),
            "--results",
            res.to_str().unwrap(),
            "--no-missing-out",
        ]),
        0,
        "a complete run must exit 0"
    );

    // `b` has no result → Incomplete, NOT a generic failure. This is the code that
    // tells a pipeline `resume` is the right next move.
    assert_eq!(
        code(&[
            "verify",
            "--input",
            two.to_str().unwrap(),
            "--results",
            res.to_str().unwrap(),
            "--no-missing-out",
        ]),
        2,
        "a run with missing ids must exit 2 (resumable), not 1"
    );
}

#[test]
fn a_bad_hook_target_is_a_usage_error_not_a_run_failure() {
    let d = tmp("hook");
    let input = d.join("in.jsonl");
    write(&input, INPUT_2);
    // An empty --on-complete is a config mistake. It must be caught BEFORE any work
    // and reported as Usage(3) — a pipeline must never retry it.
    assert_eq!(
        code(&[
            "run",
            "--input",
            input.to_str().unwrap(),
            "--out",
            d.join("out.jsonl").to_str().unwrap(),
            "--checkpoint",
            d.join("ck.db").to_str().unwrap(),
            "--workers",
            "http://127.0.0.1:1",
            "--on-complete",
            "   ",
        ]),
        3,
        "a malformed hook target must exit 3"
    );
}

#[test]
fn a_missing_input_file_is_a_usage_error() {
    let d = tmp("missing");
    // Not there now, not there on the retry either.
    assert_eq!(
        code(&[
            "run",
            "--input",
            d.join("absent.jsonl").to_str().unwrap(),
            "--out",
            d.join("out.jsonl").to_str().unwrap(),
            "--checkpoint",
            d.join("ck.db").to_str().unwrap(),
            "--workers",
            "http://127.0.0.1:1",
        ]),
        3,
        "a missing input file must exit 3, not 1"
    );
}

/// `--workers` entry syntax. The load-bearing property is that adding the optional
/// `[name=][engine:]` prefixes did not change how a plain URL parses — every existing
/// invocation in the wild is a bare URL.
mod worker_entries {
    use super::{code, forge_bin, tmp, write, INPUT_2};
    use std::process::Command;

    /// Run `forge run` far enough to parse `--workers`, and report whether the parse
    /// itself was rejected (exit 3 = usage).
    fn parse_rejected(entry: &str) -> bool {
        let d = tmp(&format!("we-{}", entry.len()));
        let input = d.join("in.jsonl");
        write(&input, INPUT_2);
        code(&[
            "run",
            "--input",
            input.to_str().unwrap(),
            "--out",
            d.join("o.jsonl").to_str().unwrap(),
            "--checkpoint",
            d.join("c.db").to_str().unwrap(),
            "--workers",
            entry,
            "--ready-grace-secs",
            "0",
        ]) == 3
    }

    #[test]
    fn a_bare_url_still_parses_and_a_bad_engine_prefix_is_a_usage_error() {
        // Backward compatibility: a plain URL must NOT be read as `engine:rest`,
        // because "http" is not an engine name. If this regresses, every existing
        // command line breaks.
        assert!(
            !parse_rejected("http://127.0.0.1:1"),
            "a bare URL must still parse"
        );
        // An engine prefix with no URL after it is caught at parse time rather than
        // becoming a connection attempt to a host named `gpu1`.
        assert!(
            parse_rejected("sglang:gpu1"),
            "an engine prefix with no URL must be a usage error"
        );
        assert!(
            parse_rejected("   "),
            "an empty entry must be a usage error"
        );
    }

    #[test]
    fn help_documents_the_mixed_fleet_syntax() {
        let out = Command::new(forge_bin())
            .args(["run", "--help"])
            .output()
            .expect("run --help");
        let help = String::from_utf8_lossy(&out.stdout);
        assert!(
            help.contains("engine:") || help.contains("name="),
            "the mixed-fleet syntax must be discoverable from --help:\n{help}"
        );
    }
}
