//! The `forge-agent` binary's exposure rules, exercised as an operator runs it.
//!
//! These spawn the real binary because the rule under test is about what the process
//! does at startup — refuse, and leave nothing behind — which a library call cannot show.
//! The last test is about what it survives, and an abort must show up as a failed
//! assertion here rather than as a crashed test harness.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn agent() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_forge-agent"));
    // Never inherit a token from the developer's shell: the tests below are about
    // what happens with and without one, and must decide that themselves.
    c.env_remove("FORGE_COORDINATOR_TOKEN");
    c
}

fn tmp(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("forge-agent-cli-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn serve_refuses_a_public_bind_without_a_token_and_creates_nothing() {
    let dir = tmp("public");
    let queue = dir.join("state.db");
    let out = dir.join("out.jsonl");
    let o = agent()
        .args(["serve", "--bind", "0.0.0.0:0", "--queue"])
        .arg(&queue)
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    assert!(!o.status.success(), "an open public bind must be refused");
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("FORGE_COORDINATOR_TOKEN"), "{err}");
    assert!(err.contains("--insecure-no-auth"), "{err}");
    assert!(
        !queue.exists(),
        "the refusal happens before the queue is opened, so no state file is left behind"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_blank_token_is_a_startup_error_not_an_open_server() {
    let dir = tmp("blank");
    let o = agent()
        .env("FORGE_COORDINATOR_TOKEN", "   ")
        .args(["serve", "--bind", "127.0.0.1:0", "--queue"])
        .arg(dir.join("state.db"))
        .arg("--out")
        .arg(dir.join("out.jsonl"))
        .output()
        .unwrap();
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("empty"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_with_a_blank_token_fails_before_contacting_anyone() {
    let o = agent()
        .env("FORGE_COORDINATOR_TOKEN", "")
        .args([
            "run",
            "--coordinator",
            "http://127.0.0.1:1",
            "--engine",
            "http://127.0.0.1:1",
        ])
        .output()
        .unwrap();
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("FORGE_COORDINATOR_TOKEN"), "{err}");
}

/// One accept thread, a token, and two hostile clients: one declaring a body far larger
/// than memory, one declaring a body it never sends. The HTTP layer used to abort the
/// process on the first and hand the second the only accept thread until it hung up.
/// (`vendor/tiny_http/FORGE_PATCHES.md` #1.)
#[test]
fn hostile_bodies_neither_kill_nor_freeze_a_tokened_coordinator() {
    let dir = tmp("hostile");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = agent()
        .env("FORGE_COORDINATOR_TOKEN", "coord-secret")
        .args([
            "serve",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--threads",
            "1",
        ])
        .arg("--queue")
        .arg(dir.join("state.db"))
        .arg("--out")
        .arg(dir.join("out.jsonl"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "forge-agent serve never started");
        std::thread::sleep(Duration::from_millis(50));
    }
    let open = |head: &str| {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        s.write_all(head.as_bytes()).unwrap();
        let mut reply = [0u8; 12];
        let n = s.read(&mut reply).unwrap_or(0);
        (s, String::from_utf8_lossy(&reply[..n]).into_owned())
    };

    let (huge, status) =
        open("POST /v1/lease HTTP/1.1\r\nHost: x\r\nContent-Length: 4611686018427387904\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 401");
    drop(huge);
    let (idle, status) =
        open("POST /v1/result HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 401");

    // The idle client still holds its connection; the single accept thread must not
    // be stuck behind it, and the process must still be there.
    std::thread::sleep(Duration::from_millis(300));
    assert!(child.try_wait().unwrap().is_none(), "the coordinator died");
    let (_s, status) = open("POST /v1/lease HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\n{}");
    assert_eq!(status, "HTTP/1.1 401", "the coordinator stopped answering");

    drop(idle);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
