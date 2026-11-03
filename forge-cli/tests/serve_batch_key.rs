//! `forge serve-batch` takes its key from the environment, so it never has to sit in
//! the process's argv, where anything that can list processes on the host reads it.
//!
//! Exercised against the real binary: what is under test is where the secret comes
//! from and what the running process exposes, which a library call cannot show.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn forge() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_forge"));
    // Each test decides for itself whether a key is present.
    c.env_remove("FORGE_API_KEY");
    c
}

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("forge-key-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A loopback port nothing is listening on right now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The status code of one plain HTTP/1.1 request, with an optional bearer token.
fn status(port: u16, path: &str, bearer: Option<&str>) -> u16 {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n"
    )
    .unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    resp.split_whitespace().nth(1).unwrap().parse().unwrap()
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_until_listening(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "serve-batch never started listening"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn the_key_from_the_environment_gates_the_server_and_never_reaches_argv() {
    let dir = tmp("env");
    let port = free_port();
    let key = "sk-from-the-environment";
    let server = Server(
        forge()
            .env("FORGE_API_KEY", key)
            .args(["serve-batch", "--listen", &format!("127.0.0.1:{port}")])
            .arg("--data-dir")
            .arg(&dir)
            .args(["--workers", "http://127.0.0.1:1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_until_listening(port);

    assert_eq!(status(port, "/v1/health", None), 200, "health stays open");
    assert_eq!(status(port, "/v1/batches", None), 401);
    assert_eq!(status(port, "/v1/batches", Some("wrong")), 401);
    assert_eq!(status(port, "/v1/batches", Some(key)), 200);

    #[cfg(target_os = "linux")]
    {
        let cmdline = std::fs::read(format!("/proc/{}/cmdline", server.0.id())).unwrap();
        assert!(
            !String::from_utf8_lossy(&cmdline).contains(key),
            "the key must not be visible in the process's argv"
        );
    }

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_blank_key_is_refused_instead_of_serving_an_open_api() {
    let dir = tmp("blank");
    let out = forge()
        .env("FORGE_API_KEY", "  ")
        .args(["serve-batch", "--listen", "127.0.0.1:0"])
        .arg("--data-dir")
        .arg(&dir)
        .args(["--workers", "http://127.0.0.1:1"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("FORGE_API_KEY") && err.contains("empty"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn help_never_prints_the_key() {
    let out = forge()
        .env("FORGE_API_KEY", "sk-do-not-print")
        .args(["serve-batch", "--help"])
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("FORGE_API_KEY"), "the env var is documented");
    assert!(!help.contains("sk-do-not-print"), "{help}");
}
