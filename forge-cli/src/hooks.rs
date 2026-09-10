//! Outbound completion / stall notification — the "you don't have to watch it"
//! half of running a long batch.
//!
//! **Doctrine note (read before extending this).** `README.md` "What forge is NOT"
//! forbids *cron/triggers*. That is **inbound** scheduling: something deciding when
//! forge runs, or what runs after it. A hook here is strictly **outbound** — forge
//! reports that its own single run reached a terminal state, and forge still decides
//! nothing about what happens next. It adds no `depends_on`, no successor edge, and
//! no second item to the queue. The dagron boundary is untouched. If a future change
//! makes a hook's *response* influence what forge does, that has crossed the line.
//!
//! Two target forms, because the shipped container is `FROM scratch`:
//!
//! - `https://…` / `http://…` → HTTP POST of the JSON payload. This is the form that
//!   works in the official image, which has **no shell and no curl** to exec.
//! - anything else → executed as a program, payload on **stdin**. For hosts where a
//!   real shell exists, this composes with whatever the operator already has.
//!
//! A hook can never fail the run. A 50M-item job that completed must not report
//! failure because a webhook 500'd; every error here is logged and swallowed.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt;

/// How long a hook gets before it is abandoned. A hook is a courtesy; it must never
/// hold a finished run open.
const HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// A notification target: an HTTP endpoint, or a program to execute.
#[derive(Debug, Clone)]
pub enum Hook {
    /// POST the JSON payload here.
    Post(String),
    /// Execute this program, payload on stdin. Split on whitespace: `argv[0]` is the
    /// program, the rest are arguments. Deliberately NOT run through a shell — there
    /// is no shell in the shipped image, and passing operator config through `sh -c`
    /// invites quoting bugs.
    Exec(Vec<String>),
}

impl Hook {
    /// Parse a `--on-complete` / `--on-stall` value.
    pub fn parse(target: &str) -> Result<Self, String> {
        let t = target.trim();
        if t.is_empty() {
            return Err("empty hook target".into());
        }
        if t.starts_with("http://") || t.starts_with("https://") {
            return Ok(Hook::Post(t.to_string()));
        }
        let argv: Vec<String> = t.split_whitespace().map(str::to_string).collect();
        if argv.is_empty() {
            return Err("empty hook command".into());
        }
        Ok(Hook::Exec(argv))
    }

    /// Deliver `payload`. Never returns an error — a failed notification is logged,
    /// never propagated, because the run's outcome does not depend on it.
    pub async fn fire(&self, payload: &serde_json::Value) {
        let body = payload.to_string();
        let outcome = tokio::time::timeout(HOOK_TIMEOUT, self.deliver(&body)).await;
        match outcome {
            Ok(Ok(())) => tracing::info!(hook = %self.describe(), "notification delivered"),
            Ok(Err(e)) => {
                tracing::warn!(hook = %self.describe(), error = %e, "notification failed")
            }
            Err(_) => tracing::warn!(
                hook = %self.describe(),
                timeout_secs = HOOK_TIMEOUT.as_secs(),
                "notification timed out"
            ),
        }
    }

    fn describe(&self) -> String {
        match self {
            Hook::Post(url) => url.clone(),
            Hook::Exec(argv) => argv.join(" "),
        }
    }

    async fn deliver(&self, body: &str) -> Result<(), String> {
        match self {
            Hook::Post(url) => {
                let resp = reqwest::Client::new()
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body.to_string())
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                let status = resp.status();
                if status.is_success() {
                    Ok(())
                } else {
                    Err(format!("endpoint returned {status}"))
                }
            }
            Hook::Exec(argv) => {
                let mut child = tokio::process::Command::new(&argv[0])
                    .args(&argv[1..])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    // A hook is a courtesy: when `fire`'s timeout drops this
                    // future the child goes with it, rather than outliving the
                    // run it was reporting on.
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| format!("spawning {:?}: {e}", argv[0]))?;
                if let Some(mut stdin) = child.stdin.take() {
                    stdin
                        .write_all(body.as_bytes())
                        .await
                        .map_err(|e| e.to_string())?;
                    // Dropping stdin closes the pipe so the child sees EOF.
                }
                let status = child.wait().await.map_err(|e| e.to_string())?;
                if status.success() {
                    Ok(())
                } else {
                    Err(format!("command exited with {status}"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_post_and_everything_else_execs() {
        assert!(matches!(
            Hook::parse("https://hooks.example.com/x").unwrap(),
            Hook::Post(_)
        ));
        assert!(matches!(
            Hook::parse("http://localhost:9000/done").unwrap(),
            Hook::Post(_)
        ));
        match Hook::parse("/usr/bin/notify --job done").unwrap() {
            Hook::Exec(argv) => assert_eq!(argv, ["/usr/bin/notify", "--job", "done"]),
            other => panic!("expected Exec, got {other:?}"),
        }
        assert!(Hook::parse("   ").is_err());
    }

    #[tokio::test]
    async fn a_failing_hook_never_propagates() {
        // The whole contract: a broken notification must not turn a completed run
        // into a failure. `fire` returns (), so this compiles only while that holds.
        let payload = serde_json::json!({"status": "complete"});
        Hook::parse("/nonexistent/definitely-not-a-program")
            .unwrap()
            .fire(&payload)
            .await;
        Hook::parse("http://127.0.0.1:1/unreachable")
            .unwrap()
            .fire(&payload)
            .await;
    }

    #[tokio::test]
    async fn exec_receives_the_payload_on_stdin() {
        let dir = std::env::temp_dir().join(format!("forge-hook-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sink = dir.join("payload.json");
        // `cp /dev/stdin <file>` captures stdin without needing a shell.
        let hook = Hook::parse(&format!("cp /dev/stdin {}", sink.display())).unwrap();
        hook.fire(&serde_json::json!({"done": 7})).await;
        let got = std::fs::read_to_string(&sink).unwrap();
        assert!(got.contains("\"done\":7"), "stdin payload missing: {got}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
