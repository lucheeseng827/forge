//! Build identity — which binary is actually running.
//!
//! The first question on any support ticket is "what version is this?", and until now
//! nothing could answer it: the shipped image is `FROM scratch` with no package
//! manager to interrogate, logs carried no version, and `/v1/health` said only `ok`.
//! An operator comparing two clusters that behave differently had no way to establish
//! whether they were even running the same code.
//!
//! **No build script.** The git sha comes from an optional compile-time environment
//! variable rather than shelling out to `git` in a `build.rs`. A build script would run
//! on every compile, break reproducible builds, and produce `unknown` anyway inside the
//! release container, where the build happens from a copied source tree with no `.git`.
//! Making it an explicit build argument means the value is either correct or honestly
//! absent — never a stale sha baked in from whenever the last local build ran.
//!
//! ```console
//! docker build --build-arg FORGE_GIT_SHA="$(git rev-parse --short HEAD)" .
//! # or, locally:
//! FORGE_GIT_SHA=$(git rev-parse --short HEAD) cargo build --release
//! ```

/// The crate version from `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Short git sha this binary was built from, or `None` when the build did not supply
/// one. `None` is a true statement; a fabricated or stale sha is worse than an absent
/// one, because it sends someone diffing the wrong commit.
pub const GIT_SHA: Option<&str> = option_env!("FORGE_GIT_SHA");

/// Target triple, so a "works on my machine" report carries the architecture.
///
/// Supplied explicitly as `FORGE_TARGET`, exactly like [`GIT_SHA`]. Cargo sets `TARGET`
/// only for **build scripts**; this crate has none, so `option_env!("TARGET")` was
/// always `None` and every build reported `target=unrecorded` while claiming to carry
/// the architecture. An absent target is a true statement, but one that is absent
/// unconditionally is a field that does not work.
pub const TARGET: Option<&str> = option_env!("FORGE_TARGET");

/// One line for a startup log or `--version`: `0.4.0 (a1b2c3d, x86_64-unknown-linux-musl)`.
pub fn identity() -> String {
    match (GIT_SHA, TARGET) {
        (Some(sha), Some(target)) => format!("{VERSION} ({sha}, {target})"),
        (Some(sha), None) => format!("{VERSION} ({sha})"),
        (None, Some(target)) => format!("{VERSION} ({target})"),
        (None, None) => VERSION.to_string(),
    }
}

/// The same facts as JSON fields, for `/v1/health` and structured logs.
///
/// A field is **omitted** rather than sent as `"unknown"` when it was not supplied, so
/// a consumer can tell "this build did not record a sha" from "this build's sha is
/// literally the string unknown".
pub fn json() -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert("version".into(), VERSION.into());
    if let Some(sha) = GIT_SHA {
        m.insert("git_sha".into(), sha.into());
    }
    if let Some(t) = TARGET {
        m.insert("target".into(), t.into());
    }
    serde_json::Value::Object(m)
}

/// How log lines are rendered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable, for a terminal. The default, because the interactive case is
    /// the one where a wrong guess is most annoying and least recoverable.
    #[default]
    Text,
    /// One JSON object per line, for a log shipper.
    Json,
}

impl std::str::FromStr for LogFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" | "plain" | "human" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
            other => Err(format!(
                "unknown log format {other:?} (expected `text` or `json`)"
            )),
        }
    }
}

/// Install the process-wide log subscriber and emit one startup line carrying the
/// build identity.
///
/// The startup line is the point. A log stream whose first line names the version, the
/// sha and the command is one an operator can act on; one that starts mid-story is not.
///
/// `RUST_LOG` still selects levels (defaulting to `info`), so nothing about existing
/// filtering changes — only the rendering.
pub fn init_logging(format: LogFormat, command: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // **stderr, not stdout.** `tracing_subscriber::fmt()` defaults to stdout, which
    // puts diagnostics in the same stream as a command's OUTPUT — so a single log line
    // corrupts `forge status --json` for anything parsing it. (That was already true of
    // every existing warning, e.g. the rejected-lines notice; adding a startup line
    // merely made it fire on every invocation.) Diagnostics go to stderr; stdout is the
    // command's answer.
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .init(),
        LogFormat::Text => tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .init(),
    }
    tracing::info!(
        version = VERSION,
        git_sha = GIT_SHA.unwrap_or("unrecorded"),
        target = TARGET.unwrap_or("unrecorded"),
        command,
        "forge starting"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_always_carries_a_version_and_never_invents_a_sha() {
        let id = identity();
        assert!(id.starts_with(VERSION), "got {id}");
        // Whatever the build supplied, `identity` must not contain a placeholder that
        // could be mistaken for a real commit.
        assert!(
            !id.contains("unknown"),
            "a missing sha must be absent, not faked: {id}"
        );
    }

    #[test]
    fn json_omits_what_the_build_did_not_supply() {
        let j = json();
        assert_eq!(j["version"], VERSION);
        assert_eq!(j.get("git_sha").is_some(), GIT_SHA.is_some());
        assert_eq!(j.get("target").is_some(), TARGET.is_some());
    }
}
