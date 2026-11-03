//! Shared-secret checks for forge's two HTTP servers (the coordinator wire and the
//! Batch REST front door).
//!
//! Both are a single bearer token compared against a configured secret. They live here
//! rather than in each server so there is exactly one comparison to get right: the
//! check is the only thing between a reachable port and the prompts behind it, and two
//! hand-written copies are two chances for one of them to grow a short-circuit `==`.

use std::net::{SocketAddr, ToSocketAddrs};

use crate::ForgeError;

/// Does an `Authorization` header value carry `Bearer <expected>`?
///
/// Absent header, a different scheme, and a wrong token all answer `false` — the
/// caller turns every one of them into the same `401`, so the response cannot be used
/// to probe which part was wrong.
pub fn bearer_matches(authorization: Option<&str>, expected: &str) -> bool {
    authorization
        .and_then(|a| a.strip_prefix("Bearer "))
        .map(|t| constant_time_eq(t.trim().as_bytes(), expected.as_bytes()))
        .unwrap_or(false)
}

/// Constant-time byte comparison.
///
/// A length-and-short-circuit `==` returns as soon as the first byte differs, which
/// leaks, through response timing, how much of a guess was right. No new dependency:
/// XOR-accumulate every byte (padding the shorter side) so the comparison always
/// touches every byte of both inputs regardless of where they first differ.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len_diff = (a.len() != b.len()) as u8;
    let n = a.len().max(b.len());
    let mut diff = len_diff;
    for i in 0..n {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

/// Would binding `bind` (`host:port`) keep the server reachable from this machine only?
///
/// Every address the bind resolves to must be loopback. `0.0.0.0` / `[::]` (all
/// interfaces) and any routable address are not. An address that does not resolve is
/// treated as **not** loopback: the question is "is it safe to serve this without a
/// credential", and "unknown" is not "safe".
pub fn is_loopback_bind(bind: &str) -> bool {
    if let Ok(addr) = bind.parse::<SocketAddr>() {
        return addr.ip().is_loopback();
    }
    match bind.to_socket_addrs() {
        Ok(addrs) => {
            let addrs: Vec<SocketAddr> = addrs.collect();
            !addrs.is_empty() && addrs.iter().all(|a| a.ip().is_loopback())
        }
        Err(_) => false,
    }
}

/// Read a shared secret from the environment variable `name`.
///
/// `Ok(None)` when it is unset. **A set-but-blank value is an error, not "no secret".**
/// An empty token would make `Bearer ` (with nothing after it) a valid credential, so a
/// templating slip that renders `FORGE_API_KEY=""` would leave the server open while
/// its operator believes it is locked. Surrounding whitespace is trimmed, because a
/// secret pasted into a Kubernetes Secret or a `.env` file routinely picks up a
/// trailing newline that no client will ever send.
pub fn secret_from_env(name: &str) -> Result<Option<String>, ForgeError> {
    match std::env::var(name) {
        Ok(v) => non_blank_secret(name, &v).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ForgeError::Config(format!(
            "{name} is set but is not valid UTF-8"
        ))),
    }
}

/// Validate a secret that arrived from anywhere (an env var, a flag, a file):
/// trimmed, and refused when blank. `source` names where it came from, for the error.
pub fn non_blank_secret(source: &str, value: &str) -> Result<String, ForgeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ForgeError::Config(format!(
            "{source} is set but empty. An empty secret would accept `Bearer ` with nothing \
             after it, which is the same as no authentication. Set a real value, or unset \
             it to run without one."
        )));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_match_logic() {
        assert!(bearer_matches(Some("Bearer sk-123"), "sk-123"));
        assert!(bearer_matches(Some("Bearer sk-123 "), "sk-123"));
        assert!(!bearer_matches(Some("Bearer sk-123"), "sk-999"));
        assert!(!bearer_matches(Some("sk-123"), "sk-123")); // missing scheme
        assert!(!bearer_matches(Some("Basic sk-123"), "sk-123")); // wrong scheme
        assert!(!bearer_matches(None, "sk-123"));
    }

    #[test]
    fn constant_time_eq_matches_regular_equality() {
        assert!(constant_time_eq(b"sk-123", b"sk-123"));
        assert!(!constant_time_eq(b"sk-123", b"sk-999"));
        assert!(!constant_time_eq(b"short", b"much-longer"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn loopback_binds_are_recognised_and_everything_else_is_not() {
        assert!(is_loopback_bind("127.0.0.1:8080"));
        assert!(is_loopback_bind("127.0.0.1:0"));
        assert!(is_loopback_bind("[::1]:8080"));
        assert!(is_loopback_bind("localhost:8080"));
        assert!(!is_loopback_bind("0.0.0.0:8080"));
        assert!(!is_loopback_bind("[::]:8080"));
        assert!(!is_loopback_bind("10.0.0.5:8080"));
        // Unresolvable is not provably local, so it is not loopback.
        assert!(!is_loopback_bind("no-such-host.invalid:8080"));
        assert!(!is_loopback_bind("not an address"));
    }

    #[test]
    fn a_blank_secret_is_refused_and_a_padded_one_is_trimmed() {
        assert!(non_blank_secret("X", "").is_err());
        assert!(non_blank_secret("X", "  \n").is_err());
        assert_eq!(non_blank_secret("X", " tok\n").unwrap(), "tok");
    }
}
