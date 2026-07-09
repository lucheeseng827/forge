//! Retry policy + exponential backoff with **full jitter**.
//!
//! `sleep = rand(0, min(cap, base · 2^attempt))` — the AWS-blog full-jitter form,
//! which decorrelates retries across a fleet of workers hammering the same
//! engine. Applied by [`crate::Worker`] implementations on 429 / 5xx / timeout.

use std::time::Duration;

/// How many times to retry a single item, and the backoff envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts before an item is dead-lettered (1 = no retry).
    pub max_attempts: u32,
    /// Base delay; the exponential's unit.
    pub base: Duration,
    /// Upper bound on a single backoff sleep.
    pub cap: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base: Duration::from_millis(500),
            cap: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Full-jitter backoff for a given (zero-based) attempt number:
    /// `rand(0, min(cap, base · 2^attempt))`.
    pub fn backoff(&self, attempt: u32) -> Duration {
        // `saturating_pow`/`saturating_mul` keep `base · 2^attempt` from overflowing,
        // and `.min(cap)` is the real ceiling — so no artificial exponent clamp (which
        // would break the documented envelope for large `cap`).
        let factor = 2u32.saturating_pow(attempt);
        let ceiling = self.base.saturating_mul(factor).min(self.cap);
        let jitter: f64 = rand::random::<f64>(); // [0, 1)
        ceiling.mul_f64(jitter)
    }

    /// True once the attempt count has reached the cap (→ dead-letter).
    pub fn is_exhausted(&self, attempts: u32) -> bool {
        attempts >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_never_exceeds_cap() {
        let p = RetryPolicy {
            max_attempts: 8,
            base: Duration::from_millis(100),
            cap: Duration::from_secs(2),
        };
        for attempt in 0..40 {
            assert!(
                p.backoff(attempt) <= p.cap,
                "attempt {attempt} exceeded cap"
            );
        }
    }

    #[test]
    fn exhaustion_boundary() {
        let p = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        assert!(!p.is_exhausted(2));
        assert!(p.is_exhausted(3));
        assert!(p.is_exhausted(4));
    }
}
