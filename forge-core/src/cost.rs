//! Cost-arbitrage reporting from **real captured** token usage.
//!
//! forge captures the standard `usage` object per item; given the GPU spend for a
//! run and a named online-API baseline, this computes the invoiceable numbers:
//! forge's `$/Mtok`, tokens-per-dollar, and the dollars saved versus paying the
//! online API for the *same* tokens. It is pure arithmetic over counts the engine
//! already has — not an estimate.
//!
//! Everything is optional: with no cost inputs you still get the token throughput;
//! supply the GPU spend for the spot-side numbers, and the online baseline for the
//! savings line. We never invent prices — the caller names them.

use serde::Serialize;

use crate::types::TokenUsage;

/// Accumulated token counts over a set of results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct UsageTotals {
    pub items: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Cached input tokens (subset of `prompt_tokens`), for discounted pricing.
    pub cached_tokens: u64,
    /// Reasoning tokens (subset of `completion_tokens`), for the ledger breakdown.
    pub reasoning_tokens: u64,
}

impl UsageTotals {
    pub fn total_tokens(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }

    /// Fold one item's usage in.
    pub fn add(&mut self, u: &TokenUsage) {
        self.items += 1;
        self.cached_tokens += u.cached_tokens;
        self.reasoning_tokens += u.reasoning_tokens;
        self.prompt_tokens += u.prompt_tokens;
        self.completion_tokens += u.completion_tokens;
    }
}

/// The cost knobs the caller supplies. All optional; absent ones drop the
/// corresponding figures from the report rather than guessing.
#[derive(Debug, Clone, Copy, Default)]
pub struct CostInputs {
    /// Total GPU spend for the run, USD — what forge actually cost on your fleet
    /// (e.g. spot `$/GPU-hour` × hours × GPUs, computed by the caller).
    pub gpu_cost_usd: Option<f64>,
    /// Online-API baseline price per 1M **input** tokens, USD.
    pub online_per_mtok_input: Option<f64>,
    /// Online-API baseline price per 1M **output** tokens, USD. Defaults to the
    /// input price when omitted (a blended estimate).
    pub online_per_mtok_output: Option<f64>,
    /// Online-API price per 1M **cached input** tokens, USD (providers bill cache hits
    /// at a discount — often ~10–25% of the input rate). When set, the online baseline
    /// prices `cached_tokens` at this rate and the rest of the prompt at the input rate,
    /// for an honest apples-to-apples comparison. When omitted, cached tokens are priced
    /// at the full input rate (conservative — never overstates savings).
    pub online_per_mtok_cached_input: Option<f64>,
}

/// The computed cost-arbitrage report. Dollar fields are `None` when their inputs
/// weren't supplied.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct CostReport {
    pub items: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Cached input tokens (subset of `prompt_tokens`) — surfaced for the discount view.
    pub cached_tokens: u64,
    /// Reasoning tokens (subset of `completion_tokens`) — the reasoning-cost breakdown.
    pub reasoning_tokens: u64,
    pub total_tokens: u64,
    /// What the run cost on your fleet (= `CostInputs.gpu_cost_usd`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forge_cost_usd: Option<f64>,
    /// forge's effective blended price per 1M tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forge_usd_per_mtok: Option<f64>,
    /// Tokens processed per dollar of GPU spend (the spot-efficiency headline).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens_per_usd: Option<f64>,
    /// What the same tokens would have cost at the online-API baseline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub online_cost_usd: Option<f64>,
    /// `online_cost − forge_cost` (the arbitrage captured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub savings_usd: Option<f64>,
    /// Savings as a percentage of the online baseline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub savings_pct: Option<f64>,
}

/// Compute the cost-arbitrage report from token totals + named cost inputs.
pub fn compute_cost(totals: UsageTotals, inputs: CostInputs) -> CostReport {
    let total_tokens = totals.total_tokens();
    let mtok = total_tokens as f64 / 1_000_000.0;

    let forge_cost_usd = inputs.gpu_cost_usd;
    let forge_usd_per_mtok = forge_cost_usd.filter(|_| mtok > 0.0).map(|c| c / mtok);
    let tokens_per_usd = forge_cost_usd
        .filter(|&c| c > 0.0)
        .map(|c| total_tokens as f64 / c);

    let online_cost_usd = inputs.online_per_mtok_input.map(|inp| {
        let out = inputs.online_per_mtok_output.unwrap_or(inp);
        // Cached input is a subset of prompt_tokens; price it at the cached rate when
        // given, the rest of the prompt at the full input rate. `cached` never exceeds
        // `prompt_tokens` in real usage, but saturate to be safe.
        let cached = totals.cached_tokens.min(totals.prompt_tokens);
        let uncached = totals.prompt_tokens - cached;
        let cached_rate = inputs.online_per_mtok_cached_input.unwrap_or(inp);
        (uncached as f64 / 1_000_000.0) * inp
            + (cached as f64 / 1_000_000.0) * cached_rate
            + (totals.completion_tokens as f64 / 1_000_000.0) * out
    });

    let savings_usd = match (online_cost_usd, forge_cost_usd) {
        (Some(online), Some(forge)) => Some(online - forge),
        _ => None,
    };
    let savings_pct = match (online_cost_usd, savings_usd) {
        (Some(online), Some(saved)) if online > 0.0 => Some(saved / online * 100.0),
        _ => None,
    };

    CostReport {
        items: totals.items,
        prompt_tokens: totals.prompt_tokens,
        completion_tokens: totals.completion_tokens,
        cached_tokens: totals.cached_tokens,
        reasoning_tokens: totals.reasoning_tokens,
        total_tokens,
        forge_cost_usd,
        forge_usd_per_mtok,
        tokens_per_usd,
        online_cost_usd,
        savings_usd,
        savings_pct,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(p: u64, c: u64) -> TokenUsage {
        TokenUsage {
            prompt_tokens: p,
            completion_tokens: c,
            total_tokens: p + c,
            ..Default::default()
        }
    }

    #[test]
    fn totals_fold() {
        let mut t = UsageTotals::default();
        t.add(&usage(700, 150));
        t.add(&usage(300, 50));
        assert_eq!(t.items, 2);
        assert_eq!((t.prompt_tokens, t.completion_tokens), (1000, 200));
        assert_eq!(t.total_tokens(), 1200);
    }

    #[test]
    fn full_arbitrage_math() {
        // 1M input + 1M output = 2M tokens; $1 of GPU; online $0.50/in + $1.50/out.
        let totals = UsageTotals {
            items: 10,
            prompt_tokens: 1_000_000,
            completion_tokens: 1_000_000,
            ..Default::default()
        };
        let r = compute_cost(
            totals,
            CostInputs {
                gpu_cost_usd: Some(1.0),
                online_per_mtok_input: Some(0.50),
                online_per_mtok_output: Some(1.50),
                ..Default::default()
            },
        );
        assert_eq!(r.total_tokens, 2_000_000);
        assert_eq!(r.forge_cost_usd, Some(1.0));
        assert_eq!(r.forge_usd_per_mtok, Some(0.5)); // $1 / 2 Mtok
        assert_eq!(r.tokens_per_usd, Some(2_000_000.0));
        assert_eq!(r.online_cost_usd, Some(2.0)); // 0.5 + 1.5
        assert_eq!(r.savings_usd, Some(1.0)); // 2.0 - 1.0
        assert_eq!(r.savings_pct, Some(50.0));
    }

    #[test]
    fn output_price_defaults_to_input() {
        let totals = UsageTotals {
            items: 1,
            prompt_tokens: 1_000_000,
            completion_tokens: 1_000_000,
            ..Default::default()
        };
        let r = compute_cost(
            totals,
            CostInputs {
                gpu_cost_usd: None,
                online_per_mtok_input: Some(1.0),
                online_per_mtok_output: None,
                ..Default::default()
            },
        );
        assert_eq!(r.online_cost_usd, Some(2.0)); // blended $1 over 2 Mtok
        assert_eq!(r.forge_cost_usd, None); // no GPU cost given → no spot figures
        assert_eq!(r.savings_usd, None);
    }

    #[test]
    fn token_only_when_no_inputs() {
        let totals = UsageTotals {
            items: 3,
            prompt_tokens: 100,
            completion_tokens: 50,
            ..Default::default()
        };
        let r = compute_cost(totals, CostInputs::default());
        assert_eq!(r.total_tokens, 150);
        assert!(r.forge_cost_usd.is_none() && r.online_cost_usd.is_none());
    }

    #[test]
    fn cached_input_priced_at_the_discount_rate() {
        // 1M prompt, 800k of it cached; 0 completion. Input $1/Mtok, cached $0.10/Mtok.
        // Online baseline: 200k @ $1 + 800k @ $0.10 = $0.20 + $0.08 = $0.28.
        let totals = UsageTotals {
            items: 1,
            prompt_tokens: 1_000_000,
            completion_tokens: 0,
            cached_tokens: 800_000,
            reasoning_tokens: 0,
        };
        let r = compute_cost(
            totals,
            CostInputs {
                online_per_mtok_input: Some(1.0),
                online_per_mtok_cached_input: Some(0.10),
                ..Default::default()
            },
        );
        assert!(
            (r.online_cost_usd.unwrap() - 0.28).abs() < 1e-9,
            "got {:?}",
            r.online_cost_usd
        );
        assert_eq!(r.cached_tokens, 800_000);

        // Without a cached rate, cached tokens fall back to the full input price = $1.00.
        let r2 = compute_cost(
            UsageTotals {
                items: 1,
                prompt_tokens: 1_000_000,
                completion_tokens: 0,
                cached_tokens: 800_000,
                reasoning_tokens: 0,
            },
            CostInputs {
                online_per_mtok_input: Some(1.0),
                ..Default::default()
            },
        );
        assert!((r2.online_cost_usd.unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn usage_captures_nested_cached_and_reasoning() {
        let v = serde_json::json!({
            "prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150,
            "prompt_tokens_details": {"cached_tokens": 80},
            "completion_tokens_details": {"reasoning_tokens": 30}
        });
        let u = TokenUsage::from_openai_usage(&v);
        assert_eq!((u.cached_tokens, u.reasoning_tokens), (80, 30));
        assert_eq!((u.prompt_tokens, u.completion_tokens), (100, 50));
        // A body with no details → zeros, no panic.
        let bare = TokenUsage::from_openai_usage(&serde_json::json!({"prompt_tokens": 5}));
        assert_eq!((bare.cached_tokens, bare.reasoning_tokens), (0, 0));
    }
}
