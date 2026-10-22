//! Wire types shared by the mock upstream (:8081), the candidate router (:8080)
//! and the chaos auditor.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

/// One upstream model as advertised by `GET :8081/upstream/models`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSpec {
    pub id: String,
    pub provider: String,
    pub input_price_per_1m_usd: f64,
    pub output_price_per_1m_usd: f64,
    pub review_score: f64,
    pub review_count: u32,
}

/// Initial user balance as advertised by `GET :8081/upstream/users`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccount {
    pub user_id: String,
    pub balance_cents: f64,
}

/// Body the candidate router sends to `POST :8081/upstream/v1/chat/completions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamChatRequest {
    pub model_id: String,
    pub prompt: String,
    pub max_output_tokens: i32,
    #[serde(default)]
    pub stream: bool,
    /// Text already delivered to the end client. The upstream resumes after it,
    /// which enables seamless mid-stream failover without duplicated tokens.
    #[serde(default)]
    pub assistant_prefix: Option<String>,
}

/// Body the chaos engine sends to `POST :8080/v1/chat/completions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterChatRequest {
    pub request_id: String,
    pub idempotency_key: Option<String>,
    pub user_id: String,
    pub prompt: String,
    pub max_output_tokens: i32,
    pub min_review_score: f64,
    pub stream: bool,
}

/// SSE payload (`data: <json>\n\n`) and the full non-streaming JSON response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamChunk {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub is_final: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Router-only, final chunk of a stream: total simulated delay across every
    /// upstream call (including mid-stream handovers). Overrides the header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_delay_ms: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Router-reported latency, optional in the candidate's `/admin/state`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    #[serde(default)]
    pub p50_ms: Option<f64>,
    #[serde(default)]
    pub p95_ms: Option<f64>,
    #[serde(default)]
    pub p99_ms: Option<f64>,
}

/// Returned by the candidate's `GET :8080/admin/state`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminStateResponse {
    #[serde(default)]
    pub user_balances_cents: HashMap<String, f64>,
    #[serde(default)]
    pub total_billed_to_users_cents: f64,
    #[serde(default)]
    pub router_latency_ms: Option<LatencyPercentiles>,
}

/// Per-model counters exposed by `GET :8081/upstream/metrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelMetrics {
    pub requests: u64,
    pub failures: u64,
    pub mid_stream_drops: u64,
    pub handover_resumes: u64,
    pub cost_cents: f64,
}

/// Returned by `GET :8081/upstream/metrics`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpstreamMetrics {
    pub total_upstream_cost_cents: f64,
    pub total_requests: u64,
    pub total_failures: u64,
    pub mid_stream_drops: u64,
    pub handover_resumes: u64,
    pub models: BTreeMap<String, ModelMetrics>,
}
