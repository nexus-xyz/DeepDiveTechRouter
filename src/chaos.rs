//! 5-wave chaos traffic generator and auditor, fired at the candidate router
//! on 127.0.0.1:8080.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use colored::Colorize;
use futures::future::join_all;
use futures::StreamExt;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde_json::json;
use tokio::sync::Barrier;

use crate::types::{AdminStateResponse, RouterChatRequest, StreamChunk};
use crate::upstream::{UpstreamState, CANONICAL_TOKENS, DELAY_HEADER};

pub const CHAT_URL: &str = "http://127.0.0.1:8080/v1/chat/completions";
pub const ADMIN_URL: &str = "http://127.0.0.1:8080/admin/state";

const PROMPT_SEED: &str =
    "Nexus routes every prompt across five upstream providers while keeping a strict ledger. ";
const NEGATIVE_EPSILON_CENTS: f64 = -0.0001;

/// Everything observed about a single request to the router.
#[derive(Debug, Default, Clone)]
pub struct Outcome {
    pub status: Option<u16>,
    pub elapsed_ms: f64,
    pub ttfb_ms: f64,
    pub upstream_delay_ms: Option<f64>,
    pub text: String,
    pub model_used: Option<String>,
    /// First `error` field seen in a chunk, or a mid-body transport failure.
    pub chunk_error: Option<String>,
    /// Connect/send failure: no HTTP response at all.
    pub transport_error: Option<String>,
    pub malformed_frames: usize,
}

impl Outcome {
    fn is_success(&self) -> bool {
        matches!(self.status, Some(s) if (200..300).contains(&s))
    }

    fn is_client_error(&self) -> bool {
        matches!(self.status, Some(s) if (400..500).contains(&s))
    }

    fn status_label(&self) -> String {
        match self.status {
            Some(s) => s.to_string(),
            None => "NO-RESPONSE".to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ChaosReport {
    pub internal_overhead_samples_ms: Vec<f64>,
    pub ttfb_samples_ms: Vec<f64>,
    pub corrupted_streams_count: usize,
    pub total_requests: usize,
    pub transport_errors: usize,
    /// 2xx responses that did not propagate `X-Upstream-Simulated-Delay-Ms`.
    pub missing_delay_header: usize,
    pub wave4_streams: usize,
    pub wave4_complete: usize,
    pub wave4_drops_injected: u64,
    pub wave4_handover_resumes: u64,
}

impl ChaosReport {
    fn record(&mut self, o: &Outcome) {
        self.total_requests += 1;
        if o.transport_error.is_some() {
            self.transport_errors += 1;
            return;
        }
        if o.is_success() && o.upstream_delay_ms.is_none() {
            self.missing_delay_header += 1;
        }
        let upstream_ms = o.upstream_delay_ms.unwrap_or(0.0);
        self.internal_overhead_samples_ms
            .push((o.elapsed_ms - upstream_ms).max(0.05));
        if o.is_success() {
            self.ttfb_samples_ms.push(o.ttfb_ms);
        }
    }
}

/// Nearest-rank percentile.
pub fn percentile(samples: &[f64], p: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

pub async fn run(client: &Client, upstream: &UpstreamState) -> ChaosReport {
    let mut report = ChaosReport::default();
    wave1_financial_precision(client, upstream, &mut report).await;
    wave2_double_spend(client, &mut report).await;
    wave3_idempotency_storm(client, upstream, &mut report).await;
    wave4_upstream_chaos(client, upstream, &mut report).await;
    wave5_malformed(client, upstream, &mut report).await;
    report
}

pub async fn fetch_admin_state(client: &Client) -> Result<AdminStateResponse, String> {
    let resp = client
        .get(ADMIN_URL)
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<AdminStateResponse>()
        .await
        .map_err(|e| format!("invalid JSON: {e}"))
}

async fn balance_of(client: &Client, user_id: &str) -> Option<f64> {
    fetch_admin_state(client)
        .await
        .ok()
        .and_then(|s| s.user_balances_cents.get(user_id).copied())
}

// ---------------------------------------------------------------------------
// Waves
// ---------------------------------------------------------------------------

async fn wave1_financial_precision(
    client: &Client,
    upstream: &UpstreamState,
    report: &mut ChaosReport,
) {
    wave_header(
        1,
        "Sub-Cent Financial Precision & Sanity",
        "40 non-streaming requests for user_whale (4 in flight), prompts of 13/71/149 chars",
    );
    const PROMPT_LENGTHS: [usize; 3] = [13, 71, 149];
    const MAX_TOKENS: [i32; 4] = [10, 7, 3, 9];
    const MIN_SCORES: [f64; 3] = [0.0, 4.0, 4.5];

    let balance_before = balance_of(client, "user_whale").await;
    let cost_before = upstream.metrics().total_upstream_cost_cents;

    let mut statuses = BTreeMap::new();
    let mut ok = 0;
    let mut valid_text = 0;
    for batch in 0..10 {
        let bodies: Vec<String> = (0..4)
            .map(|j| {
                let i = batch * 4 + j;
                encode(&new_request(
                    "user_whale",
                    prompt_of_len(PROMPT_LENGTHS[i % 3], i * 7),
                    MAX_TOKENS[i % 4],
                    MIN_SCORES[i % 3],
                    false,
                ))
            })
            .collect();
        let outcomes = join_all(bodies.into_iter().map(|b| send(client, b))).await;
        for o in &outcomes {
            report.record(o);
            *statuses.entry(o.status_label()).or_insert(0usize) += 1;
            if o.is_success() {
                ok += 1;
                if is_canonical_prefix(&o.text) && !o.text.is_empty() {
                    valid_text += 1;
                }
            }
        }
    }

    let balance_after = balance_of(client, "user_whale").await;
    let upstream_cost = upstream.metrics().total_upstream_cost_cents - cost_before;

    info(&format!("status codes: {}", format_tally(&statuses)));
    check(ok == 40, &format!("{ok}/40 requests returned 2xx"));
    check(
        valid_text == ok,
        &format!("{valid_text}/{ok} successful bodies carry valid canonical text"),
    );
    match (balance_before, balance_after) {
        (Some(before), Some(after)) => {
            let billed = before - after;
            info(&format!(
                "user_whale debited {billed:.6} cents | upstream cost incurred {upstream_cost:.6} cents"
            ));
            check(
                billed + 1e-9 >= upstream_cost,
                "user_whale was billed at least the upstream cost (no sub-cent rounding leak)",
            );
        }
        _ => warn("could not read user_whale balance from /admin/state"),
    }
}

async fn wave2_double_spend(client: &Client, report: &mut ChaosReport) {
    wave_header(
        2,
        "The Double-Spend Burst",
        "20 simultaneous requests for user_low_balance (0.05 cents), max_output_tokens=800",
    );
    let balance_before = balance_of(client, "user_low_balance").await;
    let bodies = (0..20)
        .map(|i| {
            encode(&new_request(
                "user_low_balance",
                prompt_of_len(24, i * 3),
                800,
                4.1,
                false,
            ))
        })
        .collect();
    let outcomes = fire_simultaneously(client, bodies).await;

    let mut accepted = 0;
    let mut payment_required = 0;
    let mut other = BTreeMap::new();
    for o in &outcomes {
        report.record(o);
        match o.status {
            Some(s) if (200..300).contains(&s) => accepted += 1,
            Some(402) => payment_required += 1,
            _ => *other.entry(o.status_label()).or_insert(0usize) += 1,
        }
    }
    info(&format!(
        "200 OK: {accepted} | 402 Payment Required: {payment_required} | other: {}",
        if other.is_empty() {
            "none".to_string()
        } else {
            format_tally(&other)
        }
    ));

    match balance_of(client, "user_low_balance").await {
        Some(after) => {
            info(&format!(
                "user_low_balance: {} -> {after:.6} cents",
                balance_before
                    .map(|b| format!("{b:.6}"))
                    .unwrap_or_else(|| "?".to_string())
            ));
            check(
                after >= NEGATIVE_EPSILON_CENTS,
                "user_low_balance never went negative",
            );
        }
        None => warn("could not read user_low_balance from /admin/state"),
    }
}

async fn wave3_idempotency_storm(
    client: &Client,
    upstream: &UpstreamState,
    report: &mut ChaosReport,
) {
    wave_header(
        3,
        "Idempotency Storm",
        "10 simultaneous identical requests, idempotency_key=idem_storm_999",
    );
    let balance_before = balance_of(client, "user_standard").await;
    let metrics_before = upstream.metrics();

    let mut req = new_request("user_standard", prompt_of_len(71, 5), 10, 4.0, false);
    req.request_id = "req_idem_storm_999".to_string();
    req.idempotency_key = Some("idem_storm_999".to_string());
    let outcomes = fire_simultaneously(client, vec![encode(&req); 10]).await;

    let metrics_after = upstream.metrics();
    let balance_after = balance_of(client, "user_standard").await;

    let mut statuses = BTreeMap::new();
    let mut bodies = BTreeSet::new();
    for o in &outcomes {
        report.record(o);
        *statuses.entry(o.status_label()).or_insert(0usize) += 1;
        if o.is_success() {
            bodies.insert(o.text.clone());
        }
    }
    let successes = |m: &crate::types::UpstreamMetrics| m.total_requests - m.total_failures;
    let generations = successes(&metrics_after).saturating_sub(successes(&metrics_before));

    info(&format!("status codes: {}", format_tally(&statuses)));
    info(&format!(
        "successful upstream generations triggered: {generations}"
    ));
    check(
        generations <= 1,
        "storm collapsed into at most one billable upstream generation",
    );
    check(
        bodies.len() <= 1,
        "every successful reply carries the same content",
    );
    match (balance_before, balance_after) {
        (Some(before), Some(after)) => {
            info(&format!("user_standard debited {:.6} cents", before - after))
        }
        _ => warn("could not read user_standard balance from /admin/state"),
    }
}

async fn wave4_upstream_chaos(
    client: &Client,
    upstream: &UpstreamState,
    report: &mut ChaosReport,
) {
    wave_header(
        4,
        "Upstream Chaos, Mid-Stream Drops & Latency Spikes",
        "60 concurrent streaming requests for user_whale, min_review_score=4.3",
    );
    const PROMPT_LENGTHS: [usize; 3] = [13, 71, 149];
    let canonical = CANONICAL_TOKENS.concat();
    let metrics_before = upstream.metrics();

    let bodies = (0..60)
        .map(|i| {
            encode(&new_request(
                "user_whale",
                prompt_of_len(PROMPT_LENGTHS[i % 3], i * 11),
                64,
                4.3,
                true,
            ))
        })
        .collect();
    let outcomes = fire_simultaneously(client, bodies).await;
    let metrics_after = upstream.metrics();

    let mut complete = 0;
    let mut truncated = 0;
    let mut corrupted = 0;
    let mut surfaced_errors = 0;
    let mut malformed_frames = 0;
    let mut failed = BTreeMap::new();
    let mut models = BTreeMap::new();
    let mut first_corrupted: Option<String> = None;
    for o in &outcomes {
        report.record(o);
        if !o.is_success() {
            *failed.entry(o.status_label()).or_insert(0usize) += 1;
            continue;
        }
        malformed_frames += o.malformed_frames;
        if o.chunk_error.is_some() {
            surfaced_errors += 1;
        }
        if let Some(m) = &o.model_used {
            *models.entry(m.clone()).or_insert(0usize) += 1;
        }
        if !is_canonical_prefix(&o.text) {
            corrupted += 1;
            first_corrupted.get_or_insert_with(|| o.text.clone());
        } else if o.text == canonical {
            complete += 1;
        } else {
            truncated += 1;
        }
    }

    report.corrupted_streams_count += corrupted;
    report.wave4_streams = outcomes.len();
    report.wave4_complete = complete;
    report.wave4_drops_injected = metrics_after.mid_stream_drops - metrics_before.mid_stream_drops;
    report.wave4_handover_resumes =
        metrics_after.handover_resumes - metrics_before.handover_resumes;

    info(&format!(
        "complete: {complete} | truncated: {truncated} | corrupted: {corrupted} | non-2xx: {}",
        if failed.is_empty() {
            "0".to_string()
        } else {
            format_tally(&failed)
        }
    ));
    info(&format!(
        "upstream mid-stream drops injected: {} | handover resumes (assistant_prefix): {}",
        report.wave4_drops_injected, report.wave4_handover_resumes
    ));
    if !models.is_empty() {
        info(&format!("final model per stream: {}", format_tally(&models)));
    }
    if surfaced_errors > 0 {
        info(&format!(
            "{surfaced_errors} streams leaked an error chunk to the client"
        ));
    }
    if malformed_frames > 0 {
        warn(&format!("{malformed_frames} SSE frames were not valid StreamChunk JSON"));
    }
    if let Some(text) = first_corrupted {
        warn(&format!("example corrupted stream: {text:?}"));
    }
    check(corrupted == 0, "no duplicated / corrupted streams");
    check(complete == outcomes.len(), "every stream delivered the full sentence");
}

async fn wave5_malformed(client: &Client, upstream: &UpstreamState, report: &mut ChaosReport) {
    wave_header(
        5,
        "Malformed & Unroutable Edge Cases",
        "10 invalid requests, each must be rejected with 4xx and never charged",
    );
    let balance_before = balance_of(client, "user_standard").await;
    let requests_before = upstream.metrics().total_requests;

    let cases: Vec<(&str, String)> = vec![
        ("max_output_tokens = -50", mutated(|v| v["max_output_tokens"] = json!(-50))),
        (
            "max_output_tokens = -50 (stream)",
            mutated(|v| {
                v["max_output_tokens"] = json!(-50);
                v["stream"] = json!(true);
            }),
        ),
        ("max_output_tokens = 0", mutated(|v| v["max_output_tokens"] = json!(0))),
        ("min_review_score = 4.99 (unroutable)", mutated(|v| v["min_review_score"] = json!(4.99))),
        (
            "min_review_score = 4.99 (stream)",
            mutated(|v| {
                v["min_review_score"] = json!(4.99);
                v["stream"] = json!(true);
            }),
        ),
        ("empty prompt", mutated(|v| v["prompt"] = json!(""))),
        ("unknown user_id", mutated(|v| v["user_id"] = json!("user_ghost"))),
        (
            "malformed JSON body",
            r#"{"request_id": "broken", "user_id": "user_standard", "prompt": "#.to_string(),
        ),
        (
            "missing required fields",
            json!({ "request_id": uuid::Uuid::new_v4().to_string(), "user_id": "user_standard" })
                .to_string(),
        ),
        ("wrong field types", mutated(|v| v["max_output_tokens"] = json!("a lot"))),
    ];

    let mut rejected = 0;
    let total = cases.len();
    for (name, body) in cases {
        let o = send(client, body).await;
        report.record(&o);
        let pass = o.is_client_error();
        if pass {
            rejected += 1;
        }
        check(pass, &format!("{name:<38} -> {}", o.status_label()));
    }

    let balance_after = balance_of(client, "user_standard").await;
    let upstream_calls = upstream.metrics().total_requests - requests_before;
    info(&format!("{rejected}/{total} rejected with 4xx"));
    check(
        upstream_calls == 0,
        &format!("no upstream calls for invalid requests ({upstream_calls} made)"),
    );
    match (balance_before, balance_after) {
        (Some(before), Some(after)) => check(
            (before - after).abs() < 1e-9,
            &format!("user_standard not charged (delta {:.6} cents)", before - after),
        ),
        _ => warn("could not read user_standard balance from /admin/state"),
    }
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

/// Spawns one task per body and releases them all through a barrier at once.
async fn fire_simultaneously(client: &Client, bodies: Vec<String>) -> Vec<Outcome> {
    let barrier = Arc::new(Barrier::new(bodies.len()));
    let handles = bodies.into_iter().map(|body| {
        let client = client.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            send(&client, body).await
        })
    });
    join_all(handles)
        .await
        .into_iter()
        .map(|joined| {
            joined.unwrap_or_else(|e| Outcome {
                transport_error: Some(format!("task failed: {e}")),
                ..Default::default()
            })
        })
        .collect()
}

pub async fn send(client: &Client, body: String) -> Outcome {
    let started = Instant::now();
    let mut out = Outcome::default();
    let resp = match client
        .post(CHAT_URL)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            out.elapsed_ms = elapsed_ms(started);
            out.transport_error = Some(e.to_string());
            return out;
        }
    };
    let headers_ms = elapsed_ms(started);
    out.status = Some(resp.status().as_u16());
    out.upstream_delay_ms = resp
        .headers()
        .get(DELAY_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<f64>().ok());
    let is_sse = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"));

    if is_sse {
        let first_byte_ms = read_sse(resp, started, &mut out).await;
        out.ttfb_ms = first_byte_ms.unwrap_or(headers_ms);
    } else {
        out.ttfb_ms = headers_ms;
        match resp.bytes().await {
            Ok(bytes) if out.is_success() => match serde_json::from_slice::<StreamChunk>(&bytes) {
                Ok(chunk) => absorb(&mut out, chunk),
                Err(_) => out.malformed_frames += 1,
            },
            Ok(_) => {}
            Err(e) => out.chunk_error = Some(format!("transport: {e}")),
        }
    }
    out.elapsed_ms = elapsed_ms(started);
    out
}

/// Consumes an SSE body; returns when the first byte arrived.
async fn read_sse(resp: reqwest::Response, started: Instant, out: &mut Outcome) -> Option<f64> {
    let mut first_byte_ms = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut body = resp.bytes_stream();
    while let Some(next) = body.next().await {
        match next {
            Ok(bytes) => {
                if first_byte_ms.is_none() && !bytes.is_empty() {
                    first_byte_ms = Some(elapsed_ms(started));
                }
                buf.extend_from_slice(&bytes);
                while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).collect();
                    handle_sse_line(&line, out);
                }
            }
            Err(e) => {
                out.chunk_error.get_or_insert(format!("transport: {e}"));
                break;
            }
        }
    }
    if !buf.is_empty() {
        handle_sse_line(&buf, out);
    }
    first_byte_ms
}

fn handle_sse_line(line: &[u8], out: &mut Outcome) {
    let line = String::from_utf8_lossy(line);
    let Some(payload) = line.trim().strip_prefix("data:") else {
        return;
    };
    let payload = payload.trim();
    if payload.is_empty() || payload == "[DONE]" {
        return;
    }
    match serde_json::from_str::<StreamChunk>(payload) {
        Ok(chunk) => absorb(out, chunk),
        Err(_) => out.malformed_frames += 1,
    }
}

fn absorb(out: &mut Outcome, chunk: StreamChunk) {
    out.text.push_str(&chunk.token);
    if let Some(model) = chunk.model_used {
        out.model_used = Some(model);
    }
    if let Some(error) = chunk.error {
        out.chunk_error.get_or_insert(error);
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Request builders & validators
// ---------------------------------------------------------------------------

fn new_request(
    user_id: &str,
    prompt: String,
    max_output_tokens: i32,
    min_review_score: f64,
    stream: bool,
) -> RouterChatRequest {
    RouterChatRequest {
        request_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: None,
        user_id: user_id.to_string(),
        prompt,
        max_output_tokens,
        min_review_score,
        stream,
    }
}

fn encode(req: &RouterChatRequest) -> String {
    serde_json::to_string(req).expect("RouterChatRequest always serializes")
}

/// A valid user_standard request with one field tampered with.
fn mutated(edit: impl FnOnce(&mut serde_json::Value)) -> String {
    let req = new_request("user_standard", prompt_of_len(40, 0), 10, 4.0, false);
    let mut value = serde_json::to_value(req).expect("RouterChatRequest always serializes");
    edit(&mut value);
    value.to_string()
}

/// Deterministic ASCII prompt of exactly `len` bytes.
fn prompt_of_len(len: usize, offset: usize) -> String {
    PROMPT_SEED.chars().cycle().skip(offset).take(len).collect()
}

/// True when `text` could have been produced by one clean pass over the
/// canonical sentence (no restarts, no duplicated prefixes, no garbage).
fn is_canonical_prefix(text: &str) -> bool {
    CANONICAL_TOKENS.concat().starts_with(text)
}

// ---------------------------------------------------------------------------
// Console output
// ---------------------------------------------------------------------------

fn wave_header(n: u8, title: &str, detail: &str) {
    println!();
    println!(
        "{}",
        format!("━━━ WAVE {n}: {title} ━━━").bright_cyan().bold()
    );
    println!("    {}", detail.dimmed());
}

fn check(pass: bool, msg: &str) {
    if pass {
        println!("    {} {msg}", "✔".green().bold());
    } else {
        println!("    {} {}", "✘".red().bold(), msg.red());
    }
}

fn info(msg: &str) {
    println!("    {} {msg}", "•".bright_blue());
}

fn warn(msg: &str) {
    println!("    {} {}", "!".yellow().bold(), msg.yellow());
}

fn format_tally(tally: &BTreeMap<String, usize>) -> String {
    tally
        .iter()
        .map(|(k, v)| format!("{k}×{v}"))
        .collect::<Vec<_>>()
        .join("  ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_restarted_stream() {
        assert!(is_canonical_prefix(""));
        assert!(is_canonical_prefix("The quick brown "));
        assert!(is_canonical_prefix(&CANONICAL_TOKENS.concat()));
        assert!(!is_canonical_prefix("The quick brown The quick brown fox "));
    }

    #[test]
    fn percentile_nearest_rank() {
        let s: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&s, 50.0), Some(50.0));
        assert_eq!(percentile(&s, 99.0), Some(99.0));
        assert_eq!(percentile(&[], 50.0), None);
    }

    #[test]
    fn prompt_has_exact_length() {
        for len in [13, 71, 149] {
            assert_eq!(prompt_of_len(len, 3).len(), len);
        }
    }
}
