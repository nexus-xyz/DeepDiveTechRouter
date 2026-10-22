//! Mock upstream LLM server on 127.0.0.1:8081.
//!
//! Simulates five models with deterministic latency, failure and mid-stream
//! disconnect behaviour, and keeps an exact ledger of what Nexus owes upstream.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use parking_lot::Mutex;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::types::{
    ModelMetrics, ModelSpec, StreamChunk, TokenUsage, UpstreamChatRequest, UpstreamMetrics,
    UserAccount,
};

pub const UPSTREAM_ADDR: &str = "127.0.0.1:8081";
/// Also served so routers that dial `localhost:8081` don't stall on an IPv6
/// connect attempt before falling back to IPv4.
pub const UPSTREAM_ADDR_V6: &str = "[::1]:8081";
pub const DELAY_HEADER: &str = "x-upstream-simulated-delay-ms";
pub const ERR_STREAM_DISCONNECTED: &str = "ERR_STREAM_DISCONNECTED_BY_UPSTREAM";
pub const CANONICAL_TOKENS: [&str; 10] = [
    "The ",
    "quick ",
    "brown ",
    "fox ",
    "jumps ",
    "over ",
    "the ",
    "lazy ",
    "dog ",
    "systematically.",
];

const INTER_TOKEN_DELAY_MS: u64 = 8;
const UNAVAILABLE_DELAY_MS: u64 = 20;
const LATENCY_SPIKE_TTFB_MS: u64 = 4000;
const TOKENS_BEFORE_DROP: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chaos {
    Unavailable,
    MidStreamDrop,
    Normal,
}

/// deepseek-v3 behaviour by call number (mod 20): 7x 503 (35%), 7x mid-stream
/// drop (35%, streaming only), 6x normal (30%). Deterministic so every
/// candidate faces the same storm.
const DEEPSEEK_PATTERN: [Chaos; 20] = {
    use Chaos::{MidStreamDrop as D, Normal as N, Unavailable as U};
    [U, D, N, U, D, U, N, D, U, D, N, U, D, N, U, D, N, U, D, N]
};

pub fn model_catalog() -> Vec<ModelSpec> {
    let model =
        |id: &str, provider: &str, input: f64, output: f64, score: f64, count: u32| ModelSpec {
            id: id.to_string(),
            provider: provider.to_string(),
            input_price_per_1m_usd: input,
            output_price_per_1m_usd: output,
            review_score: score,
            review_count: count,
        };
    vec![
        model("gpt-4o-mini", "OpenAI", 0.15, 0.60, 4.2, 1200),
        model("claude-3-5-sonnet", "Anthropic", 3.00, 15.00, 4.9, 3000),
        model("deepseek-v3", "DeepSeek", 0.14, 0.28, 4.6, 800),
        model("llama-3-70b-fast", "Groq", 0.50, 0.75, 4.4, 500),
        model("nexus-experimental-v1", "NexusLabs", 0.10, 0.20, 0.0, 0),
    ]
}

pub fn initial_users() -> Vec<UserAccount> {
    [
        ("user_whale", 10_000.0),
        ("user_standard", 500.0),
        ("user_low_balance", 0.05),
    ]
    .into_iter()
    .map(|(user_id, balance_cents)| UserAccount {
        user_id: user_id.to_string(),
        balance_cents,
    })
    .collect()
}

#[derive(Default)]
struct Ledger {
    total_cost_cents: f64,
    models: BTreeMap<String, ModelMetrics>,
}

pub struct UpstreamState {
    catalog: Vec<ModelSpec>,
    ledger: Mutex<Ledger>,
}

impl UpstreamState {
    pub fn new() -> Self {
        let catalog = model_catalog();
        let models = catalog
            .iter()
            .map(|m| (m.id.clone(), ModelMetrics::default()))
            .collect();
        Self {
            catalog,
            ledger: Mutex::new(Ledger {
                total_cost_cents: 0.0,
                models,
            }),
        }
    }

    pub fn metrics(&self) -> UpstreamMetrics {
        let ledger = self.ledger.lock();
        let models = ledger.models.clone();
        UpstreamMetrics {
            total_upstream_cost_cents: ledger.total_cost_cents,
            total_requests: models.values().map(|m| m.requests).sum(),
            total_failures: models.values().map(|m| m.failures).sum(),
            mid_stream_drops: models.values().map(|m| m.mid_stream_drops).sum(),
            handover_resumes: models.values().map(|m| m.handover_resumes).sum(),
            models,
        }
    }

    /// Counts the call and returns its 1-based sequence number for this model.
    fn register_call(&self, model_id: &str, is_handover: bool) -> u64 {
        let mut ledger = self.ledger.lock();
        let m = ledger.models.entry(model_id.to_string()).or_default();
        m.requests += 1;
        if is_handover {
            m.handover_resumes += 1;
        }
        m.requests
    }

    fn record(&self, model_id: &str, cost_cents: f64, outcome: Chaos) {
        let mut ledger = self.ledger.lock();
        ledger.total_cost_cents += cost_cents;
        let m = ledger.models.entry(model_id.to_string()).or_default();
        m.cost_cents += cost_cents;
        match outcome {
            Chaos::Unavailable => m.failures += 1,
            Chaos::MidStreamDrop => {
                m.failures += 1;
                m.mid_stream_drops += 1;
            }
            Chaos::Normal => {}
        }
    }
}

pub fn router(state: Arc<UpstreamState>) -> Router {
    Router::new()
        .route("/upstream/models", get(list_models))
        .route("/upstream/users", get(list_users))
        .route(
            "/upstream/v1/chat/completions",
            post(chat_completions).options(preflight),
        )
        .route("/upstream/metrics", get(get_metrics))
        .layer(axum::middleware::map_response(allow_browser_dashboards))
        .with_state(state)
}

pub async fn serve(listener: TcpListener, state: Arc<UpstreamState>) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

/// Lets a candidate's browser dashboard read `/upstream/*` directly.
async fn allow_browser_dashboards(mut res: Response) -> Response {
    let headers = res.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(DELAY_HEADER),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("content-type"),
    );
    res
}

/// CORS preflight for browser `POST`s with a JSON body.
async fn preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn list_models(State(state): State<Arc<UpstreamState>>) -> Json<Vec<ModelSpec>> {
    Json(state.catalog.clone())
}

async fn list_users() -> Json<Vec<UserAccount>> {
    Json(initial_users())
}

async fn get_metrics(State(state): State<Arc<UpstreamState>>) -> Json<UpstreamMetrics> {
    Json(state.metrics())
}

async fn chat_completions(
    State(state): State<Arc<UpstreamState>>,
    payload: Result<Json<UpstreamChatRequest>, JsonRejection>,
) -> Response {
    let started = Instant::now();
    let req = match payload {
        Ok(Json(req)) => req,
        Err(rejection) => {
            return error_response(StatusCode::BAD_REQUEST, &rejection.body_text(), 0)
        }
    };
    let Some(model) = state.catalog.iter().find(|m| m.id == req.model_id).cloned() else {
        let msg = format!("unknown model_id '{}'", req.model_id);
        return error_response(StatusCode::NOT_FOUND, &msg, 0);
    };
    if req.prompt.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "prompt must not be empty", 0);
    }
    if req.max_output_tokens < 1 {
        return error_response(StatusCode::BAD_REQUEST, "max_output_tokens must be >= 1", 0);
    }

    let already_streamed = prefix_token_count(req.assistant_prefix.as_deref());
    let call_no = state.register_call(&model.id, already_streamed > 0);
    let ttfb_ms = simulated_ttfb_ms(&model.id, call_no);

    let mut chaos = if model.id == "deepseek-v3" {
        DEEPSEEK_PATTERN[((call_no - 1) % DEEPSEEK_PATTERN.len() as u64) as usize]
    } else {
        Chaos::Normal
    };
    if chaos == Chaos::MidStreamDrop && !req.stream {
        chaos = Chaos::Normal;
    }

    if chaos == Chaos::Unavailable {
        tokio::time::sleep_until(started + Duration::from_millis(UNAVAILABLE_DELAY_MS)).await;
        state.record(&model.id, 0.0, chaos);
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream model temporarily unavailable",
            elapsed_ms(started),
        );
    }

    let input_tokens = ((req.prompt.len() as u32) / 4).max(1);
    let mut tokens: Vec<&'static str> = CANONICAL_TOKENS[already_streamed..]
        .iter()
        .copied()
        .take(req.max_output_tokens as usize)
        .collect();
    if chaos == Chaos::MidStreamDrop {
        tokens.truncate(TOKENS_BEFORE_DROP);
    }
    let usage = TokenUsage {
        input_tokens,
        output_tokens: tokens.len() as u32,
    };
    // Upstream bills for everything it generated, even if the stream later dies.
    state.record(&model.id, cost_cents(&model, usage), chaos);
    let total_delay_ms = ttfb_ms + INTER_TOKEN_DELAY_MS * tokens.len() as u64;

    if req.stream {
        let drop_mid_stream = chaos == Chaos::MidStreamDrop;
        stream_response(model.id, tokens, usage, drop_mid_stream, started, ttfb_ms).await
    } else {
        tokio::time::sleep_until(started + Duration::from_millis(total_delay_ms)).await;
        let chunk = StreamChunk {
            token: tokens.concat(),
            is_final: true,
            model_used: Some(model.id),
            usage: Some(usage),
            ..Default::default()
        };
        with_delay_header(Json(chunk).into_response(), elapsed_ms(started))
    }
}

async fn stream_response(
    model_id: String,
    tokens: Vec<&'static str>,
    usage: TokenUsage,
    drop_mid_stream: bool,
    started: Instant,
    ttfb_ms: u64,
) -> Response {
    tokio::time::sleep_until(started + Duration::from_millis(ttfb_ms)).await;
    // Header = measured TTFB + the token phase below. Tokens are paced against
    // absolute deadlines so timer overshoot never accumulates into time the
    // header does not account for.
    let tokens_ms = INTER_TOKEN_DELAY_MS * tokens.len() as u64;
    let total_delay_ms = elapsed_ms(started) + tokens_ms;
    let tokens_start = Instant::now();

    let (tx, rx) = mpsc::channel::<Bytes>(16);
    tokio::spawn(async move {
        for (i, token) in tokens.into_iter().enumerate() {
            let due = INTER_TOKEN_DELAY_MS * (i as u64 + 1);
            tokio::time::sleep_until(tokens_start + Duration::from_millis(due)).await;
            let chunk = StreamChunk {
                token: token.to_string(),
                model_used: Some(model_id.clone()),
                ..Default::default()
            };
            if tx.send(sse_frame(&chunk)).await.is_err() {
                return; // client went away
            }
        }
        let tail = if drop_mid_stream {
            StreamChunk {
                error: Some(ERR_STREAM_DISCONNECTED.to_string()),
                ..Default::default()
            }
        } else {
            StreamChunk {
                is_final: true,
                model_used: Some(model_id),
                usage: Some(usage),
                ..Default::default()
            }
        };
        let _ = tx.send(sse_frame(&tail)).await;
    });

    let body = Body::from_stream(ReceiverStream::new(rx).map(Ok::<Bytes, Infallible>));
    let mut res = Response::new(body);
    let headers = res.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    with_delay_header(res, total_delay_ms)
}

fn sse_frame(chunk: &StreamChunk) -> Bytes {
    let json = serde_json::to_string(chunk).expect("StreamChunk always serializes");
    Bytes::from(format!("data: {json}\n\n"))
}

fn error_response(status: StatusCode, message: &str, delay_ms: u64) -> Response {
    let res = (status, Json(json!({ "error": message }))).into_response();
    with_delay_header(res, delay_ms)
}

fn with_delay_header(mut res: Response, delay_ms: u64) -> Response {
    res.headers_mut()
        .insert(DELAY_HEADER, HeaderValue::from(delay_ms));
    res
}

/// Whole milliseconds actually spent, rounded down so the header never
/// overstates the upstream's share of the latency.
fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

fn simulated_ttfb_ms(model_id: &str, call_no: u64) -> u64 {
    match model_id {
        "gpt-4o-mini" => 30,
        "claude-3-5-sonnet" => 45,
        "deepseek-v3" => 20,
        "llama-3-70b-fast" if call_no <= 5 => 25,
        "llama-3-70b-fast" => LATENCY_SPIKE_TTFB_MS,
        _ => 20,
    }
}

/// $1 = 100 cents, so $X per 1M tokens = X / 10_000 cents per token.
fn cost_cents(model: &ModelSpec, usage: TokenUsage) -> f64 {
    (usage.input_tokens as f64 * model.input_price_per_1m_usd
        + usage.output_tokens as f64 * model.output_price_per_1m_usd)
        / 10_000.0
}

/// How many leading canonical tokens the client has already received.
fn prefix_token_count(prefix: Option<&str>) -> usize {
    let Some(prefix) = prefix else { return 0 };
    let mut covered = String::new();
    let mut count = 0;
    for token in CANONICAL_TOKENS {
        covered.push_str(token);
        if !prefix.starts_with(&covered) {
            break;
        }
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_resumes_after_streamed_tokens() {
        assert_eq!(prefix_token_count(None), 0);
        assert_eq!(prefix_token_count(Some("")), 0);
        assert_eq!(prefix_token_count(Some("The quick brown ")), 3);
        assert_eq!(prefix_token_count(Some("The quick bro")), 2);
        assert_eq!(prefix_token_count(Some(&CANONICAL_TOKENS.concat())), 10);
    }

    #[test]
    fn cost_is_in_cents() {
        let gpt = &model_catalog()[0];
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
        };
        // $0.15 + $0.60 = 75 cents
        assert!((cost_cents(gpt, usage) - 75.0).abs() < 1e-9);
    }
}
