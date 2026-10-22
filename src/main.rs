//! nexus-harness: mock upstream (:8081) + 5-wave chaos auditor (-> :8080).

mod chaos;
mod types;
mod upstream;

use std::sync::Arc;
use std::time::Duration;

use colored::{ColoredString, Colorize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::chaos::{percentile, ChaosReport, ADMIN_URL};
use crate::types::{AdminStateResponse, UpstreamMetrics};
use crate::upstream::{UpstreamState, UPSTREAM_ADDR};

const COLD_START_MODEL: &str = "nexus-experimental-v1";
const ROUTER_WAIT_SECS: u32 = 60;
const NEGATIVE_EPSILON_CENTS: f64 = -0.0001;

#[tokio::main]
async fn main() {
    let state = Arc::new(UpstreamState::new());
    let listener = match TcpListener::bind(UPSTREAM_ADDR).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!(
                "{} could not bind {UPSTREAM_ADDR}: {e}",
                "FATAL:".red().bold()
            );
            std::process::exit(1);
        }
    };
    let server_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = upstream::serve(listener, server_state).await {
            eprintln!("{} mock upstream stopped: {e}", "ERROR:".red().bold());
        }
    });

    print_banner();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(128)
        .build()
        .expect("failed to build HTTP client");

    wait_for_router(&client).await;

    println!();
    println!("{}", "Launching the 5 chaos waves against :8080 ...".bold());
    let report = chaos::run(&client, &state).await;
    let admin = chaos::fetch_admin_state(&client).await;
    let metrics = state.metrics();
    print_scorecard(&report, &admin, &metrics);

    println!();
    println!(
        "{}",
        "Mock upstream on :8081 stays live for your dashboard. Press Ctrl+C to exit.".dimmed()
    );
    let _ = tokio::signal::ctrl_c().await;
}

fn print_banner() {
    let line = "═".repeat(78);
    println!("{}", line.bright_magenta());
    println!("{}", "  NEXUS HARNESS · AI Router Technical Interview".bright_magenta().bold());
    println!("{}", line.bright_magenta());
    println!(
        "  {} Mock upstream LLM server live on {}",
        "✔".green().bold(),
        format!("http://{UPSTREAM_ADDR}").bold()
    );
    println!("      GET  /upstream/models               5 models, prices & reviews");
    println!("      GET  /upstream/users                initial balances (cents)");
    println!("      POST /upstream/v1/chat/completions  JSON or SSE (stream: true)");
    println!("      GET  /upstream/metrics              upstream cost & per-model counters");
    println!("      POST /upstream/reset                reset upstream counters");
    println!();
    println!("  Your router must serve:");
    println!("      POST http://127.0.0.1:8080/v1/chat/completions");
    println!("      GET  http://127.0.0.1:8080/admin/state");
    println!(
        "  Propagate {} on every response so we can isolate router overhead.",
        "X-Upstream-Simulated-Delay-Ms".bold()
    );
    println!();
}

/// Polls `/admin/state` every second for up to 60s; ENTER skips the wait.
async fn wait_for_router(client: &reqwest::Client) {
    let (tx, mut enter) = mpsc::unbounded_channel::<()>();
    // A plain thread: a blocked stdin read must never hold up runtime shutdown.
    std::thread::spawn(move || {
        let mut line = String::new();
        if matches!(std::io::stdin().read_line(&mut line), Ok(n) if n > 0) {
            let _ = tx.send(());
        }
    });

    println!(
        "  Waiting for your router at {} (up to {ROUTER_WAIT_SECS}s, press ENTER to start now) ...",
        ADMIN_URL.bold()
    );
    for attempt in 1..=ROUTER_WAIT_SECS {
        let healthy = client
            .get(ADMIN_URL)
            .timeout(Duration::from_millis(900))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        if healthy {
            println!("  {} router is healthy (after {attempt}s)", "✔".green().bold());
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            Some(()) = enter.recv() => {
                println!("  {} ENTER pressed, starting without a healthy router", "!".yellow().bold());
                return;
            }
        }
    }
    println!(
        "  {} router not healthy after {ROUTER_WAIT_SECS}s, launching waves anyway",
        "!".yellow().bold()
    );
}

// ---------------------------------------------------------------------------
// Scorecard
// ---------------------------------------------------------------------------

enum Grade {
    PrincipalPlus,
    Principal,
    Junior,
    BelowBar,
    Pass,
    HardFail,
    NoData,
}

impl Grade {
    fn label(&self) -> ColoredString {
        match self {
            Grade::PrincipalPlus => "PRINCIPAL+".bright_green().bold(),
            Grade::Principal => "PRINCIPAL".green().bold(),
            Grade::Junior => "JUNIOR".yellow().bold(),
            Grade::BelowBar => "BELOW BAR".red().bold(),
            Grade::Pass => "PASS".green().bold(),
            Grade::HardFail => "HARD FAIL".on_red().white().bold(),
            Grade::NoData => "NO DATA".red().bold(),
        }
    }
}

/// Grades a lower-is-better metric against strict `<` thresholds.
fn grade(value: Option<f64>, principal_plus: Option<f64>, principal: f64, junior: f64) -> Grade {
    match value {
        None => Grade::NoData,
        Some(v) if principal_plus.is_some_and(|t| v < t) => Grade::PrincipalPlus,
        Some(v) if v < principal => Grade::Principal,
        Some(v) if v < junior => Grade::Junior,
        Some(_) => Grade::BelowBar,
    }
}

fn gate(pass: bool) -> Grade {
    if pass {
        Grade::Pass
    } else {
        Grade::HardFail
    }
}

fn fmt_ms(v: Option<f64>) -> String {
    v.map(|v| format!("{v:.3} ms")).unwrap_or_else(|| "N/A".to_string())
}

fn row(metric: &str, measured: &str, junior: &str, principal: &str, grade: &Grade) {
    println!(
        "  {:<40} {:>14}   {:<10} {:<18} {}",
        metric,
        measured,
        junior,
        principal,
        grade.label()
    );
}

fn detail(msg: &str) {
    println!("  {:<40} {}", "", msg.dimmed());
}

fn print_scorecard(
    report: &ChaosReport,
    admin: &Result<AdminStateResponse, String>,
    metrics: &UpstreamMetrics,
) {
    let line = "═".repeat(100);
    println!();
    println!("{}", line.bright_magenta());
    println!("{}", "  NEXUS EVALUATION SCORECARD".bright_magenta().bold());
    println!("{}", line.bright_magenta());
    println!(
        "  {:<40} {:>14}   {:<10} {:<18} {}",
        "SLA".bold(),
        "MEASURED".bold(),
        "JUNIOR".bold(),
        "PRINCIPAL".bold(),
        "GRADE".bold()
    );
    println!("  {}", "─".repeat(96).dimmed());

    // 1 & 2: internal router overhead
    let p50 = percentile(&report.internal_overhead_samples_ms, 50.0);
    let p99 = percentile(&report.internal_overhead_samples_ms, 99.0);
    row(
        "1. Internal Router Overhead (p50)",
        &fmt_ms(p50),
        "< 15.0ms",
        "< 3.0ms (+ < 1.0)",
        &grade(p50, Some(1.0), 3.0, 15.0),
    );
    row(
        "2. Internal Router Overhead (p99)",
        &fmt_ms(p99),
        "< 40.0ms",
        "< 10.0ms (+ < 3.0)",
        &grade(p99, Some(3.0), 10.0, 40.0),
    );
    detail(&format!(
        "{} samples (total elapsed - X-Upstream-Simulated-Delay-Ms)",
        report.internal_overhead_samples_ms.len()
    ));

    // 3: end-to-end TTFB
    let ttfb_p95 = percentile(&report.ttfb_samples_ms, 95.0);
    row(
        "3. End-to-End TTFB (p95)",
        &fmt_ms(ttfb_p95),
        "< 600ms",
        "< 350ms",
        &grade(ttfb_p95, None, 350.0, 600.0),
    );
    detail(&format!("{} successful responses sampled", report.ttfb_samples_ms.len()));

    // 4: negative balances (hard gate)
    match admin {
        Ok(state) => {
            let negative = state
                .user_balances_cents
                .values()
                .filter(|b| **b < NEGATIVE_EPSILON_CENTS)
                .count();
            row(
                "4. Negative User Balances",
                &format!("{negative} accounts"),
                "0",
                "0 (hard gate)",
                &gate(negative == 0),
            );
            let mut balances: Vec<_> = state.user_balances_cents.iter().collect();
            balances.sort_by(|a, b| a.0.cmp(b.0));
            for (user, cents) in balances {
                let text = format!("{user:<18} {cents:>16.6} cents  (${:.6})", cents / 100.0);
                if *cents < NEGATIVE_EPSILON_CENTS {
                    println!("  {:<40} {}", "", text.red().bold());
                } else {
                    detail(&text);
                }
            }
        }
        Err(e) => {
            row(
                "4. Negative User Balances",
                "N/A",
                "0",
                "0 (hard gate)",
                &Grade::HardFail,
            );
            detail(&format!("/admin/state unavailable: {e}"));
        }
    }

    // 5: corrupted streams (hard gate)
    row(
        "5. Corrupted / Duplicated Streams",
        &format!("{} streams", report.corrupted_streams_count),
        "0",
        "0 (hard gate)",
        &gate(report.corrupted_streams_count == 0),
    );
    detail(&format!(
        "wave 4: {} mid-stream drops injected, {} seamless handovers (assistant_prefix), {}/{} streams complete",
        report.wave4_drops_injected,
        report.wave4_handover_resumes,
        report.wave4_complete,
        report.wave4_streams
    ));

    // 6: unbilled upstream bleed
    let billed_cents = admin
        .as_ref()
        .map(|s| s.total_billed_to_users_cents)
        .unwrap_or(0.0);
    let bleed_usd = (metrics.total_upstream_cost_cents - billed_cents).max(0.0) / 100.0;
    row(
        "6. Unbilled Upstream Bleed (Nexus Loss)",
        &format!("${bleed_usd:.4}"),
        "< $0.50",
        "< $0.15",
        &grade(Some(bleed_usd), None, 0.15, 0.50),
    );
    detail(&format!(
        "upstream cost {:.6} cents | billed to users {:.6} cents",
        metrics.total_upstream_cost_cents, billed_cents
    ));

    println!("  {}", "─".repeat(96).dimmed());

    // Cold start exploration
    let cold_requests = metrics
        .models
        .get(COLD_START_MODEL)
        .map(|m| m.requests)
        .unwrap_or(0);
    let cold_share = if metrics.total_requests == 0 {
        0.0
    } else {
        cold_requests as f64 * 100.0 / metrics.total_requests as f64
    };
    println!(
        "  {:<40} {:>14}   {}",
        "Cold Start Traffic Share",
        format!("{cold_share:.2} %"),
        format!("{cold_requests}/{} upstream calls to {COLD_START_MODEL}", metrics.total_requests)
            .dimmed()
    );

    println!("  {}", "Upstream calls per model:".dimmed());
    for (model, m) in &metrics.models {
        detail(&format!(
            "{model:<22} requests {:>4}  failures {:>3}  drops {:>3}  cost {:.6}¢",
            m.requests, m.failures, m.mid_stream_drops, m.cost_cents
        ));
    }

    if let Ok(Some(latency)) = admin.as_ref().map(|s| s.router_latency_ms.as_ref()) {
        println!(
            "  {:<40} p50 {}  p95 {}  p99 {}",
            "Router-reported latency",
            fmt_ms(latency.p50_ms),
            fmt_ms(latency.p95_ms),
            fmt_ms(latency.p99_ms)
        );
    }

    if report.missing_delay_header > 0 {
        println!(
            "  {} {} successful responses lacked X-Upstream-Simulated-Delay-Ms; full latency counted as overhead",
            "!".yellow().bold(),
            report.missing_delay_header
        );
    }
    if report.transport_errors > 0 {
        println!(
            "  {} {}/{} requests got no HTTP response from :8080",
            "!".yellow().bold(),
            report.transport_errors,
            report.total_requests
        );
    }

    let negatives_ok = admin.as_ref().is_ok_and(|s| {
        s.user_balances_cents
            .values()
            .all(|b| *b >= NEGATIVE_EPSILON_CENTS)
    });
    let gates_ok = negatives_ok && report.corrupted_streams_count == 0;
    println!("{}", line.bright_magenta());
    println!(
        "  HARD GATES: {}",
        if gates_ok {
            "PASS".green().bold()
        } else {
            "FAIL".on_red().white().bold()
        }
    );
    println!("{}", line.bright_magenta());
}
