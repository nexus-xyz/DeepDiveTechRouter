# DeepDiveTechRouter · `nexus-harness`

Black-box test environment for the Nexus AI Router technical interview.

Candidates have **60 minutes** to build, in any language/framework:

1. An **AI Router** on `http://127.0.0.1:8080`
2. A **Live Ops Frontend Dashboard**

Requires Rust 1.89+.

```bash
cargo run --release
```

One process does two things:

1. **Mock upstream LLM server** on `http://127.0.0.1:8081`, also on `[::1]:8081` so `localhost` works (5 models, JSON + SSE, latency, failures, mid-stream drops, exact upstream cost ledger).
2. **5-wave chaos generator & auditor** against `:8080`. It waits up to 60s for `GET /admin/state` to answer 2xx (or until you press ENTER), runs the waves, then prints the **NEXUS EVALUATION SCORECARD**.

The upstream stays live after the scorecard (Ctrl+C to exit) so the dashboard can keep reading it.

## Upstream API (`:8081`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/upstream/models` | 5 models with prices (USD / 1M tokens) and reviews |
| GET | `/upstream/users` | Initial balances in **cents** |
| POST | `/upstream/v1/chat/completions` | `{model_id, prompt, max_output_tokens, stream, assistant_prefix?}` |
| GET | `/upstream/metrics` | Upstream cost and per-model counters |

Every chat response (including `503`) carries `X-Upstream-Simulated-Delay-Ms`: the time the upstream actually spent on that call (for streams, measured time to first byte plus the token phase).

| Model | Behaviour |
|---|---|
| `gpt-4o-mini` | Stable, 30ms TTFB |
| `claude-3-5-sonnet` | Stable, 45ms TTFB |
| `deepseek-v3` | 35% `503`; 35% of streams drop after 3 tokens with `ERR_STREAM_DISCONNECTED_BY_UPSTREAM` (tokens still billed); 30% normal |
| `llama-3-70b-fast` | 25ms TTFB for calls 1-5, then 4000ms |
| `nexus-experimental-v1` | Cold start (0 reviews), 20ms TTFB |

**Mid-stream handover:** send `assistant_prefix` with the text the client already received (for example `"The quick brown "`) and the upstream streams only the remaining tokens.

Cost in cents: `(input_tokens * input_price + output_tokens * output_price) / 10_000`, with `input_tokens = max(1, len(prompt) / 4)`, where `len` counts UTF-8 bytes.

## Router contract (`:8080`)

`POST /v1/chat/completions`

```json
{"request_id":"...","idempotency_key":null,"user_id":"user_whale","prompt":"...","max_output_tokens":64,"min_review_score":4.3,"stream":true}
```

- `stream: false` returns one JSON `StreamChunk`: `{"token":"<full text>","is_final":true,"model_used":"...","usage":{...}}`
- `stream: true` returns `text/event-stream` frames `data: <StreamChunk JSON>\n\n`
- Propagate `X-Upstream-Simulated-Delay-Ms` as the **sum** over every upstream call made for the request (including `503` retries). Without it, the full latency counts as router overhead.
- Streams send headers before a mid-stream failover can happen, so the final chunk may carry `"upstream_delay_ms": <total>`. When present it replaces the header value.
- A delay larger than the request's own end-to-end time is ignored and the full latency counts as overhead.
- Use `402` for insufficient balance and `4xx` for invalid or unroutable requests.
- Requests sharing an `idempotency_key` must trigger at most one billable upstream generation and charge the user once.

`GET /admin/state`

```json
{"user_balances_cents":{"user_whale":9999.98},"total_billed_to_users_cents":0.02,"router_latency_ms":{"p50_ms":0.4,"p95_ms":1.2,"p99_ms":2.0}}
```

## Waves

1. **Financial precision:** 40 non-stream requests, sub-cent deductions.
2. **Double-spend burst:** 20 simultaneous requests for `user_low_balance` (0.05¢) with `min_review_score: 4.8`, so only `claude-3-5-sonnet` (~0.0168¢ per reply) qualifies. Upstream spend must stay within the balance.
3. **Idempotency storm:** 10 simultaneous identical requests sharing `idempotency_key`; at most one generation, charged once.
4. **Upstream chaos:** 60 concurrent streams; every stream must deliver the full sentence with no duplicated or restarted text.
5. **Malformed/unroutable:** 10 invalid requests; each must get `4xx` and never be charged.

## Scorecard

| # | SLA | Junior | Principal |
|---|---|---|---|
| 1 | Internal router overhead p50 | < 15ms | < 3ms (+ < 1ms) |
| 2 | Internal router overhead p99 | < 40ms | < 10ms (+ < 3ms) |
| 3 | End-to-end TTFB p95 | < 600ms | < 350ms |
| 4 | Negative user balances / wave 2 overspend | **0 (hard gate)** | **0 (hard gate)** |
| 5 | Corrupted / duplicated streams | **0 (hard gate)** | **0 (hard gate)** |
| 6 | Failed / incomplete valid requests (waves 1 & 4) | **0 (hard gate)** | **0 (hard gate)** |
| 7 | Unbilled upstream bleed (% of upstream cost) | < 5% | < 1% |

Overhead and TTFB are sampled from `2xx` replies. Billing for row 7 is read from balance changes in `/admin/state` (against `/upstream/users`), not from `total_billed_to_users_cents`.

The scorecard also reports the **Cold Start Traffic Share** sent to `nexus-experimental-v1`.

## License

[MIT](LICENSE)
