# DeepDiveTechRouter · `nexus-harness`

Test environment for the Nexus AI Router live coding interview.

## The exercise

Nexus sits between its users and several third-party LLM providers. In the live session you will have **60 minutes** to build, in any language/framework:

1. An **AI Router** on `http://127.0.0.1:8080` that takes chat requests from Nexus users, picks a suitable upstream model, relays the response (JSON or streaming) and charges the user's balance for what was consumed.
2. A **Live Ops Dashboard** (frontend) that lets an operator see what the router and the upstream are doing.

Real providers are not equally fast, cheap or reliable, and real traffic is not always well behaved. A production-grade router is expected to stay correct, fast and financially sound regardless. The detailed requirements are given at the start of the session.

### Before the session

- Install Rust 1.89+ and run this harness once (below) to make sure it works on your machine.
- Set up the language, framework and tooling you want to use, so you can start writing the router immediately.
- Read the upstream API and the router contract below. You do not need to build anything in advance.

## Running the harness

```bash
cargo run --release
```

One process does two things:

1. **Mock upstream LLM server** on `http://127.0.0.1:8081` (also `[::1]:8081`, so `localhost` works). It simulates the third-party providers and keeps an exact ledger of what Nexus owes them.
2. **Traffic generator** against `:8080`. It waits up to 60s for `GET /admin/state` to answer 2xx (or until you press ENTER), sends a series of requests to your router, then prints a summary.

The upstream stays live after the summary (Ctrl+C to exit) so your dashboard can keep reading it.

## Upstream API (`:8081`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/upstream/models` | Available models with prices (USD / 1M tokens) and review score/count |
| GET | `/upstream/users` | Nexus users and their initial balances in **cents** |
| POST | `/upstream/v1/chat/completions` | `{model_id, prompt, max_output_tokens, stream, assistant_prefix?}` |
| GET | `/upstream/metrics` | Upstream cost and per-model counters |

- `stream: true` returns `text/event-stream` frames `data: <StreamChunk JSON>\n\n`; `stream: false` returns a single `StreamChunk` with the full text.
- `assistant_prefix` (optional): text that was already generated for this prompt. The upstream continues from there instead of starting over.
- Every chat response carries `X-Upstream-Simulated-Delay-Ms`: the time the upstream spent on that call.
- Each model behaves differently (latency, availability, stream stability), like real providers do.

Upstream cost in cents: `(input_tokens * input_price + output_tokens * output_price) / 10_000`, with `input_tokens = max(1, len(prompt) / 4)`, where `len` counts UTF-8 bytes.

## Router contract (`:8080`)

`POST /v1/chat/completions`

```json
{"request_id":"...","idempotency_key":null,"user_id":"user_whale","prompt":"...","max_output_tokens":64,"min_review_score":4.3,"stream":true}
```

- `stream: false` returns one JSON `StreamChunk`: `{"token":"<full text>","is_final":true,"model_used":"...","usage":{...}}`
- `stream: true` returns `text/event-stream` frames `data: <StreamChunk JSON>\n\n`
- Only models with `review_score >= min_review_score` may serve the request.
- Return the total upstream time spent on the request in `X-Upstream-Simulated-Delay-Ms`, summed over every upstream call you made for it. For streams, the final chunk may also carry `"upstream_delay_ms": <total>`, which replaces the header value.
- Use `402` for insufficient balance and `4xx` for invalid or unroutable requests.

`GET /admin/state`

```json
{"user_balances_cents":{"user_whale":9999.98},"total_billed_to_users_cents":0.02,"router_latency_ms":{"p50_ms":0.4,"p95_ms":1.2,"p99_ms":2.0}}
```

## License

[MIT](LICENSE)
