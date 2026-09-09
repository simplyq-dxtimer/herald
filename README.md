# Herald

Your agent doesn't need to be always-on. Herald is.

Lightweight webhook relay and message queue for AI agents and local services that can't expose a public endpoint. Give your agent a stable URL. Herald receives, queues, and delivers.

**[herald.tools](https://herald.tools)** | **[Specification](SPEC.md)** | **[proxy.herald.tools](https://proxy.herald.tools/health)**

## How it works

```
GitHub/Stripe/etc.  ──POST──▶  Herald  ◀──poll/ws──  Your Agent
                               (queue)                (when ready)
```

1. Point webhooks at `https://proxy.herald.tools/<you>/<endpoint>`
2. Herald encrypts and queues them (FIFO, deduplicated)
3. Your agent polls or streams via WebSocket when it's ready
4. ACK processed messages. Failed? Requeued or sent to DLQ.

## Quick start

```bash
# 1. Register and get an API key
curl -X POST https://proxy.herald.tools/register \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"myagent"}'
# → 201 {"object":"account","customer_id":"myagent","api_key":"hrl_sk_...","created":1775183297}

# 1b. (Optional) Register with ingest auth — providers must authenticate
curl -X POST https://proxy.herald.tools/register \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"myagent","ingest_auth":{"type":"hmac","key":"signing-secret","header":"X-Hub-Signature-256"}}'

# 2. Send a webhook (if ingest_auth configured, provider must authenticate)
curl -X POST https://proxy.herald.tools/myagent/github \
  -H "Content-Type: application/json" \
  -d '{"action":"push","ref":"refs/heads/main"}'

# 3. Poll for messages (auth required)
curl -H "Authorization: Bearer $API_KEY" \
  https://proxy.herald.tools/endpoints/github/messages
# → {"object":"list","data":[{"object":"message","message_id":"msg_...","body":"...","received_at":1775183297,...}],"has_more":false,"queue_depth":0}

# 4. Acknowledge processed messages
curl -X POST -H "Authorization: Bearer $API_KEY" \
  https://proxy.herald.tools/endpoints/github/messages/<msg_id>/ack

# 5. NACK to retry, or to send to the DLQ
curl -X POST -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"disposition":"requeue"}' \
  https://proxy.herald.tools/endpoints/github/messages/<msg_id>/nack
```

## Self-hosting

Herald is a single Rust binary + Redis.

```bash
# Clone and build
git clone https://github.com/jmcentire/herald.git
cd herald
cargo build --release -p herald-server

# Run (requires Redis)
HERALD_REDIS_URL=redis://127.0.0.1/ \
HERALD_ENCRYPTION_KEY=$(openssl rand -hex 32) \
./target/release/herald-server
```

### Split listeners

By default every route is served on `HERALD_LISTEN_ADDR`. Set
`HERALD_ADMIN_LISTEN_ADDR` to split the surface across two addresses:

| Listener | Routes | Exposure |
| --- | --- | --- |
| `HERALD_LISTEN_ADDR` | `POST /<customer>/<endpoint>`, `/stripe/webhook`, `/health` | public — providers must reach it |
| `HERALD_ADMIN_LISTEN_ADDR` | `/register`, `/account/*`, `/endpoints/*` (poll, ack, nack, stream), `/health` | private — bind to loopback, a VPN address, or a private subnet |

Anything not served on a listener returns 404 there, so a leaked API key alone
does not let queued payloads be read from the internet.

```bash
HERALD_LISTEN_ADDR=0.0.0.0:8080 \
HERALD_ADMIN_LISTEN_ADDR=100.90.105.9:8081 \
HERALD_REGISTER_SECRET=$(openssl rand -hex 32) \
./target/release/herald-server
```

Leave `HERALD_ADMIN_LISTEN_ADDR` unset to keep the single-listener behavior.

## herald-cli

Optional local daemon that polls Herald and invokes your agent.

```yaml
# ~/.config/herald/config.yaml
server: https://proxy.herald.tools
api_key: hrl_sk_...
connection: websocket

handlers:
  github-push:
    command: claude
    args: ["-p"]
    prompt_template: |
      Use kindex to search for context.
      Then handle this event: {{.body}}
    stdin: prompt
    hooks:
      pre:
        command: kindex
        args: ["ingest", "--tags", "herald", "--stdin"]
```

```bash
cargo build --release -p herald-cli
./target/release/herald-cli run
```

## Stack

- **nginx** — edge, TLS, rate limiting
- **Rust** — tokio + axum, zero-cost abstractions
- **Redis** — FIFO queues, in-flight tracking, pub/sub

## Features

- Encrypted on receipt (AES-256-GCM, no plaintext at rest)
- Content-addressable deduplication (SHA-256)
- At-least-once delivery with visibility timeout
- Dead letter queue after configurable retries
- WebSocket streaming with first-message auth
- Pluggable storage (Redis, PostgreSQL, SQLite, filesystem)
- BYOK encryption (Pro tier)
- Self-hostable, MIT licensed

## Ecosystem

Herald is the ears. [Kindex](https://kindex.tools) is the memory. Your agent is the brain.

Part of the [Exemplar](https://exemplar.tools) stack.

## License

MIT
