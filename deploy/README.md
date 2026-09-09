# Deploying Herald

Self-hosted Herald on `production-kafka01.fsn.supportwing.app`
(188.245.204.151, Hetzner arm64, Ubuntu 24.04), managed with
[Kamal](https://kamal-deploy.org) 2.

## Topology

```
internet :443 ──▶ kamal-proxy ──▶ herald :8080     ingest only
                  (Let's Encrypt)                  POST /<customer>/<endpoint>
                  herald.supportwing.app           GET  /health

tailnet 100.90.105.9:8081 ─────▶ herald :8081      management
loopback 127.0.0.1:8081                            /register, /account/*
                                                   /endpoints/* poll·ack·stream

                                 herald-redis      accounts, API keys,
                                 (kamal network)   encrypted payloads
```

The split is enforced by the server itself (`HERALD_ADMIN_LISTEN_ADDR`), not by
a proxy rule: routes absent from a listener return 404 there. A leaked API key
therefore cannot drain queues from the internet — the caller also has to be on
the tailnet.

The host runs one unrelated workload, `burty-chromadb` (bound to
`10.0.0.3:8070`). It shares the `kamal` Docker network but is not managed by
this config.

## Prerequisites

- `kamal` 2.x locally, Docker with buildx (arm64 host, so an Apple Silicon Mac
  builds natively).
- SSH access as `simplyqops` with `~/.ssh/simplyqops`.
- DNS: `herald.supportwing.app` A → `188.245.204.151`, **DNS-only** (grey
  cloud). Cloudflare proxying would break the Let's Encrypt HTTP-01 challenge
  that kamal-proxy performs.

## Secrets

`.kamal/secrets` reads from the shell environment. Real values live in
`.env.deploy` (chmod 600, gitignored):

| Variable | Purpose |
| --- | --- |
| `KAMAL_REGISTRY_PASSWORD` | Docker Hub token for `simplyqops` |
| `HERALD_ENCRYPTION_KEY` | AES-256-GCM key, 64 hex chars |
| `HERALD_REGISTER_SECRET` | Bearer token required by `POST /register` |
| `REDIS_PASSWORD` | Redis `requirepass` |
| `HERALD_REDIS_URL` | `redis://:<REDIS_PASSWORD>@herald-redis:6379/` |

> `HERALD_ENCRYPTION_KEY` is unrecoverable. Lose it and every queued payload is
> permanently undecryptable. Keep a copy in the password manager, not only here.

## Deploy

```bash
source .env.deploy

kamal setup     # first time: installs Docker bits, boots proxy + redis, deploys
kamal deploy    # subsequent releases
```

## Operating

```bash
kamal app logs -f              # follow logs
kamal app details              # container status
kamal proxy logs               # TLS / routing, incl. cert issuance
kamal accessory logs redis
kamal rollback <version>
```

Registration is gated, so create accounts over the tailnet:

```bash
curl -X POST http://100.90.105.9:8081/register \
  -H "Authorization: Bearer $HERALD_REGISTER_SECRET" \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"myagent"}'
```

Point providers at `https://herald.supportwing.app/myagent/<endpoint>`, and
poll from the tailnet at `http://100.90.105.9:8081/endpoints/<endpoint>/messages`.

## Backups

Everything durable is in the Redis accessory volume,
`/home/simplyqops/herald-redis/data` on the host (appendonly + RDB).

```bash
ssh -i ~/.ssh/simplyqops simplyqops@188.245.204.151 \
  'sudo tar czf - -C /home/simplyqops/herald-redis data' > herald-redis-$(date +%F).tar.gz
```

## Gotchas

- The admin port is published to `100.90.105.9`, a `tailscale0` address. If
  `tailscaled` is not up when Docker restores containers at boot, that publish
  fails and the container will not start. `127.0.0.1:8081` is published too, so
  `ssh -L 8081:127.0.0.1:8081` is the fallback path in.
- `kamal-proxy` takes host ports 80 and 443. Nothing else on this host binds
  them today.
