# Deploying Herald

Self-hosted Herald on `production-kafka01.fsn.supportwing.app`
(188.245.204.151, Hetzner arm64, Ubuntu 24.04), managed with
[Kamal](https://kamal-deploy.org) 2.

## Topology

```
internet :443 ──▶ kamal-proxy ──▶ herald :8080     ingest only
                  (Let's Encrypt)                  POST /<customer>/<endpoint>
                  herald.supportwing.app           GET  /health

tailnet 100.90.105.9:8081 ─┐
loopback 127.0.0.1:8081 ───┴▶ herald-admin-gw ──▶ herald :8081    management
                              (socat, stable)     /register, /account/*
                                                  /endpoints/* poll·ack·stream

                                 herald-redis      accounts, API keys,
                                 (kamal network)   encrypted payloads
```

The app container publishes **no** host ports and is reached on the `kamal`
network — by kamal-proxy for ingest, and by `herald-admin-gw` (a socat forwarder
holding the tailnet binding) for management. See *Gotchas* for why the admin
port cannot live on the app container.

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

- **Never give the app role a fixed `publish`.** Kamal boots the replacement
  container before retiring the old one, so a fixed host port collides with
  itself on the second deploy — `Bind for 100.90.105.9:8081 failed: port is
  already allocated`, the new container never starts, and `kamal setup` will
  have appeared to work because nothing held the port the first time. The
  tailnet binding lives on the `admin-gw` accessory, which is not recreated per
  deploy; the app is reached through the `herald-app` network alias.
- `herald-admin-gw` binds `100.90.105.9`, a `tailscale0` address. If
  `tailscaled` is down when Docker restores containers at boot, that publish
  fails and the accessory will not start. It also binds `127.0.0.1:8081`, so
  `ssh -L 8081:127.0.0.1:8081 simplyqops@188.245.204.151` is the way back in.
- `kamal-proxy` takes host ports 80 and 443. Nothing else on this host binds
  them today.
- Hetzner's **cloud firewall** sits in front of ufw and is not visible from the
  host. ufw showed 80/443 allowed while the cloud firewall silently dropped
  them. To tell the two apart, bind a listener and probe from outside:
  `refused` means the packet reached the host, `timeout` means it was dropped
  upstream. Always probe a known-closed port as a control.

## Known upstream behavior

Ingest is unauthenticated by design, and upstream accepts a webhook for **any**
`customer_id`, registered or not — `lookup_tier` defaults unknown ids to Free
and carries on. On a public endpoint that means anyone can create queues and
persist payloads; internet scanners probing `/api/graphql` did exactly that
within seconds of this deployment going live. Per-customer rate limits do not
help, because the caller chooses the `customer_id`.

This fork rejects ingest for unregistered customers (404, nothing written). If
you rebase onto upstream, keep that check. It has not been reported upstream.
