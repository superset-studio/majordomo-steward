# majordomo-steward

LLM API proxy that routes requests to upstream providers (OpenAI, Anthropic, Gemini), logs usage to a local PostgreSQL database, and syncs records back to Butler.

## How it works

The steward sits between your application and the upstream LLM API. Your code sends requests to the steward with an `X-Majordomo-Key` header; the steward validates the key, proxies the request, logs token usage and cost, then asynchronously syncs those records to Butler.

```
your app  →  steward (:7680)  →  OpenAI / Anthropic / Gemini
                ↓
          PostgreSQL (local log)
                ↓  (background sync)
            Butler API
```

## Running

```bash
make run
```

Or with Docker:

```dockerfile
docker run \
  --env-file .env \
  -p 7680:7680 \
  ghcr.io/superset-studio/majordomo-steward:latest
```

Migrations run automatically on startup — no separate step needed.

## Configuration

Copy `.env.example` to `.env` and fill in the required values.

| Variable | Required | Description |
|---|---|---|
| `PORT` | | Server port (default: `7680`) |
| `POSTGRES_HOST` | yes | PostgreSQL host |
| `POSTGRES_PORT` | | PostgreSQL port (default: `5432`) |
| `POSTGRES_USER` | yes | PostgreSQL user |
| `POSTGRES_PASSWORD` | yes | PostgreSQL password |
| `POSTGRES_DB` | yes | PostgreSQL database name |
| `ENCRYPTION_KEY` | yes | AES-256 key for encrypting provider keys, hex-encoded (64 chars) |
| `STEWARD_ADMIN_TOKEN` | | Enables the admin API — required to use `majordomo steward` commands |
| `BATCH_INTERVAL` | | How often to sync records to Butler (default: `60s`) |
| `BATCH_MAX_SIZE` | | Max records per sync batch (default: `500`) |
| `WORK_TICK_INTERVAL` | | How often to poll Butler for work (sync notifications + replay/eval jobs) via `GET /work` (default: `30s`) |
| `WORK_TICK_LIMIT` | | Max jobs returned per `GET /work` (default: `25`) |
| `LOG_LEVEL` | | `debug`, `info`, `warn`, `error` (default: `info`) |

Generate the encryption key:

```bash
openssl rand -hex 32
```

### Managed steward

If running as a Majordomo-hosted steward (org assignments managed by Butler automatically):

| Variable | Description |
|---|---|
| `MANAGED_ENABLED` | Set to `true` to enable managed mode |
| `MANAGED_MASTER_TOKEN` | `mdm_st_...` token issued by Butler for this managed steward |
| `MANAGED_BUTLER_URL` | Butler API base URL |
| `MANAGED_POLL_INTERVAL` | How often to poll for new org assignments (default: `30s`) |

### Provider URL overrides

Override upstream base URLs to route through a local proxy or mock during development:

```
OPENAI_BASE_URL=https://api.openai.com
ANTHROPIC_BASE_URL=https://api.anthropic.com
GEMINI_BASE_URL=https://generativelanguage.googleapis.com
```

## Registering orgs

Once the steward is running, register an org with it using the CLI:

```bash
majordomo steward register --token mdm_st_... --butler-url https://butler.example.com
```

This tells the steward which Butler to sync usage data to. The steward will immediately start pulling API keys and syncing logs.

To see registered orgs and their pending sync counts:

```bash
majordomo steward orgs
```

To remove an org:

```bash
majordomo steward deregister <org-id>
```

These commands require `STEWARD_ADMIN_TOKEN` to be set. See the [CLI README](../majordomo-cli/README.md) for full details.

## Proxying requests

Send requests to the steward exactly as you would to the upstream provider, adding an `X-Majordomo-Key` header:

```bash
curl http://localhost:7680/v1/chat/completions \
  -H "X-Majordomo-Key: mdm_..." \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Hello"}]}'
```

The steward auto-detects the provider from the request path. Anthropic and Gemini requests follow the same pattern with their respective paths.

## Development

```bash
make build    # Build binary
make run      # Build and run (localhost:7680)
make test     # Run tests
make lint     # Run golangci-lint
```
