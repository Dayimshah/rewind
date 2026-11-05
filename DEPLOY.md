# Deploying Rewind

Rewind is heavier than a typical web app (the full engine uses Kafka/Debezium/
Airflow). For a public portfolio deployment you do **not** deploy those — you
deploy the frontend + API + Postgres, and show the streaming/orchestration stack
via a local demo or recording. This is the standard, honest split.

## What goes where

| Piece | Host | Cost | Notes |
|---|---|---|---|
| Frontend (Next.js) | Vercel | free, always on | works with **no backend** (demo mode) |
| API (FastAPI) | Render (Docker) | free | sleeps after ~15 min idle, cold-starts ~30-60s |
| Control DB (Postgres) | Render Postgres | free | always on |
| Kafka / Debezium / Airflow | **not deployed** | — | run locally; show via recording |

## 1. Frontend → Vercel (always live, even with no backend)

The UI falls back to seeded "demo mode" data whenever the API is unreachable or
asleep, so the deployed page always renders.

```bash
# from the repo root
cd frontend
# Vercel: import the repo, set the project root to "frontend"
# Build command / framework are picked up from vercel.json (Next.js)
```

Set one env var in Vercel (optional):

- `NEXT_PUBLIC_REWIND_API` = your deployed API URL (e.g. `https://rewind-api.onrender.com`).
  If you leave it unset or the API is asleep, the UI shows seeded data with a
  "demo mode" banner.

## 2. API + Postgres → Render (the "it really runs" story)

A `render.yaml` blueprint is included. On render.com: **New + → Blueprint →**
pick this repo. Render will:

- provision a free Postgres 16 instance and inject `DATABASE_URL`,
- build the `Dockerfile`,
- run migrations on boot (`RUN_MIGRATIONS_ON_START=1`),
- seed a demo `fct_revenue` backfill through to `published` (`SEED_ON_START=1`),
- expose a public URL with a `/health` check.

Then set that URL as `NEXT_PUBLIC_REWIND_API` in Vercel and redeploy the frontend.

### Env vars the API understands
- `DATABASE_URL` — Postgres connection string (required).
- `DATA_ROOT` — where Parquet lives (default `/app/data`; ephemeral on free tier,
  re-seeded each boot — fine for a demo).
- `RUN_MIGRATIONS_ON_START` / `SEED_ON_START` — `1` to run on boot (default off
  locally, on in `render.yaml`).
- `CORS_ORIGINS` — comma-separated allowed origins, or `*` for a public demo.
- `PUBLISHER_BACKEND` — `pointer` (default) or `iceberg`.

## 3. Streaming + orchestration (local / recorded)

Kafka, Debezium, and Airflow are not worth paying to host 24/7. Run them locally
to demo, and link a short screen recording from the README:

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
# Airflow UI http://localhost:8088 (admin/admin), Kafka-UI http://localhost:8085
```

## Honest notes
- The Render free web tier **sleeps**; first request after idle takes ~30-60s.
  The frontend handles this (4s timeout → demo mode), so the link never looks broken.
- Free-tier disk is **ephemeral**; the demo data is re-seeded on each cold start.
  That's intentional for a showcase — not a claim of durable storage.
