# Rewind

Rewind is a backfill engine for time-partitioned data. The idea is to make reprocessing
historical data a safe, boring operation instead of the scary manual thing it usually is.

It figures out which partitions actually need to be reprocessed, reruns them into a staging
area without stepping on the live pipeline, checks the staged result against what's already
published, and only then swaps the corrected data in. If the check doesn't pass, nothing
gets published.

The engine runs on a laptop (Postgres + Parquet + DuckDB). There's also an optional heavier
stack — Iceberg, Kafka, Debezium, Airflow — that I added once the core was solid, behind
flags/compose overlays so the lean version still works on its own.

## Why I built it

Every data team I've looked at handles backfills the same way: someone reruns a month of the
ingestion job by hand and hopes for the best. That's fine until it isn't. The actual hard
part was never rerunning the job — it's getting it *right*:

- if the backfill overlaps whatever the live pipeline is currently writing, you double-count
- if it isn't idempotent, running it twice gives you two different answers
- and you usually don't find out the numbers are wrong until a dashboard looks off three
  weeks later

There are tools that each own a slice of this. Airflow will happily schedule historical runs
but won't stop you double-counting. Soda / Great Expectations will tell you two datasets
disagree, but they don't *produce* the correct data. Delta/Iceberg give you snapshots to
read from, but you're still the one deciding scope and avoiding collisions. Rewind is me
gluing scope → isolated reprocessing → idempotent swap → a reconciliation gate into one
thing, with a few guarantees it actually enforces:

1. it only touches the minimum set of partitions, computed from source watermarks and the
   change you're making, and it skips anything the live stream currently owns (the "hot
   window")
2. reprocessing goes to staging — the recent partitions the stream is still writing never
   get touched
3. republishing the same partition gives the same result; retries are no-ops
4. nothing publishes until reconciliation (row counts, key-set diff, control totals) passes

## How it fits together

```
   Engineer / CLI / UI
        │  "backfill fct_revenue 2026-09-20..2026-11-03, transform v7"
        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         Rewind Control Plane (FastAPI)                     │
│                                                                            │
│  ┌───────────────┐   ┌────────────────┐   ┌───────────────┐  ┌──────────┐ │
│  │ Scope Planner │──▶│ Reprocessing   │──▶│ Reconciliation│─▶│ Publisher│ │
│  │ (which        │   │ Executor       │   │ Engine        │  │ (atomic  │ │
│  │  partitions?) │   │ (idempotent,   │   │ counts,       │  │  swap +  │ │
│  │  watermark +  │   │  staged,       │   │ key-set diff, │  │  ledger) │ │
│  │  hot window)  │   │  parallel)     │   │ control totals│  │          │ │
│  └──────┬────────┘   └──────┬─────────┘   └──────┬────────┘  └────┬─────┘ │
└─────────┼───────────────────┼────────────────────┼────────────────┼───────┘
          │                   │ staging partitions  │ verdict        │ swap
          ▼                   ▼                      ▼                ▼
   ┌──────────────┐   ┌────────────────────────────────────────────────────┐
   │ Control DB   │   │  Data Plane (partitioned Parquet on local disk)     │
   │ (PostgreSQL) │   │  raw/ (event source + live stream)                  │
   │ runs, ledger,│   │  staging/ (backfill output)                         │
   │ recon results│   │  published/ (live-readable, pointer-swapped)        │
   └──────────────┘   └────────────────────────────────────────────────────┘
       DuckDB reads Parquet for reconciliation queries (counts, diffs, sums)
```

Control plane is FastAPI + Postgres (asyncpg). Data lives as partitioned Parquet on disk —
I wanted something genuinely columnar and partitioned without a cloud bill. DuckDB does the
reconciliation queries straight over the Parquet files. Pydantic for request/response,
Docker Compose to bring it up, pytest for the correctness tests (that last part is really
the whole point — see below).

## The heavier stack (added later, optional)

I started lean on purpose and only reached for the big DE tools once the correctness story
held up on its own. Each one earns its place:

- **Iceberg** replaces the filesystem pointer-swap with real atomic snapshots + time travel.
  Opt in with `PUBLISHER_BACKEND=iceberg`; the pointer backend is still the default.
- **Kafka** carries the live event stream that feeds the hot window — so the
  "don't collide with the stream" guarantee is tested against an actual stream, not a script
  appending files.
- **Debezium** does CDC off the Postgres *control plane*, so the backfill lifecycle
  (planned → running → reconciling → publishable → published) streams out as real-time
  events. It's CDC off my own metadata, which is a slightly unusual but legit use.
- **Airflow** wraps the whole lifecycle as a DAG, with the reconciliation gate implemented
  as a branch — it literally can't route to "publish" unless the run came back publishable.

I deliberately did **not** add PySpark. Spark would be pure resume-padding on a dataset this
size, and the executor already has a clean seam where a distributed version would slot in.

## Layout

```
Rewind/
├── docker-compose.yml             # Postgres (wal_level=logical) + API
├── docker-compose.streaming.yml   # optional: Kafka + Debezium + Kafka-UI + Airflow
├── rewind/
│   ├── config.py                  # settings: data root, zones, publisher backend
│   ├── db/                        # asyncpg client + SQL migrations
│   ├── dataplane/                 # partitioned Parquet read/write + pointer swap
│   ├── registry/                  # table registry
│   ├── planner/                   # scope planner — which partitions to reprocess
│   ├── transform.py               # the reprocessing transform (swap-in seam)
│   ├── executor/                  # reprocessing executor (idempotent, staged, parallel)
│   ├── publisher/                 # pointer-swap + ledger publisher; iceberg_backend
│   ├── reconciliation/            # DuckDB counts / key diff / control totals
│   ├── orchestrator.py            # the backfill state machine
│   ├── cli.py                     # plan / create / run / publish / status
│   ├── stream/                    # Kafka producer+consumer, Debezium register+consume
│   └── api/                       # FastAPI control plane
├── airflow/dags/                  # rewind_backfill DAG
├── frontend/                      # Next.js control room (Planner / Run Monitor / Recon Diff)
├── scripts/                       # simulators, benchmark, guided demo
└── tests/
```

On disk the data plane looks like this — raw, staging, and published each hold the same
partition layout, and `published/` has a pointer file that's what the atomic swap flips:

```
data/
├── raw/fct_revenue/dt=2026-09-20/part.parquet
├── staging/fct_revenue/dt=2026-09-20/part.parquet
└── published/fct_revenue/
    ├── dt=2026-09-20/part.parquet
    └── _pointer.json
```

The control DB is five tables: `tables_registry`, `backfills`, `partition_tasks`,
`reconciliation`, and `publish_ledger` (the last one is the idempotency guard, keyed on
table + partition + transform version).

## Running it

You need Docker and Python 3.12+ (I'm on 3.13 locally and it's fine).

```bash
cp .env.example .env
docker compose up -d postgres
python -m venv .venv && .venv\Scripts\Activate.ps1   # PowerShell
pip install -r requirements.txt
python -m rewind.db.migrate
python -m rewind.registry.registry        # registers the demo table fct_revenue
```

Generate some history, then run a backfill through the CLI:

```bash
python -m scripts.simulate_source --table fct_revenue --days 60

python -m rewind.cli plan    --table fct_revenue --start 2026-09-20 --end 2026-11-02 --transform v7
python -m rewind.cli create  --table fct_revenue --start 2026-09-20 --end 2026-11-02 --transform v7
python -m rewind.cli run     --id 1        # stages + reconciles -> publishable or failed
python -m rewind.cli publish --id 1        # only swaps if reconciliation passed
python -m rewind.cli status  --id 1
```

API and UI:

```bash
uvicorn rewind.api.main:app --port 8010    # docs at /docs
cd frontend && npm install && npm run dev  # http://localhost:3000
```

The UI has three screens — Planner (preview the exact scope before touching anything),
Run Monitor (per-partition task status), and a Reconciliation Diff (counts, key-set delta,
control totals, pass/fail).

There's a guided demo script that walks all four guarantees end to end and cleans up after
itself — probably the fastest way to see what the thing does:

```bash
python -m scripts.demo                              # pointer backend
$env:PUBLISHER_BACKEND="iceberg"; python -m scripts.demo   # Iceberg snapshots + time travel
```

Config lives in `.env` — Postgres on 5440, API on 8010 (odd ports so it doesn't fight other
local projects), `DATA_ROOT=./data`, and `HOT_WINDOW_DAYS=2` (how many recent days the stream
owns and backfills leave alone).

## Tests

The test suite is the part I care about most — it's what makes the correctness claims more
than marketing. 40-odd tests covering idempotency, concurrency, fault injection, crash
recovery, and the reconciliation gate, plus a seeded throughput benchmark.

```bash
docker compose up -d postgres
python -m pytest -q
python -m scripts.benchmark --days 90 --rows 2000 --concurrency 8
```

## The streaming stack

If you want to see Kafka/Debezium/Airflow in action, bring up the overlay (heads up: the
images are a chunky pull the first time):

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build
python -m rewind.db.migrate

# stream events into the hot window
python -m rewind.stream.producer --table fct_revenue --count 50
python -m rewind.stream.consumer --table fct_revenue --drain

# CDC off the control plane
python -m rewind.stream.register_cdc
python -m rewind.stream.cdc_consumer --drain

# Airflow DAG at http://localhost:8088 (admin/admin), trigger with conf:
#   {"table":"fct_revenue","range_start":"2026-09-20",
#    "range_end":"2026-11-02","transform_version":"v7"}
```

Ports: API 8010, Kafka 9092, Kafka Connect 8083, Kafka-UI 8085, Airflow 8088, Postgres 5440.
`docker compose -f docker-compose.yml -f docker-compose.streaming.yml down -v` to tear it all
down including the CDC slot.

None of the streaming stuff is required — the engine, the Iceberg backend, and the whole test
suite run against Postgres alone. Kafka/Debezium/Airflow add realism, not new ideas.

Deploy notes (Vercel for the frontend, Render for the API+DB) are in `DEPLOY.md`.

## Things I'd call out honestly

- The default "atomic swap" is a `_pointer.json` write-then-replace. It mirrors how Iceberg
  does manifest swaps, but on a plain filesystem it's not the same as object-store rename
  atomicity. The Iceberg backend is the real version of this — that's why I built it.
- Idempotent republish is content-based, not just keyed on the ledger. The ledger key
  (table, partition, transform version) alone turned out to be too coarse — late data under
  the *same* transform version has to republish even though the key didn't change. The demo
  script actually caught that bug, which is a nice argument for having the demo.
- The scope planner assumes a single source dependency per table. Arbitrary multi-source DAGs
  are out of scope — that'd be a real project on its own.
- Every number in here (throughput, row counts) comes from a seeded reproducible run. I
  didn't make any up.
