-- Rewind control plane — initial schema (Week 1).
-- Mirrors the "Control DB schema (summary)" section of the README.

-- A registered time-partitioned table that Rewind can backfill.
CREATE TABLE IF NOT EXISTS tables_registry (
    name              TEXT PRIMARY KEY,
    partition_grain   TEXT NOT NULL DEFAULT 'day',   -- only 'day' in Phase 1
    published_path    TEXT NOT NULL,                 -- relative to DATA_ROOT
    hot_window_days   INT  NOT NULL DEFAULT 2,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A backfill request: one table, one date range, one transform version.
CREATE TABLE IF NOT EXISTS backfills (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name        TEXT NOT NULL REFERENCES tables_registry(name),
    range_start       DATE NOT NULL,
    range_end         DATE NOT NULL,                 -- inclusive
    transform_version TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'planned',
        -- planned -> running -> reconciling -> publishable -> published | failed
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (range_end >= range_start)
);

-- One row per partition inside a backfill.
CREATE TABLE IF NOT EXISTS partition_tasks (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backfill_id       BIGINT NOT NULL REFERENCES backfills(id) ON DELETE CASCADE,
    partition         DATE NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
        -- pending -> running -> staged -> published | failed | skipped
    staging_path      TEXT,
    attempt           INT NOT NULL DEFAULT 0,
    error             TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (backfill_id, partition)
);

-- Per-partition staged-vs-published reconciliation metrics + verdict.
CREATE TABLE IF NOT EXISTS reconciliation (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    backfill_id         BIGINT NOT NULL REFERENCES backfills(id) ON DELETE CASCADE,
    partition           DATE NOT NULL,
    published_row_count BIGINT,
    staged_row_count    BIGINT,
    keyset_added        BIGINT,
    keyset_removed      BIGINT,
    control_total_before NUMERIC,
    control_total_after  NUMERIC,
    verdict             TEXT NOT NULL DEFAULT 'pending',  -- pass | fail | pending
    details             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (backfill_id, partition)
);

-- Idempotent publication guard. A given (table, partition, transform_version)
-- may be published exactly once; retries are no-ops.
CREATE TABLE IF NOT EXISTS publish_ledger (
    table_name        TEXT NOT NULL,
    partition         DATE NOT NULL,
    transform_version TEXT NOT NULL,
    backfill_id       BIGINT REFERENCES backfills(id),
    published_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, partition, transform_version)
);
