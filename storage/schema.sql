-- ─────────────────────────────────────────────
-- RigLogix Router — SQLite Schema
-- ─────────────────────────────────────────────

PRAGMA foreign_keys = ON;

-- Raw news as fetched from Daily Logix
CREATE TABLE IF NOT EXISTS news_items (
    id              TEXT PRIMARY KEY,          -- sha256[:16] of (source_url + title)
    source          TEXT NOT NULL DEFAULT 'daily_logix',
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    source_url      TEXT,
    published_at    TEXT,                      -- ISO-8601 from source, best-effort
    fetched_at      TEXT NOT NULL,             -- ISO-8601 UTC
    run_id          TEXT NOT NULL
);

-- Structured fields extracted by LLM
CREATE TABLE IF NOT EXISTS structured_news (
    news_id         TEXT PRIMARY KEY REFERENCES news_items(id),
    client          TEXT,
    geography       TEXT,
    operation_type  TEXT,                      -- e.g. drilling, completion, intervention
    wells           TEXT,                      -- JSON array of well names / count
    asset           TEXT,
    phase           TEXT,                      -- exploration, appraisal, development
    timing_raw      TEXT,                      -- raw timing text from news
    environment     TEXT,                      -- deepwater, shelf, onshore
    depth_m         REAL,
    contractor      TEXT,
    history_notes   TEXT,
    structured_at   TEXT NOT NULL
);

-- Enriched fields added on top of structured
CREATE TABLE IF NOT EXISTS enriched_news (
    news_id             TEXT PRIMARY KEY REFERENCES news_items(id),
    depth_m             REAL,
    conditions          TEXT,
    wells_json          TEXT,                  -- JSON array
    timeline            TEXT,
    rig                 TEXT,
    contractor          TEXT,
    phase_inferred      TEXT,
    relationships_json  TEXT,                  -- JSON object {operator, contractor, ...}
    uncertainty         TEXT CHECK(uncertainty IN ('low','medium','high')),
    enriched_at         TEXT NOT NULL
);

-- One row per (news_item × product) evaluated
CREATE TABLE IF NOT EXISTS product_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id             TEXT NOT NULL REFERENCES news_items(id),
    run_id              TEXT NOT NULL,
    product             TEXT NOT NULL,
    technical_fit       REAL NOT NULL CHECK(technical_fit BETWEEN 0 AND 1),
    timing_fit          REAL NOT NULL CHECK(timing_fit BETWEEN 0 AND 1),
    commercial_priority REAL NOT NULL CHECK(commercial_priority BETWEEN 0 AND 1),
    composite_score     REAL NOT NULL CHECK(composite_score BETWEEN 0 AND 1),
    rationale           TEXT NOT NULL,         -- JSON array of strings
    uncertainty         TEXT NOT NULL CHECK(uncertainty IN ('low','medium','high')),
    evaluated_at        TEXT NOT NULL
);

-- Each daily pipeline run
CREATE TABLE IF NOT EXISTS opportunity_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    news_fetched    INTEGER DEFAULT 0,
    news_processed  INTEGER DEFAULT 0,
    signals_created INTEGER DEFAULT 0,
    digest_sent     INTEGER DEFAULT 0,         -- 0 or 1
    status          TEXT DEFAULT 'running' CHECK(status IN ('running','success','error')),
    error_message   TEXT
);

-- Sent email digests
CREATE TABLE IF NOT EXISTS digests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL REFERENCES opportunity_runs(run_id),
    sent_at         TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    subject         TEXT NOT NULL,
    body_html       TEXT NOT NULL,
    signal_ids      TEXT NOT NULL              -- JSON array of product_signals.id
);

-- Seller feedback per signal
CREATE TABLE IF NOT EXISTS feedback (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id       INTEGER NOT NULL REFERENCES product_signals(id),
    digest_id       INTEGER REFERENCES digests(id),
    rating          INTEGER CHECK(rating BETWEEN 1 AND 5),
    comment         TEXT,
    received_at     TEXT NOT NULL
);
