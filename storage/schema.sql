CREATE TABLE IF NOT EXISTS raw_jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    company     TEXT NOT NULL,
    raw_title   TEXT NOT NULL,
    location    TEXT,
    jd_text     TEXT,
    url         TEXT,
    posted_at   TEXT,
    fetched_at  TEXT NOT NULL,
    UNIQUE(source, source_id)
);

CREATE TABLE IF NOT EXISTS canonical_jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint      TEXT UNIQUE NOT NULL,
    company          TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    location         TEXT,
    jd_hash          TEXT,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    repost_count     INTEGER DEFAULT 1,
    source_count     INTEGER DEFAULT 1,
    sources          TEXT DEFAULT '[]',
    is_ghost         INTEGER DEFAULT 0,
    ghost_reason     TEXT
);

CREATE TABLE IF NOT EXISTS raw_to_canonical (
    raw_id       INTEGER REFERENCES raw_jobs(id),
    canonical_id INTEGER REFERENCES canonical_jobs(id),
    PRIMARY KEY (raw_id, canonical_id)
);

CREATE TABLE IF NOT EXISTS signal_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_id    INTEGER REFERENCES canonical_jobs(id),
    scored_at       TEXT NOT NULL,
    recency_weight  REAL,
    source_weight   REAL,
    repost_factor   REAL,
    freshness_decay REAL,
    final_score     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS weekly_trends (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    week_start      TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    dimension_value TEXT NOT NULL,
    signal_units    REAL NOT NULL,
    job_count       INTEGER NOT NULL,
    UNIQUE(week_start, dimension, dimension_value)
);

CREATE INDEX IF NOT EXISTS idx_canonical_fingerprint ON canonical_jobs(fingerprint);
CREATE INDEX IF NOT EXISTS idx_signal_canonical ON signal_scores(canonical_id);
CREATE INDEX IF NOT EXISTS idx_trends_week ON weekly_trends(week_start, dimension);
