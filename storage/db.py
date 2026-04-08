import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "jobs.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def execute_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text())
    conn.commit()


def upsert_raw_job(conn: sqlite3.Connection, job: dict) -> int:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO raw_jobs
            (source, source_id, company, raw_title, location, jd_text, url, posted_at, fetched_at)
        VALUES
            (:source, :source_id, :company, :raw_title, :location, :jd_text, :url, :posted_at, :fetched_at)
        """,
        job,
    )
    conn.commit()
    if cur.lastrowid:
        return cur.lastrowid
    row = conn.execute(
        "SELECT id FROM raw_jobs WHERE source = ? AND source_id = ?",
        (job["source"], job["source_id"]),
    ).fetchone()
    return row["id"]


def upsert_canonical_job(conn: sqlite3.Connection, job: dict) -> tuple[int, bool]:
    """Returns (canonical_id, is_new)."""
    existing = conn.execute(
        "SELECT id, repost_count, source_count, sources FROM canonical_jobs WHERE fingerprint = ?",
        (job["fingerprint"],),
    ).fetchone()

    if existing:
        sources = set(json.loads(existing["sources"]))
        sources.add(job["source"])
        conn.execute(
            """
            UPDATE canonical_jobs
            SET last_seen_at = ?, repost_count = repost_count + 1,
                source_count = ?, sources = ?
            WHERE id = ?
            """,
            (job["last_seen_at"], len(sources), json.dumps(list(sources)), existing["id"]),
        )
        conn.commit()
        return existing["id"], False

    conn.execute(
        """
        INSERT INTO canonical_jobs
            (fingerprint, company, normalized_title, location, jd_hash,
             first_seen_at, last_seen_at, repost_count, source_count, sources)
        VALUES
            (:fingerprint, :company, :normalized_title, :location, :jd_hash,
             :first_seen_at, :last_seen_at, 1, 1, :sources)
        """,
        {**job, "sources": json.dumps([job["source"]])},
    )
    conn.commit()
    return conn.execute(
        "SELECT id FROM canonical_jobs WHERE fingerprint = ?", (job["fingerprint"],)
    ).fetchone()["id"], True


def link_raw_to_canonical(conn: sqlite3.Connection, raw_id: int, canonical_id: int) -> None:
    # Verify both rows exist before linking to satisfy FK constraint
    raw_exists = conn.execute("SELECT 1 FROM raw_jobs WHERE id = ?", (raw_id,)).fetchone()
    canonical_exists = conn.execute("SELECT 1 FROM canonical_jobs WHERE id = ?", (canonical_id,)).fetchone()
    if not raw_exists or not canonical_exists:
        return
    conn.execute(
        "INSERT OR IGNORE INTO raw_to_canonical (raw_id, canonical_id) VALUES (?, ?)",
        (raw_id, canonical_id),
    )
    conn.commit()


def get_all_canonicals(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM canonical_jobs").fetchall()


def insert_signal_score(conn: sqlite3.Connection, score: dict) -> None:
    conn.execute(
        """
        INSERT INTO signal_scores
            (canonical_id, scored_at, recency_weight, source_weight,
             repost_factor, freshness_decay, final_score)
        VALUES
            (:canonical_id, :scored_at, :recency_weight, :source_weight,
             :repost_factor, :freshness_decay, :final_score)
        """,
        score,
    )
    conn.commit()


def upsert_weekly_trend(conn: sqlite3.Connection, trend: dict) -> None:
    conn.execute(
        """
        INSERT INTO weekly_trends (week_start, dimension, dimension_value, signal_units, job_count)
        VALUES (:week_start, :dimension, :dimension_value, :signal_units, :job_count)
        ON CONFLICT(week_start, dimension, dimension_value)
        DO UPDATE SET
            signal_units = signal_units + excluded.signal_units,
            job_count    = job_count + excluded.job_count
        """,
        trend,
    )
    conn.commit()
