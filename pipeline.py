import os
from pathlib import Path

import yaml

# Force UTF-8 output on Windows to avoid charmap encoding errors
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from ingestion.greenhouse import GreenhouseIngestor
from ingestion.lever import LeverIngestor
from ingestion.weworkremotely import WWRIngestor
from ingestion.adzuna import AdzunaIngestor
from processing.deduplicator import Deduplicator, make_fingerprint, make_jd_hash
from processing.ghost_filter import run_ghost_filter
from processing.normalizer import normalize_title, normalize_location
from processing.scorer import compute_score
from storage.db import (
    execute_schema,
    get_connection,
    get_all_canonicals,
    insert_signal_score,
    link_raw_to_canonical,
    upsert_canonical_job,
    upsert_raw_job,
)
from trends.engine import compute_weekly_trends

CONFIG_DIR = Path(__file__).parent / "config"


def load_config() -> tuple[dict, dict]:
    settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text())
    companies = yaml.safe_load((CONFIG_DIR / "companies.yaml").read_text())
    return settings, companies


def run_pipeline() -> None:
    settings, companies = load_config()

    # Drop and recreate DB + MinHash index on every run for a clean slate
    from storage.db import DB_PATH
    from processing.deduplicator import LSH_INDEX_PATH
    for path in [DB_PATH, LSH_INDEX_PATH]:
        if path.exists():
            path.unlink()
    print("[pipeline] Dropped existing database and dedup index")

    conn = get_connection()
    execute_schema(conn)
    print(f"[pipeline] Fresh database created")

    ingestors = [
        GreenhouseIngestor(companies=companies.get("greenhouse", [])),
        LeverIngestor(companies=companies.get("lever", [])),
        WWRIngestor(),
        AdzunaIngestor(settings=settings["adzuna"], target_roles=settings.get("target_roles", [])),
    ]

    dedup = Deduplicator()

    total_raw = 0
    total_new_canonical = 0

    for ingestor in ingestors:
        source_name = type(ingestor).__name__
        print(f"[pipeline] Ingesting from {source_name}...")
        for raw_job in ingestor.fetch():
            raw_dict = raw_job.to_dict()
            raw_id = upsert_raw_job(conn, raw_dict)
            total_raw += 1

            norm_title = normalize_title(raw_job.raw_title)
            norm_location = normalize_location(raw_job.location)
            jd_hash = make_jd_hash(raw_job.jd_text)

            # Layer 1: exact fingerprint
            fingerprint = make_fingerprint(
                raw_job.company, norm_title, norm_location, raw_job.jd_text
            )

            # Layer 2: fuzzy near-duplicate check
            near_dup = dedup.find_near_duplicate(fingerprint, raw_job.jd_text)
            if near_dup:
                fingerprint = near_dup  # redirect to existing canonical

            from ingestion.base import utcnow_iso
            canonical_dict = {
                "fingerprint": fingerprint,
                "company": raw_job.company,
                "normalized_title": norm_title,
                "location": norm_location,
                "jd_hash": jd_hash,
                "first_seen_at": utcnow_iso(),
                "last_seen_at": utcnow_iso(),
                "source": raw_job.source,
            }

            canonical_id, is_new = upsert_canonical_job(conn, canonical_dict)
            link_raw_to_canonical(conn, raw_id, canonical_id)

            if is_new:
                dedup.register(fingerprint, raw_job.jd_text)
                total_new_canonical += 1

    print(f"[pipeline] Ingested {total_raw} raw jobs -> {total_new_canonical} new canonical jobs")

    print("[pipeline] Running ghost filter...")
    flagged = run_ghost_filter(conn, settings)
    print(f"[pipeline] Flagged {flagged} ghost jobs")

    print("[pipeline] Scoring...")
    canonicals = get_all_canonicals(conn)
    for job in canonicals:
        score = compute_score(dict(job), settings)
        insert_signal_score(conn, score)
    print(f"[pipeline] Scored {len(canonicals)} canonical jobs")

    print("[pipeline] Computing weekly trends...")
    trend_count = compute_weekly_trends(conn, settings)
    print(f"[pipeline] Computed {trend_count} trend data points")

    dedup.persist()
    print("[pipeline] Done.")


if __name__ == "__main__":
    run_pipeline()
