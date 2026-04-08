from datetime import datetime, timezone


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def evaluate_ghost(job: dict, settings: dict) -> tuple[bool, str | None]:
    thresholds = settings["ghost_thresholds"]
    first_seen = job["first_seen_at"]
    if not first_seen:
        return False, None

    now = _utcnow()
    first_seen_dt = _parse_iso(first_seen)
    age_days = (now - first_seen_dt).days

    # Skip ghost detection entirely for jobs seen today — they're fresh ingestions.
    # Repost-based ghosting only makes sense across multiple pipeline runs over time.
    if age_days < 1:
        return False, None

    if age_days > thresholds["max_age_days"]:
        return True, "stale"

    return False, None


def run_ghost_filter(conn, settings: dict) -> int:
    """Flag ghost jobs in canonical_jobs. Returns count of jobs flagged."""
    jobs = conn.execute("SELECT * FROM canonical_jobs WHERE is_ghost = 0").fetchall()
    flagged = 0

    for job in jobs:
        is_ghost, reason = evaluate_ghost(dict(job), settings)
        if is_ghost:
            conn.execute(
                "UPDATE canonical_jobs SET is_ghost = 1, ghost_reason = ? WHERE id = ?",
                (reason, job["id"]),
            )
            flagged += 1

    conn.commit()
    return flagged
