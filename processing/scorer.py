import json
import math
from datetime import datetime, timezone


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_score(job: dict, settings: dict) -> dict:
    source_weights = settings["source_weights"]
    half_life = settings["scoring"]["freshness_half_life_days"]

    sources = json.loads(job["sources"]) if isinstance(job["sources"], str) else job["sources"]
    source_weight = max((source_weights.get(s, 0.5) for s in sources), default=0.5)

    last_seen = job["last_seen_at"]
    age_days = (_parse_iso(_utcnow_iso()) - _parse_iso(last_seen)).days

    recency_weight = 1.0 if age_days <= 7 else (0.7 if age_days <= 14 else 0.3)
    freshness_decay = math.exp(-0.693 * age_days / half_life)

    # Cross-source bonus: more platforms listing the same role = stronger hiring signal.
    # Each additional source adds +20% (capped at 3 sources = 1.4x).
    source_count = job.get("source_count", 1) or 1
    cross_source_bonus = 1.0 + (min(source_count, 3) - 1) * 0.2

    ghost_penalty = 0.05 if job["is_ghost"] else 1.0

    final_score = recency_weight * source_weight * cross_source_bonus * freshness_decay * ghost_penalty

    return {
        "canonical_id": job["id"],
        "scored_at": _utcnow_iso(),
        "recency_weight": recency_weight,
        "source_weight": source_weight,
        "repost_factor": cross_source_bonus,   # column reused; now stores cross-source bonus
        "freshness_decay": freshness_decay,
        "final_score": round(final_score, 6),
    }
