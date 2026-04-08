from datetime import date, timedelta

from storage.db import upsert_weekly_trend


def _current_week_start() -> str:
    today = date.today()
    return (today - timedelta(days=today.weekday())).isoformat()


def _extract_skills(jd_text: str | None, skill_list: list[str]) -> list[str]:
    if not jd_text:
        return []
    lower = jd_text.lower()
    return [skill for skill in skill_list if skill in lower]


def _matches_target_roles(normalized_title: str, target_roles: list[str]) -> bool:
    """Return True if the title contains any target role keyword."""
    if not target_roles:
        return True  # no filter configured — include everything
    title_lower = normalized_title.lower()
    return any(role.lower() in title_lower for role in target_roles)


def compute_weekly_trends(conn, settings: dict, week_start: str | None = None) -> int:
    if not week_start:
        week_start = _current_week_start()

    skill_list = settings.get("skills", [])
    target_roles = settings.get("target_roles", [])

    rows = conn.execute(
        """
        SELECT c.id, c.normalized_title, c.location, c.is_ghost,
               s.final_score, r.jd_text
        FROM canonical_jobs c
        JOIN signal_scores s ON s.canonical_id = c.id
        LEFT JOIN raw_to_canonical rc ON rc.canonical_id = c.id
        LEFT JOIN raw_jobs r ON r.id = rc.raw_id
        WHERE c.is_ghost = 0
        ORDER BY c.id, s.scored_at DESC
        """
    ).fetchall()

    # Keep only the latest score per canonical job
    seen = set()
    unique_rows = []
    for row in rows:
        if row["id"] not in seen:
            seen.add(row["id"])
            unique_rows.append(row)

    # Filter to target roles
    unique_rows = [r for r in unique_rows if _matches_target_roles(r["normalized_title"], target_roles)]

    count = 0
    for row in unique_rows:
        score = row["final_score"]

        # Role dimension
        upsert_weekly_trend(conn, {
            "week_start": week_start,
            "dimension": "role",
            "dimension_value": row["normalized_title"],
            "signal_units": score,
            "job_count": 1,
        })
        count += 1

        # Location dimension
        if row["location"]:
            upsert_weekly_trend(conn, {
                "week_start": week_start,
                "dimension": "location",
                "dimension_value": row["location"],
                "signal_units": score,
                "job_count": 1,
            })

        # Skill dimensions
        for skill in _extract_skills(row["jd_text"], skill_list):
            upsert_weekly_trend(conn, {
                "week_start": week_start,
                "dimension": "skill",
                "dimension_value": skill,
                "signal_units": score,
                "job_count": 1,
            })

    return count
