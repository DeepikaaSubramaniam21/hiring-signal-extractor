import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests

from ingestion.base import BaseIngestor, RawJob, utcnow_iso

BUDGET_FILE = Path(__file__).parent.parent / "data" / "adzuna_budget.json"
BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search/{page}"


def _load_budget(daily_limit: int) -> dict:
    today = datetime.now(timezone.utc).date().isoformat()
    if BUDGET_FILE.exists():
        data = json.loads(BUDGET_FILE.read_text())
        if data.get("date") == today:
            return data
    return {"date": today, "used": 0, "limit": daily_limit}


def _save_budget(budget: dict) -> None:
    BUDGET_FILE.parent.mkdir(exist_ok=True)
    BUDGET_FILE.write_text(json.dumps(budget))


class AdzunaIngestor(BaseIngestor):
    def __init__(self, settings: dict, target_roles: list[str] | None = None):
        self.api_id = settings["api_id"]
        self.api_key = settings["api_key"]
        # target_roles from top-level settings takes precedence over legacy adzuna.roles
        self.roles = target_roles or settings.get("roles", [])
        self.daily_limit = settings.get("daily_request_budget", 250)
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "hiring-signal-extractor/1.0"

    def fetch(self) -> Iterator[RawJob]:
        if not self.api_id or not self.api_key:
            print("[adzuna] Skipping - api_id and api_key are not set. Add them to config/settings.yaml to enable Adzuna ingestion.")
            return

        budget = _load_budget(self.daily_limit)

        for role in self.roles:
            if budget["used"] >= budget["limit"]:
                print(f"[adzuna] Daily request budget exhausted ({budget['limit']} reqs)")
                break
            yield from self._fetch_role(role, budget)

        _save_budget(budget)

    def _fetch_role(self, role: str, budget: dict) -> Iterator[RawJob]:
        fetched = utcnow_iso()

        for page in range(1, 6):
            if budget["used"] >= budget["limit"]:
                break

            try:
                resp = self.session.get(
                    BASE_URL.format(page=page),
                    params={
                        "app_id": self.api_id,
                        "app_key": self.api_key,
                        "results_per_page": 50,
                        "what": role,
                        "content-type": "application/json",
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                budget["used"] += 1
                time.sleep(0.5)
            except requests.RequestException as e:
                print(f"[adzuna] {role} page {page}: {e}")
                break

            results = resp.json().get("results", [])
            if not results:
                break

            for job in results:
                created = job.get("created")
                posted_at = created[:19] if created else None

                yield RawJob(
                    source="adzuna",
                    source_id=str(job.get("id", "")),
                    company=job.get("company", {}).get("display_name", "Unknown"),
                    raw_title=job.get("title", ""),
                    location=job.get("location", {}).get("display_name"),
                    jd_text=job.get("description", ""),
                    url=job.get("redirect_url"),
                    posted_at=posted_at,
                    fetched_at=fetched,
                )
