import time
from datetime import datetime, timezone
from typing import Iterator

import requests

from ingestion.base import BaseIngestor, RawJob, utcnow_iso


class LeverIngestor(BaseIngestor):
    BASE_URL = "https://api.lever.co/v0/postings/{company}?mode=json"

    def __init__(self, companies: list[str]):
        self.companies = companies
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "hiring-signal-extractor/1.0"

    def fetch(self) -> Iterator[RawJob]:
        for company in self.companies:
            yield from self._fetch_company(company)
            time.sleep(1)

    def _fetch_company(self, company: str) -> Iterator[RawJob]:
        url = self.BASE_URL.format(company=company)
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 404:
                print(f"[lever] skipping '{company}' — not found (not on Lever or wrong slug)")
                return
            resp.raise_for_status()
        except requests.Timeout:
            print(f"[lever] skipping '{company}' — request timed out")
            return
        except requests.RequestException as e:
            print(f"[lever] skipping '{company}' — {e}")
            return

        for job in resp.json():
            created_ms = job.get("createdAt", 0)
            posted_at = datetime.utcfromtimestamp(created_ms / 1000).isoformat() if created_ms else None
            categories = job.get("categories", {})

            yield RawJob(
                source="lever",
                source_id=job.get("id", ""),
                company=company,
                raw_title=job.get("text", ""),
                location=categories.get("location") or categories.get("allLocations", [None])[0],
                jd_text=job.get("descriptionPlain", ""),
                url=job.get("hostedUrl"),
                posted_at=posted_at,
                fetched_at=utcnow_iso(),
            )
