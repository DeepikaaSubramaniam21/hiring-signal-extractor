import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Iterator

import requests

from ingestion.base import BaseIngestor, RawJob, utcnow_iso


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []

    def handle_data(self, data):
        self._parts.append(data)

    def get_text(self):
        return " ".join(self._parts)


def strip_html(html: str) -> str:
    s = _HTMLStripper()
    s.feed(html)
    return re.sub(r"\s+", " ", s.get_text()).strip()


class GreenhouseIngestor(BaseIngestor):
    BASE_URL = "https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true"

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
                print(f"[greenhouse] skipping '{company}' — not found (not on Greenhouse or wrong slug)")
                return
            resp.raise_for_status()
        except requests.Timeout:
            print(f"[greenhouse] skipping '{company}' — request timed out")
            return
        except requests.RequestException as e:
            print(f"[greenhouse] skipping '{company}' — {e}")
            return

        for job in resp.json().get("jobs", []):
            yield RawJob(
                source="greenhouse",
                source_id=str(job["id"]),
                company=company,
                raw_title=job.get("title", ""),
                location=job.get("location", {}).get("name"),
                jd_text=strip_html(job.get("content", "")),
                url=job.get("absolute_url"),
                posted_at=job.get("updated_at"),
                fetched_at=utcnow_iso(),
            )
