import re
from datetime import datetime
from html.parser import HTMLParser
from typing import Iterator

import feedparser

from ingestion.base import BaseIngestor, RawJob, utcnow_iso

FEED_URL = "https://weworkremotely.com/remote-jobs.rss"


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []

    def handle_data(self, data):
        self._parts.append(data)

    def get_text(self):
        return re.sub(r"\s+", " ", " ".join(self._parts)).strip()


def strip_html(html: str) -> str:
    s = _HTMLStripper()
    s.feed(html)
    return s.get_text()


class WWRIngestor(BaseIngestor):
    def fetch(self) -> Iterator[RawJob]:
        feed = feedparser.parse(FEED_URL)
        fetched = utcnow_iso()

        for entry in feed.entries:
            title = entry.get("title", "")
            # WWR titles follow: "Company: Role Title"
            if ": " in title:
                company, raw_title = title.split(": ", 1)
            else:
                company, raw_title = "Unknown", title

            published_parsed = entry.get("published_parsed")
            posted_at = datetime(*published_parsed[:6]).isoformat() if published_parsed else None

            yield RawJob(
                source="wwr",
                source_id=entry.get("link", title),
                company=company.strip(),
                raw_title=raw_title.strip(),
                location="Remote",
                jd_text=strip_html(entry.get("summary", "")),
                url=entry.get("link"),
                posted_at=posted_at,
                fetched_at=fetched,
            )
