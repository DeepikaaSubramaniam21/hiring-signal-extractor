from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RawJob:
    source: str
    source_id: str
    company: str
    raw_title: str
    location: str | None
    jd_text: str | None
    url: str | None
    posted_at: str | None
    fetched_at: str = field(default_factory=utcnow_iso)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "source_id": self.source_id,
            "company": self.company,
            "raw_title": self.raw_title,
            "location": self.location,
            "jd_text": self.jd_text,
            "url": self.url,
            "posted_at": self.posted_at,
            "fetched_at": self.fetched_at,
        }


class BaseIngestor(ABC):
    @abstractmethod
    def fetch(self) -> Iterator[RawJob]: ...
