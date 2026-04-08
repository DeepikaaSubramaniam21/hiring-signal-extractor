import hashlib
import json
import pickle
import re
from pathlib import Path

from datasketch import MinHash, MinHashLSH

LSH_INDEX_PATH = Path(__file__).parent.parent / "data" / "minhash_index.pkl"
JACCARD_THRESHOLD = 0.85
NUM_PERM = 128


def _shingles(text: str, k: int = 3) -> set[str]:
    words = re.sub(r"[^\w\s]", "", text.lower()).split()
    return {" ".join(words[i : i + k]) for i in range(len(words) - k + 1)}


def _minhash(text: str) -> MinHash:
    m = MinHash(num_perm=NUM_PERM)
    for shingle in _shingles(text):
        m.update(shingle.encode("utf-8"))
    return m


def make_fingerprint(company: str, normalized_title: str, location: str, jd_text: str | None) -> str:
    company_slug = re.sub(r"\s+", "-", company.lower().strip())
    jd_hash = hashlib.sha256((jd_text or "")[:2000].encode()).hexdigest()[:16]
    raw = f"{company_slug}|{normalized_title}|{location}|{jd_hash}"
    return hashlib.sha256(raw.encode()).hexdigest()


def make_jd_hash(jd_text: str | None) -> str:
    return hashlib.sha256((jd_text or "")[:2000].encode()).hexdigest()[:32]


class Deduplicator:
    def __init__(self, index_path: Path = LSH_INDEX_PATH):
        self.index_path = index_path
        self._lsh, self._minhashes = self._load_index()

    def _load_index(self) -> tuple[MinHashLSH, dict]:
        if self.index_path.exists():
            with open(self.index_path, "rb") as f:
                return pickle.load(f)
        return MinHashLSH(threshold=JACCARD_THRESHOLD, num_perm=NUM_PERM), {}

    def persist(self) -> None:
        self.index_path.parent.mkdir(exist_ok=True)
        with open(self.index_path, "wb") as f:
            pickle.dump((self._lsh, self._minhashes), f)

    def find_near_duplicate(self, fingerprint: str, jd_text: str | None) -> str | None:
        """Return fingerprint of a near-duplicate if found, else None."""
        if not jd_text or len(jd_text.split()) < 20:
            return None
        m = _minhash(jd_text)
        candidates = self._lsh.query(m)
        return candidates[0] if candidates else None

    def register(self, fingerprint: str, jd_text: str | None) -> None:
        """Add a new canonical job to the LSH index."""
        if not jd_text or len(jd_text.split()) < 20:
            return
        if fingerprint in self._minhashes:
            return
        m = _minhash(jd_text)
        self._lsh.insert(fingerprint, m)
        self._minhashes[fingerprint] = m
