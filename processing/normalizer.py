import re

_TITLE_ABBREVS = {
    r"\bsr\.?\b": "senior",
    r"\bjr\.?\b": "junior",
    r"\beng\.?\b": "engineer",
    r"\bmgr\.?\b": "manager",
    r"\bdir\.?\b": "director",
    r"\bvp\b": "vice president",
    r"\bswe\b": "software engineer",
    r"\bsde\b": "software engineer",
    r"\bml\b": "machine learning",
    r"\bai\b": "artificial intelligence",
}

_LOCATION_REMOTE = re.compile(
    r"\b(remote|work from home|wfh|distributed|anywhere|worldwide)\b", re.IGNORECASE
)

_LOCATION_NOISE = re.compile(
    r"\b(hybrid|onsite|on-site|in-office|flexible)\b", re.IGNORECASE
)

_LOCATION_CANONICAL = {
    "nyc": "New York",
    "new york city": "New York",
    "new york, ny": "New York",
    "san francisco, ca": "San Francisco",
    "sf": "San Francisco",
    "la": "Los Angeles",
    "los angeles, ca": "Los Angeles",
    "london, uk": "London",
    "london, england": "London",
}


def normalize_title(raw: str) -> str:
    title = raw.lower().strip()
    # Remove location fragments that leak into titles
    title = re.sub(r"\s*[-–|]\s*(remote|us|uk|eu|emea|apac)[,\s]*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\([^)]*\)\s*$", "", title)  # trailing parentheticals
    # Expand abbreviations
    for pattern, replacement in _TITLE_ABBREVS.items():
        title = re.sub(pattern, replacement, title)
    # Collapse whitespace and punctuation
    title = re.sub(r"[^\w\s]", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def normalize_location(raw: str | None) -> str:
    if not raw:
        return "Unknown"
    cleaned = raw.strip()
    if _LOCATION_REMOTE.search(cleaned):
        return "Remote"
    lower = cleaned.lower()
    for key, canonical in _LOCATION_CANONICAL.items():
        if key in lower:
            return canonical
    # Strip noise words, return title-cased remainder
    cleaned = _LOCATION_NOISE.sub("", cleaned).strip().strip(",").strip()
    return cleaned.title() if cleaned else "Unknown"
