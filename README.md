# 📡 Hiring Signal Extractor

A noise-free job trend analyzer that ingests postings from multiple sources, deduplicates them, scores them by hiring signal strength, and surfaces trends through an interactive Streamlit dashboard.

Built for tracking specific roles (e.g. Engineering Manager, Director of Engineering) across target companies — without the spam, ghost jobs, or duplicate noise that makes raw job boards unusable.

---

## How It Works

```
Greenhouse ──┐
Lever       ──┤  ingest  ──>  deduplicate  ──>  score  ──>  trends  ──>  dashboard
WWR         ──┤
Adzuna      ──┘
```

1. **Ingest** — pulls live job postings from ATS APIs (Greenhouse, Lever), RSS (We Work Remotely), and the Adzuna aggregator API
2. **Deduplicate** — two-layer dedup: exact SHA-256 fingerprint + MinHash LSH (Jaccard ≥ 0.85) to catch near-duplicates with reworded JDs
3. **Score** — each canonical job gets a signal score based on recency, source quality, cross-platform demand, and freshness decay
4. **Trend** — aggregates scores into weekly role / skill / location dimensions
5. **Dashboard** — Streamlit UI with filters for company, score threshold, and title keyword

---

## Scoring Formula

```
final_score = recency_weight × source_weight × cross_source_bonus × freshness_decay
```

| Factor | Description |
|---|---|
| `recency_weight` | 1.0 (≤7 days) → 0.7 (≤14 days) → 0.3 (older) |
| `source_weight` | Greenhouse/Lever = 1.0, WWR = 0.9, Adzuna = 0.6 |
| `cross_source_bonus` | +20% per additional platform (capped at 1.4× for 3+ sources) |
| `freshness_decay` | Exponential decay with 14-day half-life |

> Scores above 1.0 mean the same role was found on multiple platforms — a strong hiring signal.

---

## Ghost Job Detection

A job is marked as a ghost only if it is **stale** (`first_seen_at` > 60 days ago). Jobs ingested today are never ghosted — repost-based ghosting is only meaningful across historical pipeline runs.

Ghost jobs are excluded from the signal feed and trends but visible in the dashboard's Ghost Monitor tab.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure target companies

Edit `config/companies.yaml` to add Greenhouse and Lever slugs:

```yaml
greenhouse:
  - stripe
  - datadog
  - mongodb
  - airbnb
  - databricks

lever:
  - redoxengine
```

> **Finding slugs:** For Greenhouse, check `https://boards-api.greenhouse.io/v1/boards/{slug}/jobs`. For Lever, check `https://api.lever.co/v0/postings/{slug}`.

### 3. Configure target roles and settings

Edit `config/settings.yaml`:

```yaml
target_roles:
  - engineering manager
  - senior engineering manager
  - director of engineering

ghost_thresholds:
  max_age_days: 60

scoring:
  freshness_half_life_days: 14

adzuna:
  api_id: ""       # optional — get a free key at developer.adzuna.com
  api_key: ""
  daily_request_budget: 250
```

> `target_roles` is a substring filter applied at query time — change it without re-ingesting.

### 4. (Optional) Adzuna API

Sign up for a free key at [developer.adzuna.com](https://developer.adzuna.com). Add `api_id` and `api_key` to `settings.yaml`. Without it, only Greenhouse, Lever, and WWR are used.

---

## Running the Pipeline

```bash
python pipeline.py
```

This will:
- Drop and recreate the database (fresh slate on every run)
- Ingest from all configured sources
- Deduplicate, score, and compute weekly trends
- Print a summary of raw → canonical jobs and any skipped companies

Example output:
```
[pipeline] Dropped existing database and dedup index
[pipeline] Fresh database created
[pipeline] Ingesting from GreenhouseIngestor...
[pipeline] Ingesting from LeverIngestor...
[pipeline] Ingesting from WWRIngestor...
[pipeline] Ingesting from AdzunaIngestor...
[pipeline] Ingested 3385 raw jobs -> 2406 new canonical jobs
[pipeline] Running ghost filter...
[pipeline] Flagged 0 ghost jobs
[pipeline] Scoring...
[pipeline] Scored 2406 canonical jobs
[pipeline] Computing weekly trends...
[pipeline] Computed 61 trend data points
[pipeline] Done.
```

---

## Running the Dashboard

```bash
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.

### Sidebar controls

**Trends Chart**
- **Week** — select the week to visualize (useful when running pipeline daily)
- **Dimension** — role, skill, or location
- **Min signal** — filter out low-signal entries
- **Top N** — number of bars to show

**Signal Feed**
- **Companies** — multiselect to focus on specific companies (default = all)
- **Min score** — hide low-confidence listings; scores >1.0 = cross-platform postings
- **Title keyword** — free-text search on normalized title (e.g. `staff`, `ml`, `platform`)
- **Max rows** — cap the feed size

---

## Project Structure

```
hiring-signal-extractor/
├── pipeline.py                 # Main entry point
├── config/
│   ├── settings.yaml           # Roles, scoring params, API keys
│   └── companies.yaml          # Greenhouse + Lever company slugs
├── ingestion/
│   ├── base.py                 # RawJob dataclass + shared utilities
│   ├── greenhouse.py           # Greenhouse ATS API ingestor
│   ├── lever.py                # Lever ATS API ingestor
│   ├── weworkremotely.py       # WWR RSS feed ingestor
│   └── adzuna.py               # Adzuna aggregator API ingestor
├── processing/
│   ├── deduplicator.py         # SHA-256 fingerprint + MinHash LSH
│   ├── normalizer.py           # Title and location normalization
│   ├── scorer.py               # Signal scoring formula
│   └── ghost_filter.py         # Stale job detection
├── storage/
│   ├── schema.sql              # SQLite schema
│   └── db.py                   # DB connection + upsert helpers
├── trends/
│   └── engine.py               # Weekly trend aggregation
└── dashboard/
    └── app.py                  # Streamlit dashboard
```

---

## Data Sources

| Source | Type | Coverage | Notes |
|---|---|---|---|
| Greenhouse | ATS API | Direct company boards | Best signal quality; free, no auth |
| Lever | ATS API | Direct company boards | Free, no auth |
| We Work Remotely | RSS | Remote-first jobs | Good for remote signal |
| Adzuna | Aggregator API | Broad market coverage | Free tier: 250 req/day |

---

## Adding More Companies

Run a quick triage to find a company's ATS:

```bash
# Test Greenhouse
curl -s "https://boards-api.greenhouse.io/v1/boards/SLUG/jobs" | python -m json.tool | head -5

# Test Lever
curl -s "https://api.lever.co/v0/postings/SLUG" | python -m json.tool | head -5
```

If both return 404, the company is likely on Workday, iCIMS, or a custom ATS and not currently supported.
