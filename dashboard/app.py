import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.db import get_connection

CONFIG_DIR = Path(__file__).parent.parent / "config"


@st.cache_data(ttl=60)
def load_settings() -> dict:
    return yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text())


@st.cache_data(ttl=300)
def load_weeks() -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT week_start FROM weekly_trends ORDER BY week_start DESC"
    ).fetchall()
    return [r["week_start"] for r in rows]


@st.cache_data(ttl=300)
def load_companies() -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT company FROM canonical_jobs ORDER BY company"
    ).fetchall()
    return [r["company"] for r in rows]


@st.cache_data(ttl=300)
def load_trends(week_start: str, dimension: str, min_signal: float) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(
        """
        SELECT dimension_value, signal_units, job_count
        FROM weekly_trends
        WHERE week_start = ? AND dimension = ? AND signal_units >= ?
        ORDER BY signal_units DESC
        """,
        conn,
        params=[week_start, dimension, min_signal],
    )


@st.cache_data(ttl=300)
def load_trend_history(dimension: str, top_values: list[str]) -> pd.DataFrame:
    """Load week-over-week signal history for a set of dimension values."""
    if not top_values:
        return pd.DataFrame()
    conn = get_connection()
    placeholders = ",".join("?" * len(top_values))
    return pd.read_sql_query(
        f"""
        SELECT week_start, dimension_value, signal_units, job_count
        FROM weekly_trends
        WHERE dimension = ? AND dimension_value IN ({placeholders})
        ORDER BY week_start ASC
        """,
        conn,
        params=[dimension, *top_values],
    )


@st.cache_data(ttl=300)
def load_wow_delta(dimension: str, week_start: str) -> pd.DataFrame:
    """Week-over-week change for the current week vs previous week."""
    conn = get_connection()
    weeks = conn.execute(
        "SELECT DISTINCT week_start FROM weekly_trends ORDER BY week_start DESC LIMIT 2"
    ).fetchall()
    if len(weeks) < 2:
        return pd.DataFrame()
    current_week, prev_week = weeks[0]["week_start"], weeks[1]["week_start"]
    return pd.read_sql_query(
        """
        SELECT c.dimension_value,
               c.signal_units AS current_signal,
               COALESCE(p.signal_units, 0) AS prev_signal,
               ROUND((c.signal_units - COALESCE(p.signal_units, 0))
                     / MAX(COALESCE(p.signal_units, 0.001), 0.001) * 100, 1) AS pct_change,
               c.job_count AS current_jobs
        FROM weekly_trends c
        LEFT JOIN weekly_trends p
          ON p.dimension_value = c.dimension_value AND p.dimension = c.dimension
          AND p.week_start = ?
        WHERE c.dimension = ? AND c.week_start = ?
        ORDER BY pct_change DESC
        """,
        conn,
        params=[prev_week, dimension, current_week],
    )


@st.cache_data(ttl=300)
def load_ghost_jobs() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(
        """
        SELECT company, normalized_title, location,
               first_seen_at, last_seen_at, repost_count, ghost_reason
        FROM canonical_jobs
        WHERE is_ghost = 1
        ORDER BY first_seen_at ASC
        """,
        conn,
    )


@st.cache_data(ttl=300)
def load_signal_feed(
    target_roles: tuple[str, ...],
    companies: tuple[str, ...],
    min_score: float,
    keyword: str,
    limit: int = 100,
) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT c.company, c.normalized_title, c.location,
               c.repost_count, c.source_count, c.sources,
               c.first_seen_at, c.is_ghost,
               s.final_score, s.recency_weight, s.freshness_decay
        FROM canonical_jobs c
        JOIN signal_scores s ON s.canonical_id = c.id
        WHERE s.id = (
            SELECT id FROM signal_scores
            WHERE canonical_id = c.id
            ORDER BY scored_at DESC
            LIMIT 1
        )
        AND c.is_ghost = 0
        ORDER BY s.final_score DESC
        """,
        conn,
    )

    # Role filter (from settings.yaml)
    if target_roles:
        mask = df["normalized_title"].str.lower().apply(
            lambda t: any(r.lower() in t for r in target_roles)
        )
        df = df[mask]

    # Company filter
    if companies:
        df = df[df["company"].isin(companies)]

    # Min score filter
    df = df[df["final_score"] >= min_score]

    # Keyword search on title
    if keyword.strip():
        df = df[df["normalized_title"].str.contains(keyword.strip().lower(), case=False, na=False)]

    return df.head(limit)


# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Hiring Signal", layout="wide")
st.title("📡 Hiring Signal Dashboard")

settings = load_settings()
target_roles = tuple(settings.get("target_roles", []))

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    weeks = load_weeks()
    if not weeks:
        st.warning("No data yet — run `python pipeline.py` first.")
        st.stop()

    # Trends filters
    st.subheader("Trends Chart")
    week_start = st.selectbox("Week", weeks)
    dimension = st.selectbox("Dimension", ["role", "skill", "location"])
    min_signal = st.slider("Min signal", 0.0, 3.0, 0.05, 0.05)
    top_n = st.slider("Top N", 5, 50, 20)
    show_ghosts = st.checkbox("Show ghost job monitor", value=True)

    st.divider()

    # Signal feed filters
    st.subheader("Signal Feed")
    all_companies = load_companies()
    selected_companies = tuple(
        st.multiselect("Companies", all_companies, default=[], placeholder="All companies")
    )
    feed_min_score = st.slider("Min score", 0.0, 2.0, 0.0, 0.05,
                               help="Scores above 1.0 = listed on multiple platforms")
    role_keyword = st.text_input("Title keyword", placeholder="e.g. staff, platform, ml")
    feed_limit = st.slider("Max rows", 10, 200, 100, 10)

# ── Trends chart ──────────────────────────────────────────────────────────────
st.subheader(f"🔥 Top {top_n} by {dimension.title()} — week of {week_start}")
trends = load_trends(week_start, dimension, min_signal).head(top_n)

if trends.empty:
    st.info("No data for this filter combination.")
else:
    fig = px.bar(
        trends,
        x="signal_units",
        y="dimension_value",
        orientation="h",
        color="job_count",
        color_continuous_scale="Blues",
        labels={
            "dimension_value": dimension.title(),
            "signal_units": "Signal Units",
            "job_count": "Job Count",
        },
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=max(400, top_n * 22))
    st.plotly_chart(fig, width="stretch")

# ── Week-over-week delta ──────────────────────────────────────────────────────
weeks_available = load_weeks()
if len(weeks_available) >= 2:
    st.subheader(f"📈 Week-over-Week Change — {dimension.title()}")
    wow = load_wow_delta(dimension, week_start)
    if not wow.empty:
        wow["delta_label"] = wow["pct_change"].apply(
            lambda x: f"+{x:.0f}%" if x >= 0 else f"{x:.0f}%"
        )
        fig_wow = px.bar(
            wow.head(top_n),
            x="pct_change",
            y="dimension_value",
            orientation="h",
            color="pct_change",
            color_continuous_scale="RdYlGn",
            range_color=[-100, 100],
            text="delta_label",
            labels={"dimension_value": dimension.title(), "pct_change": "WoW Change (%)"},
        )
        fig_wow.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=max(400, top_n * 22),
            coloraxis_showscale=False,
        )
        fig_wow.update_traces(textposition="outside")
        st.plotly_chart(fig_wow, width="stretch")

# ── Historical trend lines ────────────────────────────────────────────────────
if len(weeks_available) >= 2:
    st.subheader(f"📉 Signal History — Top {min(top_n, 10)} {dimension.title()}s")
    top_values = trends["dimension_value"].head(10).tolist() if not trends.empty else []
    history = load_trend_history(dimension, top_values)
    if not history.empty and history["week_start"].nunique() >= 2:
        fig_hist = px.line(
            history,
            x="week_start",
            y="signal_units",
            color="dimension_value",
            markers=True,
            labels={
                "week_start": "Week",
                "signal_units": "Signal Units",
                "dimension_value": dimension.title(),
            },
        )
        fig_hist.update_layout(height=400, xaxis_title="Week of")
        st.plotly_chart(fig_hist, width="stretch")
    else:
        st.info("Run the pipeline again next week to see trend history.")

# ── Skill heatmap ─────────────────────────────────────────────────────────────
if dimension == "skill":
    st.subheader("🗺️ Skill Demand Heatmap (all weeks)")
    conn = get_connection()
    heatmap_df = pd.read_sql_query(
        "SELECT week_start, dimension_value AS skill, signal_units "
        "FROM weekly_trends WHERE dimension = 'skill'",
        conn,
    )
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot_table(
            index="skill", columns="week_start", values="signal_units", fill_value=0
        )
        fig2 = px.imshow(pivot, color_continuous_scale="Blues", aspect="auto")
        st.plotly_chart(fig2, width="stretch")

# ── Ghost monitor ─────────────────────────────────────────────────────────────
if show_ghosts:
    st.subheader("👻 Ghost Job Monitor")
    ghosts = load_ghost_jobs()
    if ghosts.empty:
        st.success("No ghost jobs detected.")
    else:
        st.dataframe(ghosts, width="stretch")

# ── Signal feed ───────────────────────────────────────────────────────────────
st.subheader("📋 Signal Feed")

caption_parts = []
if target_roles:
    caption_parts.append(f"roles: {', '.join(target_roles)}")
if selected_companies:
    caption_parts.append(f"companies: {', '.join(selected_companies)}")
if feed_min_score > 0:
    caption_parts.append(f"score >= {feed_min_score:.2f}")
if role_keyword.strip():
    caption_parts.append(f'keyword: "{role_keyword.strip()}"')
if caption_parts:
    st.caption("Filtered to — " + " | ".join(caption_parts))

feed = load_signal_feed(
    target_roles=target_roles,
    companies=selected_companies,
    min_score=feed_min_score,
    keyword=role_keyword,
    limit=feed_limit,
)

if feed.empty:
    st.info("No jobs match the current filters.")
else:
    st.caption(f"{len(feed)} jobs shown")
    st.dataframe(feed, width="stretch")
