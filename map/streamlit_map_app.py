# map/streamlit_map_app.py
# ─────────────────────────────────────────────────────────────────────────────
# NYC Restaurant Intelligence — Interactive Inspection Map
# Streamlit in Snowflake deployment
#
# Authentication : get_active_session() — Snowflake manages identity.
# Data source    : SEMANTIC_VIEW(RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections)
#                  Single source of truth — identical numbers to the Cortex Agent.
# No agent dependency — pure session.sql() calls throughout.
#
# Sidebar filters:
#   Borough        — multiselect (all 5 boroughs; values are INITCAP from semantic view)
#   Grade          — multiselect (A B C Z P G N)
#   Cuisine type   — multiselect (top 20 by total_restaurants metric)
#   Date range     — date pickers (default: last 12 months)
#   Critical only  — checkbox (has_critical_violation dimension in inspections table)
#   Layer mode     — radio (scatter / heatmap)
#
# Main panel : pydeck map via st.pydeck_chart
# Below map  : summary table — restaurant, borough, cuisine,
#              inspection count, avg score, latest grade
#
# SEMANTIC_VIEW() column name mapping (semantic name → underlying expr):
#   restaurant_name  → RESTAURANT_NAME      (restaurants table)
#   borough          → BOROUGH              (restaurants table, INITCAP values)
#   cuisine_type     → CUISINE_DESCRIPTION  (restaurants table)
#   grade            → GRADE               (inspections table)
#   inspection_date  → INSPECTION_DATE     (inspections table)
#   has_critical_violation → HAS_CRITICAL_VIOLATION (inspections table, BOOLEAN)
#   latitude         → LATITUDE            (restaurants table, declared as FACT)
#   longitude        → LONGITUDE           (restaurants table, declared as FACT)
#   average_score    → AVG(INSPECTION_SCORE)  (inspections metric)
#   total_inspections → COUNT(FCT_INSPECTION_KEY) (inspections metric)
#   total_restaurants → COUNT(DISTINCT RESTAURANT_ID) (restaurants metric)
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ── Path setup — makes map_engine importable when run locally ─────────────────
_MAP_DIR = Path(__file__).parent
if str(_MAP_DIR) not in sys.path:
    sys.path.insert(0, str(_MAP_DIR))

from map_engine import build_deck, GRADE_COLORS  # noqa: E402

# ── Semantic view reference ───────────────────────────────────────────────────
# Single constant — all queries use this, never direct MARTS table names.
_SV_NAME = "RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections"

_ALL_GRADES   = ["A", "B", "C", "Z", "P", "G", "N"]
_DEFAULT_DAYS = 365

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Restaurant Map",
    page_icon="🗺️",
    layout="wide",
)

# ── Session ───────────────────────────────────────────────────────────────────
session = get_active_session()


# ── SEMANTIC_VIEW() builder ───────────────────────────────────────────────────

def _sv(
    dimensions: str = "",
    facts: str = "",
    metrics: str = "",
) -> str:
    """
    Return the SEMANTIC_VIEW() table-function call as a SQL fragment.

    Enforces the governed layer — never references MARTS tables directly.
    latitude and longitude must appear in the FACTS clause (declared as facts
    in the semantic view YAML, not dimensions).
    """
    parts = [f"    {_SV_NAME}"]
    if dimensions:
        parts.append(f"    DIMENSIONS {dimensions}")
    if facts:
        parts.append(f"    FACTS {facts}")
    if metrics:
        parts.append(f"    METRICS {metrics}")
    return "SEMANTIC_VIEW(\n" + "\n".join(parts) + "\n    )"


# ── SQL helper: per-restaurant two-CTE pattern ────────────────────────────────
#
# All map and summary queries share the same structure:
#
#   sv           — raw SEMANTIC_VIEW() result, filtered by date + borough + cuisine
#   per_restaurant — one row per restaurant with latest grade, avg score, flags
#
# Grade and critical filters are applied AFTER aggregation (on the latest grade
# and any_critical flag) so that the map's grade colours reflect the restaurant's
# most recent inspection outcome — not which individual inspections matched the
# filter. This matches the original direct-SQL behaviour.

def _build_per_restaurant_ctes(
    start_date: date,
    end_date: date,
    boroughs: list[str],
    cuisines: list[str],
) -> str:
    """
    Return the WITH sv_coords AS (...), sv_insp AS (...), per_restaurant AS (...)
    CTE block.

    Snowflake SEMANTIC_VIEW() constraints (discovered empirically):
      1. FACTS and METRICS cannot coexist in the same call.
      2. When FACTS are present, ALL DIMENSIONS must come from the same entity
         as the facts — cross-entity dimensions are rejected.
      3. inspection_date exists on both inspections and violations entities;
         qualify it as inspections.inspection_date to disambiguate.

    Three-CTE pattern that satisfies all three constraints:
      sv_coords — restaurants entity only: restaurant_name (dim) +
                  latitude, longitude (facts). No cross-entity dims, no metrics.
                  GROUP BY collapses to one row per restaurant (prevents JOIN fan-out).
      sv_insp   — no FACTS clause, so entities can mix freely.
                  Inspection dimensions + metrics, date/borough/cuisine filtered.
      per_restaurant — JOINs sv_coords + sv_insp on restaurant_name,
                  aggregates to one row per restaurant.

    Grade and critical filters are intentionally excluded — apply them in the
    final SELECT that wraps these CTEs.
    """
    insp_clauses: list[str] = [
        f"inspection_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')",
    ]
    if boroughs:
        quoted = ", ".join(f"'{b}'" for b in boroughs)
        insp_clauses.append(f"borough IN ({quoted})")
    if cuisines:
        quoted = ", ".join(
            f"'{c.replace(chr(39), chr(39)+chr(39))}'" for c in cuisines
        )
        insp_clauses.append(f"cuisine_type IN ({quoted})")

    insp_where = "\n          AND ".join(insp_clauses)

    return f"""
    sv_coords AS (
        -- Restaurants entity only.
        -- FACTS constraint: when FACTS are present, all DIMENSIONS must come
        -- from the same entity as the facts. latitude/longitude are facts on
        -- the restaurants entity, so only restaurants dimensions are allowed here.
        -- GROUP BY ensures one row per restaurant — prevents fan-out on the JOIN.
        SELECT   restaurant_name,
                 MAX(latitude)  AS latitude,
                 MAX(longitude) AS longitude
        FROM     {_sv(
            dimensions="restaurant_name",
            facts="latitude, longitude",
        )}
        GROUP BY restaurant_name
    ),
    sv_insp AS (
        -- No FACTS clause — entities can be mixed freely.
        -- inspections.inspection_date is qualified to disambiguate from
        -- violations.inspection_date (same name exists on two entities).
        SELECT *
        FROM   {_sv(
            dimensions="restaurant_name, borough, cuisine_type, "
                       "inspections.inspection_date, grade, has_critical_violation",
            metrics="average_score, total_inspections",
        )}
        WHERE  {insp_where}
    ),
    per_restaurant AS (
        SELECT
            i.restaurant_name,
            i.borough,
            i.cuisine_type,
            MAX(c.latitude)                             AS latitude,
            MAX(c.longitude)                            AS longitude,
            MAX_BY(i.grade, i.inspection_date)          AS latest_grade,
            ROUND(AVG(i.average_score), 1)              AS avg_score,
            SUM(i.total_inspections)                    AS inspection_count,
            MAX(i.has_critical_violation::INT)::BOOLEAN AS any_critical
        FROM sv_insp i
        JOIN sv_coords c
            ON  i.restaurant_name = c.restaurant_name
            AND c.latitude        IS NOT NULL
            AND c.longitude       IS NOT NULL
        GROUP BY i.restaurant_name, i.borough, i.cuisine_type
    )"""


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_boroughs() -> list[str]:
    """
    Distinct boroughs from the restaurants dimension via the semantic view.
    Named filter equivalent: no named filter exists for borough exclusion;
    we explicitly exclude '0' (missing borough sentinel per the YAML spec).
    """
    sql = f"""
    SELECT borough
    FROM {_sv(dimensions="borough", metrics="total_restaurants")}
    WHERE borough IS NOT NULL
      AND borough != '0'
    ORDER BY borough
    """
    return [r[0] for r in session.sql(sql).collect()]


@st.cache_data(ttl=3600)
def load_top_cuisines(n: int = 20) -> list[str]:
    """
    Top N cuisines by distinct restaurant count via the total_restaurants metric.
    Uses the semantic name cuisine_type (expr: CUISINE_DESCRIPTION in the YAML).
    """
    sql = f"""
    SELECT cuisine_type
    FROM {_sv(dimensions="cuisine_type", metrics="total_restaurants")}
    WHERE cuisine_type IS NOT NULL
    ORDER BY total_restaurants DESC
    LIMIT {n}
    """
    return [r[0] for r in session.sql(sql).collect()]


@st.cache_data(ttl=300)
def load_map_data(
    boroughs: list[str],
    grades: list[str],
    cuisines: list[str],
    start_date: date,
    end_date: date,
    critical_only: bool,
) -> pd.DataFrame:
    """
    Return one row per restaurant — the restaurant's latest grade within the
    selected date range — filtered by sidebar selections.

    Query pattern (two CTEs):
      sv             — all inspections in range, borough/cuisine filtered
      per_restaurant — one row per restaurant: MAX_BY(grade, inspection_date),
                       MAX(latitude), any_critical flag, avg_score
    Grade and critical_only filters are applied in the final WHERE so they act
    on the latest-grade outcome, not on individual inspection rows. This ensures
    the map colour reflects the restaurant's most recent inspection status.

    Columns returned match map_engine.build_deck() requirements:
      DBA, BOROUGH, LATITUDE, LONGITUDE, CUISINE, GRADE, SCORE
    """
    outer_clauses: list[str] = []
    if grades:
        quoted = ", ".join(f"'{g}'" for g in grades)
        outer_clauses.append(f"latest_grade IN ({quoted})")
    if critical_only:
        outer_clauses.append("any_critical = TRUE")

    outer_where = (
        "WHERE " + "\n      AND ".join(outer_clauses)
        if outer_clauses else ""
    )

    sql = f"""
    WITH {_build_per_restaurant_ctes(start_date, end_date, boroughs, cuisines)}
    SELECT
        restaurant_name   AS DBA,
        borough           AS BOROUGH,
        latitude          AS LATITUDE,
        longitude         AS LONGITUDE,
        cuisine_type      AS CUISINE,
        latest_grade      AS GRADE,
        avg_score         AS SCORE
    FROM per_restaurant
    {outer_where}
    """
    return session.sql(sql).to_pandas()


@st.cache_data(ttl=300)
def load_summary_table(
    boroughs: list[str],
    grades: list[str],
    cuisines: list[str],
    start_date: date,
    end_date: date,
    critical_only: bool,
) -> pd.DataFrame:
    """
    Per-restaurant summary for the selected filters.

    Inspection count  = total_inspections metric (COUNT FCT_INSPECTION_KEY),
                        the governed metric used by the Cortex Agent.
    Avg score         = average_score metric (AVG INSPECTION_SCORE).
    Latest grade      = MAX_BY(grade, inspection_date) — most recent grade
                        in the date range.

    Grade filter applied on latest_grade (post-aggregation), so "Inspections"
    counts all inspection events for the restaurant in the period, not just
    those matching the selected grade — consistent with the map behaviour.
    """
    outer_clauses: list[str] = []
    if grades:
        quoted = ", ".join(f"'{g}'" for g in grades)
        outer_clauses.append(f"latest_grade IN ({quoted})")
    if critical_only:
        outer_clauses.append("any_critical = TRUE")

    outer_where = (
        "WHERE " + "\n      AND ".join(outer_clauses)
        if outer_clauses else ""
    )

    sql = f"""
    WITH {_build_per_restaurant_ctes(start_date, end_date, boroughs, cuisines)}
    SELECT
        restaurant_name   AS "Restaurant",
        borough           AS "Borough",
        cuisine_type      AS "Cuisine",
        inspection_count  AS "Inspections",
        avg_score         AS "Avg Score",
        latest_grade      AS "Latest Grade"
    FROM per_restaurant
    {outer_where}
    ORDER BY inspection_count DESC, avg_score ASC
    LIMIT 500
    """
    return session.sql(sql).to_pandas()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🗺️ NYC Restaurant Map")
    st.caption("Via `SEMANTIC_VIEW(nyc_restaurant_inspections)`")
    st.divider()

    # Borough — values come from the semantic view (INITCAP: "Manhattan", etc.)
    all_boroughs = load_boroughs()
    sel_boroughs = st.multiselect(
        "Borough",
        options=all_boroughs,
        default=all_boroughs,
        help="borough dimension from restaurants table (INITCAP values)"
    )

    # Grade
    sel_grades = st.multiselect(
        "Grade",
        options=_ALL_GRADES,
        default=_ALL_GRADES,
        help="Applied to the restaurant's latest grade in the date range. "
             "Equivalent named filter for A/B/C only: graded_inspections"
    )

    # Cuisine — populated via total_restaurants metric (semantic name: cuisine_type)
    top_cuisines = load_top_cuisines(20)
    sel_cuisines = st.multiselect(
        "Cuisine type (top 20)",
        options=top_cuisines,
        default=[],
        help="cuisine_type dimension (expr: CUISINE_DESCRIPTION). "
             "Leave empty to include all cuisines."
    )

    # Date range
    st.markdown("**Date range**")
    default_end   = date.today()
    default_start = default_end - timedelta(days=_DEFAULT_DAYS)
    col_s, col_e  = st.columns(2)
    sel_start = col_s.date_input("From", value=default_start, key="date_start")
    sel_end   = col_e.date_input("To",   value=default_end,   key="date_end")
    if sel_start > sel_end:
        st.error("'From' date must be before 'To' date.")
        st.stop()

    # Critical violations only — has_critical_violation dimension (BOOLEAN)
    sel_critical = st.checkbox(
        "Critical violations only",
        value=False,
        help="Shows restaurants with at least one critical violation "
             "in the selected date range (has_critical_violation = TRUE)."
    )

    st.divider()

    # Layer mode
    sel_layer = st.radio(
        "Map layer",
        options=["scatter", "heatmap"],
        index=0,
        horizontal=True,
        help="Scatter: colour-coded by grade. Heatmap: inspection density."
    )

    st.divider()
    st.caption(
        "Source: `SEMANTIC_VIEW(nyc_restaurant_inspections)`  \n"
        "Produces identical numbers to the Cortex Agent."
    )


# ── Main panel ────────────────────────────────────────────────────────────────

st.title("NYC Restaurant Inspection Map")
st.caption(
    "One point per restaurant — colour reflects the most recent grade "
    "within the selected date range. Hover for details."
)

effective_cuisines = sel_cuisines if sel_cuisines else []

with st.spinner("Loading inspection data…"):
    map_df = load_map_data(
        boroughs      = sel_boroughs,
        grades        = sel_grades,
        cuisines      = effective_cuisines,
        start_date    = sel_start,
        end_date      = sel_end,
        critical_only = sel_critical,
    )

# ── KPI strip ─────────────────────────────────────────────────────────────────
total   = len(map_df)
graded  = map_df[map_df["GRADE"].notna() & (map_df["GRADE"] != "")].shape[0]
pct_a   = (
    round(100 * (map_df["GRADE"] == "A").sum() / graded, 1)
    if graded else 0
)
avg_score = (
    round(map_df["SCORE"].apply(pd.to_numeric, errors="coerce").mean(), 1)
    if total else "–"
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Restaurants shown",    f"{total:,}")
k2.metric("With a grade",         f"{graded:,}")
k3.metric("Grade A rate",         f"{pct_a}%")
k4.metric("Avg inspection score", avg_score, help="Lower = better (penalty-based)")

st.divider()

# ── Grade legend ──────────────────────────────────────────────────────────────
legend_cols = st.columns(len(GRADE_COLORS) + 1)
labels = {
    "A":     "Grade A — 0–13 pts",
    "B":     "Grade B — 14–27 pts",
    "C":     "Grade C — 28+ pts",
    "other": "Z / P / G / N / ungraded",
}
color_css = {
    "A":     "rgb(0,180,0)",
    "B":     "rgb(255,165,0)",
    "C":     "rgb(220,50,50)",
    "other": "rgb(150,150,150)",
}
for col, (key, label) in zip(legend_cols, labels.items()):
    col.markdown(
        f'<span style="display:inline-block;width:12px;height:12px;'
        f'border-radius:50%;background:{color_css[key]};margin-right:5px"></span>'
        f'<small>{label}</small>',
        unsafe_allow_html=True,
    )

st.divider()

# ── Map ────────────────────────────────────────────────────────────────────────
if map_df.empty:
    st.info(
        "No restaurants match the current filters. "
        "Try broadening the date range or removing a filter."
    )
else:
    deck = build_deck(map_df, layer_mode=sel_layer)
    st.pydeck_chart(deck, use_container_width=True)

st.divider()

# ── Summary table ─────────────────────────────────────────────────────────────
st.subheader("Restaurant summary")
st.caption(
    "Inspection count uses the `total_inspections` metric "
    "(COUNT FCT_INSPECTION_KEY — same definition as the Cortex Agent). "
    "Sorted by inspection count descending, capped at 500 rows."
)

with st.spinner("Loading summary…"):
    summary_df = load_summary_table(
        boroughs      = sel_boroughs,
        grades        = sel_grades,
        cuisines      = effective_cuisines,
        start_date    = sel_start,
        end_date      = sel_end,
        critical_only = sel_critical,
    )

if summary_df.empty:
    st.info("No data for summary table.")
else:
    st.dataframe(summary_df, use_container_width=True)
    st.caption(f"Showing {len(summary_df):,} restaurants.")
