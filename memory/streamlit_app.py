# memory/streamlit_app.py
# ─────────────────────────────────────────────────────────────────────────────
# NYC Restaurant Intelligence — Phase 5: Persistent Memory
# Streamlit in Snowflake deployment
#
# Authentication: get_active_session() — Snowflake manages identity at the
#   platform level. CURRENT_USER() provides a trustworthy user_id. The session's
#   OAuth token is used to call the Cortex Agent REST API with
#   X-Snowflake-Authorization-Token-Type: OAUTH.
#   Token retrieval varies by SiS runtime version (both are private/undocumented):
#     Newer: session.connection.rest.token
#     Older: session.connection._rest._token   ← warehouse runtime fallback
#
# This is correct for Streamlit in Snowflake (SiS). For CLI/local access use
# agent_with_memory.py which uses JWT key-pair auth instead.
#
# Sidebar panels:
#   🧠  What I know about you   — persistent memory facts, editable in-session
#   📅  Data freshness          — most recent inspection date from last query
#   🔍  Filters applied         — implicit filters the agent applied last turn
#
# Deploy:
#   python memory/upload_to_stage.py   (uploads 3 files — no cortex_agent.py needed)
#   to @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE, then:
#   CREATE STREAMLIT ... ROOT_LOCATION = '@...' MAIN_FILE = 'streamlit_app.py'
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import streamlit as st
from snowflake.snowpark.context import get_active_session

# ── Path setup (memory_manager and response_parser are on the same stage dir) ─
_MEM_DIR = Path(__file__).parent
if str(_MEM_DIR) not in sys.path:
    sys.path.insert(0, str(_MEM_DIR))

from memory_manager import MemoryManager   # noqa: E402
from response_parser import parse_response  # noqa: E402
from map_engine import build_deck           # noqa: E402

# ── Agent constants (inlined — no dependency on cortex_agent.py) ─────────────
# cortex_agent.py reads SNOWFLAKE_ACCOUNT from os.environ at module level, which
# raises KeyError in SiS where no .env is loaded. These four values are the only
# things streamlit_app.py needs; inlining them makes this file fully self-contained.

CORTEX_TOOLS = [
    {
        "tool_spec": {
            "type": "cortex_analyst_text_to_sql",
            "name": "nyc_inspection_analyst",
        },
    },
    {
        "tool_spec": {
            "type": "cortex_search",
            "name": "nyc_health_docs_search",
        },
    },
]

CORTEX_TOOL_RESOURCES = {
    "nyc_inspection_analyst": {
        "semantic_view": "RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections",
        "execution_environment": {
            "type": "warehouse",
            "warehouse": "RESTAURANT_WH",
        },
    },
    "nyc_health_docs_search": {
        "search_service": "RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search",
    },
}

_SYSTEM_INSTRUCTION = """You are a NYC public health analytics assistant built for DOHMH regulators,
restaurant operators, and public health researchers. You have access to live
inspection data (296k+ records) and official NYC Health Code documents.

You provide accurate, citation-backed answers. You never fabricate violation
codes, section numbers, or penalty amounts. If you cannot find the specific
information requested, say so clearly rather than guessing.

HARD RULES — these override all other instructions:
- Proximity queries ("near", "close to", "within walking distance"): use exactly
  0.5-mile (800-meter) radius. NEVER use 1 mile or more unless the user
  explicitly asks for a larger radius.
- "recent" means the last 6 months. NEVER treat "recent" as 12 months.
- "this year" means YEAR(inspection_date) = YEAR(CURRENT_DATE).
- When reporting results, always distinguish VIOLATION RECORDS from RESTAURANTS:
  e.g. "358 violations across 37 restaurants" — never say "358 restaurants" when
  you mean violation rows."""

_ORCHESTRATION_INSTRUCTION = """TOOL SELECTION:
- NUMBERS, COUNTS, RATES, TRENDS, RANKINGS, COMPARISONS → nyc_inspection_analyst ONLY
- DEFINITIONS, LEGAL TEXT, CONDITION LEVELS, ENFORCEMENT RULES, PENALTY SCHEDULES, CLOSURE TRIGGERS → nyc_health_docs_search ONLY
- Questions combining data AND regulatory context → use BOTH tools
- When unsure which tool → try nyc_inspection_analyst first, then supplement with nyc_health_docs_search if the answer needs regulatory context

MULTI-TOOL SEQUENCING:
- When using both tools, query the structured data FIRST to identify specific violation codes or patterns, THEN search documents for those specific codes

AMBIGUITY HANDLING:
- If a question could apply to inspections OR violations table, prefer the inspections table
- "worst" or "best" restaurants → rank by average_score (lower = better)
- "most violations" → count from violations table, not inspections
- If a borough is not specified, include all boroughs but exclude borough = '0' (data quality artifact)

TIME DEFAULTS:
- If the user does not specify a date range or time period, default to the last 12 months

DATA FRESHNESS:
- For EVERY question routed to nyc_inspection_analyst, you MUST determine
  the most recent inspection date in the dataset by querying
  MAX(inspection_date) from the inspections data
- Include this date in your response context so the response layer can display it"""

_RESPONSE_INSTRUCTION = """DATA FRESHNESS REQUIREMENT:
Every response that uses inspection data MUST begin with:
📅 Data through: <most recent inspection date from MAX(inspection_date)>
Display this line BEFORE the filters section. If the query did not
involve inspection data (e.g., pure document search), omit this line.

TRANSPARENCY REQUIREMENT:
Whenever you apply a filter or condition that the user did NOT explicitly
request in their question, you MUST state it at the top of your response
in this format:
🔍 Filters applied:
   - <condition> — <reason>
Examples:
  - grade IN ('A','B','C') → "Final grades only — pre-permit and pending excluded"
  - inspection_date >= DATEADD(month, -12, CURRENT_DATE) → "Last 12 months (default when no date specified)"
  - borough != '0' → "Excluded records with missing borough data"
If no implicit filters were applied, omit this section entirely.

FORMATTING:
- Scores are penalty-based: LOWER scores = BETTER performance. Never describe high scores as "good."
- Round percentages to 1 decimal place. Round scores to whole numbers.
- When citing health code sections, include the section number (e.g., §81.09).
- When showing violation codes, include the human-readable description."""



def _parse_sse(resp) -> list[dict]:
    """Consume a streaming SSE response and return all parsed event dicts."""
    events = []
    event_type = None
    for line in resp.iter_lines():
        if not line:
            continue
        raw = line.decode("utf-8")
        if raw.startswith("event: "):
            event_type = raw[7:].strip()
            continue
        if not raw.startswith("data: "):
            continue
        data = raw[6:].strip()
        if data in ("[DONE]", ""):
            break
        try:
            event = json.loads(data)
            event["_sse_event"] = event_type
            events.append(event)
        except json.JSONDecodeError:
            continue
    return events

# ── Geo-question detection & map data ────────────────────────────────────────

import re as _re
from datetime import date as _date, timedelta as _timedelta

_GEO_RE = _re.compile(
    r"\b(near|close to|within|around|vicinity|"
    r"near my (home|house|place|address|location)|"
    r"show.*map|map of|on a map|geospatial|coordinates?)\b",
    _re.I,
)

_SV = "RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections"


def _build_result_df(table_events: list[dict]) -> "pd.DataFrame | None":
    """
    Reconstruct a pandas DataFrame from all response.table events.
    Used to display the agent's structured results as a grid + CSV download.
    Returns None when no table data is available.
    """
    import pandas as pd  # noqa: PLC0415

    all_rows: list = []
    col_names: list[str] | None = None

    for evt in table_events:
        try:
            rs = evt["data"]["result_set"]
            if col_names is None:
                col_names = [c["name"] for c in rs["resultSetMetaData"]["rowType"]]
            for item in rs["data"]:
                if not isinstance(item, list):
                    continue
                if item and isinstance(item[0], list):
                    all_rows.extend(item)
                else:
                    all_rows.append(item)
        except Exception:
            continue

    if not col_names or not all_rows:
        return None
    return pd.DataFrame(all_rows, columns=col_names)


def _is_geo_question(text: str) -> bool:
    return bool(_GEO_RE.search(text))


def _extract_agent_sql(raw_events: list[dict]) -> str | None:
    """Return the first SQL string from response.tool_result events."""
    for evt in raw_events:
        if evt.get("event") != "response.tool_result":
            continue
        try:
            for item in evt["data"]["content"]:
                sql = item.get("json", {}).get("sql", "")
                if sql:
                    return sql
        except Exception:
            continue
    return None


def _extract_home_coords(sql: str) -> "tuple[float, float] | None":
    """
    Parse (lat, lng) from ST_MAKEPOINT(lng, lat) in the agent's SQL.
    Cortex Analyst follows ST_MAKEPOINT(longitude, latitude) convention.
    Returns None if no match.
    """
    m = _re.search(
        r"ST_MAKEPOINT\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*\)",
        sql,
    )
    if m:
        return float(m.group(2)), float(m.group(1))  # (lat, lng)
    return None


def _load_map_data_from_geo(
    sf_session,
    lat: float,
    lng: float,
    radius_m: float = 804.0,
) -> "pd.DataFrame | None":
    """
    Query MARTS tables directly (same tables as the agent's Cortex Analyst SQL)
    for restaurants within radius_m metres of (lat, lng) with recent critical
    violations. Bypasses SEMANTIC_VIEW so ST_DISTANCE filtering is exact.
    """
    import pandas as pd  # noqa: PLC0415

    sql = f"""
    SELECT
        MAX(r.restaurant_name)                       AS DBA,
        MAX(r.street_address)                        AS STREET_ADDRESS,
        MAX(r.borough)                               AS BOROUGH,
        MAX(r.cuisine_description)                   AS CUISINE,
        MAX(r.latitude)                              AS LATITUDE,
        MAX(r.longitude)                             AS LONGITUDE,
        MAX_BY(i.grade,          v.inspection_date)  AS GRADE,
        ROUND(AVG(i.inspection_score), 1)            AS SCORE,
        TO_VARCHAR(MAX(v.inspection_date), 'YYYY-MM-DD') AS LAST_INSPECTION
    FROM RESTAURANT_INTELLIGENCE.MARTS.DIM_RESTAURANT   r
    JOIN RESTAURANT_INTELLIGENCE.MARTS.FCT_VIOLATIONS   v
        ON  r.dim_restaurant_key = v.dim_restaurant_key
    JOIN RESTAURANT_INTELLIGENCE.MARTS.FCT_INSPECTIONS  i
        ON  v.fct_inspection_key = i.fct_inspection_key
    WHERE v.is_critical_violation = TRUE
      AND v.inspection_date >= DATEADD(MONTH, -6, CURRENT_DATE)
      AND r.latitude  IS NOT NULL
      AND r.longitude IS NOT NULL
      AND ST_DISTANCE(
            ST_MAKEPOINT(r.longitude, r.latitude),
            ST_MAKEPOINT({lng}, {lat})
          ) <= {radius_m}
    GROUP BY r.dim_restaurant_key
    """
    try:
        return sf_session.sql(sql).to_pandas()
    except Exception as exc:
        st.warning(f"Map data unavailable: {exc}")
        return None


def _build_map_df_from_table(table_events: list[dict]) -> "pd.DataFrame | None":
    """
    Build a map-ready DataFrame directly from response.table events when the
    agent's result set includes LATITUDE and LONGITUDE columns.

    Avoids a separate SEMANTIC_VIEW query — the agent's own coordinates are
    used, so the map shows exactly the restaurants the agent identified
    (correct proximity filter, correct count).

    Returns None if no lat/lng columns are present (caller falls back to
    _load_map_data_from_pairs).

    map_engine.build_deck() requires: LATITUDE, LONGITUDE, DBA, GRADE, SCORE, BOROUGH
    CUISINE is optional.
    """
    import pandas as pd  # noqa: PLC0415

    all_rows: list = []
    col_names: list[str] | None = None

    for evt in table_events:
        try:
            result_set = evt["data"]["result_set"]
            if col_names is None:
                col_names = [c["name"] for c in result_set["resultSetMetaData"]["rowType"]]
            data = result_set["data"]
            for item in data:
                if not isinstance(item, list):
                    continue
                if item and isinstance(item[0], list):
                    all_rows.extend(item)   # nested block format
                else:
                    all_rows.append(item)   # flat row format
        except Exception:
            continue

    if not col_names or not all_rows:
        return None
    if "LATITUDE" not in col_names or "LONGITUDE" not in col_names:
        return None  # no coordinates — caller will use SEMANTIC_VIEW fallback

    df = pd.DataFrame(all_rows, columns=col_names)
    df["LATITUDE"]  = pd.to_numeric(df["LATITUDE"],  errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])
    if df.empty:
        return None

    # Aggregate to one row per restaurant (multiple violation rows → one map point)
    group_cols = [c for c in ["RESTAURANT_NAME", "BOROUGH"] if c in df.columns]
    agg_dict: dict = {"LATITUDE": "first", "LONGITUDE": "first"}
    if "CUISINE_TYPE"     in df.columns: agg_dict["CUISINE_TYPE"]     = "first"
    if "STREET_ADDRESS"   in df.columns: agg_dict["STREET_ADDRESS"]   = "first"
    if "INSPECTION_SCORE" in df.columns: agg_dict["INSPECTION_SCORE"] = "mean"

    agg = df.groupby(group_cols, as_index=False).agg(agg_dict)
    agg = agg.rename(columns={"RESTAURANT_NAME": "DBA", "CUISINE_TYPE": "CUISINE"})

    # Infer GRADE from average inspection score (penalty-based: lower = better)
    if "INSPECTION_SCORE" in agg.columns:
        def _infer_grade(s: float) -> str:
            if pd.isna(s): return ""
            return "A" if s <= 13 else "B" if s <= 27 else "C"
        agg["GRADE"] = agg["INSPECTION_SCORE"].apply(_infer_grade)
        agg["SCORE"] = agg["INSPECTION_SCORE"].round().astype("Int64")
        agg = agg.drop(columns=["INSPECTION_SCORE"])
    else:
        agg["GRADE"] = ""
        agg["SCORE"] = None

    if "CUISINE"  not in agg.columns: agg["CUISINE"]  = ""
    if "BOROUGH"  not in agg.columns: agg["BOROUGH"]  = ""

    return agg


def _extract_name_borough_pairs(table_events: list[dict]) -> list[tuple[str, str]]:
    """
    Pull unique (RESTAURANT_NAME, BOROUGH) pairs from all response.table events.
    Collecting all events handles multi-partition result sets where a single event
    only carries a subset of rows.
    Using both name + borough prevents cross-borough name collisions on the map.
    """
    pairs: set[tuple[str, str]] = set()
    for table_event in table_events:
        try:
            result_set = table_event["data"]["result_set"]
            col_names  = [c["name"] for c in result_set["resultSetMetaData"]["rowType"]]
            name_idx    = col_names.index("RESTAURANT_NAME") if "RESTAURANT_NAME" in col_names else -1
            borough_idx = col_names.index("BOROUGH")         if "BOROUGH"          in col_names else -1
            if name_idx < 0:
                continue
            for row in result_set["data"]:
                if not isinstance(row, list):
                    continue
                name    = row[name_idx]    if len(row) > name_idx    and row[name_idx]    else None
                borough = row[borough_idx] if borough_idx >= 0 and len(row) > borough_idx and row[borough_idx] else ""
                if name:
                    pairs.add((str(name), str(borough)))
        except Exception:
            continue
    return list(pairs)


def _load_map_data_from_pairs(
    sf_session,
    name_borough_pairs: list[tuple[str, str]],
) -> "pd.DataFrame | None":
    """
    Query SEMANTIC_VIEW for coordinates of restaurants identified by
    (name, borough) pairs. Filtering on both columns eliminates cross-borough
    name collisions (e.g. a Starbucks in Brooklyn matching a UWS result).
    """
    import pandas as pd  # noqa: PLC0415

    if not name_borough_pairs:
        return None

    def _q(s: str) -> str:
        return s.replace("'", "''")

    # Build OR-joined conditions for each (name, borough) pair (cap at 300)
    conditions = " OR ".join(
        f"(restaurant_name = '{_q(n)}' AND borough = '{_q(b)}')"
        if b else
        f"restaurant_name = '{_q(n)}'"
        for n, b in name_borough_pairs[:300]
    )

    # Name-only list for sv_coords (FACTS clause requires same-entity dimensions —
    # borough is also on restaurants entity but keeping it out is safer and matches
    # the proven three-CTE pattern from the map app).
    name_only_in = ", ".join(
        f"'{_q(n)}'" for n, _ in name_borough_pairs[:300]
    )

    sql = f"""
    WITH sv_coords AS (
        -- restaurants entity only (FACTS constraint: same-entity dims only)
        SELECT restaurant_name,
               MAX(latitude)  AS latitude,
               MAX(longitude) AS longitude
        FROM SEMANTIC_VIEW(
            {_SV}
            DIMENSIONS restaurant_name
            FACTS latitude, longitude
        )
        WHERE restaurant_name IN ({name_only_in})
        GROUP BY restaurant_name
    ),
    sv_insp AS (
        -- No FACTS → entities can mix freely; filter by (name, borough) pairs
        -- to avoid cross-borough name collisions.
        -- cuisine_type intentionally excluded from GROUP BY — one row per restaurant.
        SELECT *
        FROM SEMANTIC_VIEW(
            {_SV}
            DIMENSIONS restaurant_name, borough,
                       inspections.inspection_date, grade, has_critical_violation
            METRICS average_score
        )
        WHERE ({conditions})
          AND has_critical_violation = TRUE
    ),
    per_restaurant AS (
        SELECT
            i.restaurant_name                           AS DBA,
            i.borough                                   AS BOROUGH,
            MAX(c.latitude)                             AS LATITUDE,
            MAX(c.longitude)                            AS LONGITUDE,
            MAX_BY(i.grade, i.inspection_date)          AS GRADE,
            ROUND(AVG(i.average_score), 1)              AS SCORE
        FROM sv_insp i
        JOIN sv_coords c
            ON  i.restaurant_name = c.restaurant_name
            AND c.latitude  IS NOT NULL
            AND c.longitude IS NOT NULL
        GROUP BY i.restaurant_name, i.borough
    )
    SELECT * FROM per_restaurant
    """

    try:
        return sf_session.sql(sql).to_pandas()
    except Exception as exc:
        st.warning(f"Map data unavailable: {exc}")
        return None


def _load_map_data(sf_session, critical_only: bool = True) -> "pd.DataFrame | None":
    """
    Query the semantic view for restaurant coordinates using the three-CTE pattern.
    Returns a pandas DataFrame with columns expected by map_engine.build_deck():
      LATITUDE, LONGITUDE, DBA, GRADE, SCORE, BOROUGH, CUISINE
    Returns None on error or when no rows found.
    """
    import pandas as pd  # available in SiS runtime

    end_dt   = _date.today()
    start_dt = end_dt - _timedelta(days=365)

    critical_clause = "AND has_critical_violation = TRUE" if critical_only else ""

    sql = f"""
    WITH sv_coords AS (
        SELECT restaurant_name,
               MAX(latitude)  AS latitude,
               MAX(longitude) AS longitude
        FROM SEMANTIC_VIEW(
            {_SV}
            DIMENSIONS restaurant_name
            FACTS latitude, longitude
        )
        GROUP BY restaurant_name
    ),
    sv_insp AS (
        SELECT *
        FROM SEMANTIC_VIEW(
            {_SV}
            DIMENSIONS restaurant_name, borough,
                       inspections.inspection_date, grade, has_critical_violation
            METRICS average_score, total_inspections
        )
        WHERE inspection_date BETWEEN DATE('{start_dt}') AND DATE('{end_dt}')
        {critical_clause}
    ),
    per_restaurant AS (
        SELECT
            i.restaurant_name                           AS DBA,
            i.borough                                   AS BOROUGH,
            MAX(c.latitude)                             AS LATITUDE,
            MAX(c.longitude)                            AS LONGITUDE,
            MAX_BY(i.grade, i.inspection_date)          AS GRADE,
            ROUND(AVG(i.average_score), 1)              AS SCORE
        FROM sv_insp i
        JOIN sv_coords c
            ON  i.restaurant_name = c.restaurant_name
            AND c.latitude  IS NOT NULL
            AND c.longitude IS NOT NULL
        GROUP BY i.restaurant_name, i.borough
    )
    SELECT * FROM per_restaurant
    LIMIT 2000
    """

    try:
        return sf_session.sql(sql).to_pandas()
    except Exception as exc:
        st.warning(f"Map data unavailable: {exc}")
        return None


# ── Streamlit version compat ──────────────────────────────────────────────────
# _rerun() was added in 1.27; older SiS runtimes use st.experimental_rerun().
def _rerun() -> None:
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()  # type: ignore[attr-defined]


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Restaurant Intelligence",
    page_icon="🍽️",
    layout="wide",
)

# ── Snowflake session ─────────────────────────────────────────────────────────
session = get_active_session()
user_id = session.sql("SELECT CURRENT_USER()").collect()[0][0]
_TABLE  = "RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY"


# ── Cortex Agent call via _snowflake.send_snow_api_request ───────────────────
#
# In SiS warehouse runtime:
#   - OAuth token retrieval via session.connection.rest is not available
#     (StoredProcRestful does not expose the token).
#   - Outbound requests via `requests` require External Access Integration.
#
# _snowflake.send_snow_api_request is the correct pattern for warehouse runtime.
# It calls Snowflake REST endpoints internally — no token, no EAI needed.
# SQL execution (Cortex Analyst text-to-SQL) is handled server-side.
# Response is non-streaming: the full answer is returned at once.
import _snowflake  # noqa: E402 — available only inside SiS; not importable locally


def _call_agent_sis(
    question: str,
    conversation_history: list[dict] | None = None,
) -> tuple[str, list]:
    """
    Call the Cortex Agent from SiS warehouse runtime.

    Uses _snowflake.send_snow_api_request — the internal API that handles
    Snowflake auth automatically. No OAuth token retrieval needed.
    Cortex Analyst SQL (client_side_execute) is executed server-side.
    Returns (full_response_text, raw_events) — raw_events for debugging.
    """
    messages = (conversation_history or []) + [
        {"role": "user", "content": [{"type": "text", "text": question}]}
    ]

    payload = {
        "models":         {"orchestration": "claude-4-sonnet"},
        "tools":          CORTEX_TOOLS,
        "tool_resources": CORTEX_TOOL_RESOURCES,
        "messages":       messages,
        "instructions": {
            "system":        _SYSTEM_INSTRUCTION,
            "orchestration": _ORCHESTRATION_INSTRUCTION,
            "response":      _RESPONSE_INSTRUCTION,
        },
    }

    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/agent:run",
        {},       # headers — auth is injected automatically
        {},       # query params
        payload,  # body as dict (not JSON string)
        None,     # request_guid
        7200,     # timeout seconds
    )

    if resp.get("status") != 200:
        raise RuntimeError(
            f"Cortex Agent returned HTTP {resp.get('status')}: {resp.get('reason')}"
        )

    raw_content = resp["content"]
    # content may be a JSON string (array of event dicts) or already a list/dict
    if isinstance(raw_content, str):
        events = json.loads(raw_content)
    else:
        events = raw_content

    text_parts: list[str] = []
    table_events: list[dict] = []

    for event in (events if isinstance(events, list) else [events]):
        if event.get("event") == "response.text.delta":
            text_parts.append(event.get("data", {}).get("text", ""))
        elif event.get("event") == "response.table":
            table_events.append(event)

    return "".join(text_parts), events, table_events


# ── Session state init ────────────────────────────────────────────────────────

if "memory" not in st.session_state:
    # Instantiate MemoryManager but load facts via session.sql() — env vars are
    # not available in Streamlit in Snowflake, so bypass MemoryManager._connect()
    memory = MemoryManager(user_id=user_id)
    rows   = session.sql(
        f"SELECT fact_key, fact_value "
        f"FROM {_TABLE} "
        f"WHERE user_id = '{user_id}' "
        f"ORDER BY fact_key"
    ).collect()
    memory.facts = {row[0]: row[1] for row in rows}
    st.session_state.memory = memory

if "messages"       not in st.session_state:
    st.session_state.messages       = []
if "pending_key"    not in st.session_state:
    st.session_state.pending_key    = None
if "last_freshness" not in st.session_state:
    st.session_state.last_freshness = None
if "last_filters"   not in st.session_state:
    st.session_state.last_filters   = []
if "debug_events"   not in st.session_state:
    st.session_state.debug_events   = None
if "map_df"         not in st.session_state:
    st.session_state.map_df         = None
if "result_df"        not in st.session_state:
    st.session_state.result_df        = None
if "result_truncated" not in st.session_state:
    st.session_state.result_truncated = False
if "table_events"   not in st.session_state:
    st.session_state.table_events   = []
if "feedbacks"      not in st.session_state:
    st.session_state.feedbacks      = {}  # {message_index: "up" | "down"}
if "session_id"     not in st.session_state:
    st.session_state.session_id     = str(uuid.uuid4())

memory: MemoryManager = st.session_state.memory


# ── Memory helpers using session.sql() ───────────────────────────────────────

def _save_fact(key: str, value: str, source_turn: str = "") -> None:
    """Upsert a fact via session.sql() and sync memory.facts in-place."""
    session.sql(f"""
        MERGE INTO {_TABLE} AS tgt
        USING (SELECT '{user_id}' AS user_id, '{key}' AS fact_key) AS src
            ON tgt.user_id = src.user_id AND tgt.fact_key = src.fact_key
        WHEN MATCHED THEN
            UPDATE SET fact_value  = '{value}',
                       source_turn = '{source_turn}',
                       updated_at  = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (user_id, fact_key, fact_value, source_turn)
            VALUES ('{user_id}', '{key}', '{value}', '{source_turn}')
    """).collect()
    memory.facts[key] = value


def _delete_fact(key: str) -> None:
    """Delete one fact via session.sql() and remove from memory.facts."""
    session.sql(
        f"DELETE FROM {_TABLE} WHERE user_id = '{user_id}' AND fact_key = '{key}'"
    ).collect()
    memory.facts.pop(key, None)


def _clear_all_facts() -> None:
    """Wipe all facts for this user via session.sql()."""
    session.sql(
        f"DELETE FROM {_TABLE} WHERE user_id = '{user_id}'"
    ).collect()
    memory.facts.clear()


def _augment_question(question: str) -> str:
    """Prepend stored user facts to the question as injected context."""
    context = memory.to_system_context()
    if context:
        return f"{context}\n\n---\n\nUser question: {question}"
    return question


_FEEDBACK_TABLE = "RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK"


def _save_feedback(turn_index: int, question: str, answer: str, rating: str) -> None:
    """Persist a thumbs-up/down rating to AGENT_FEEDBACK."""
    def _q(s: str) -> str:
        return s.replace("'", "''")

    session.sql(f"""
        INSERT INTO {_FEEDBACK_TABLE}
            (user_id, session_id, turn_index, question, answer, rating)
        VALUES (
            '{_q(user_id)}',
            '{_q(st.session_state.session_id)}',
            {turn_index},
            '{_q(question)}',
            '{_q(answer)}',
            '{rating}'
        )
    """).collect()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:

    # ── Panel 1: Memory ───────────────────────────────────────────────────────
    st.subheader("🧠 What I know about you")

    if memory.facts:
        for k, v in list(memory.facts.items()):
            col_label, col_val, col_del = st.columns([2, 3, 1])
            col_label.markdown(f"**{k.replace('_', ' ')}**")
            col_val.markdown(v)
            if col_del.button("✕", key=f"del_{k}", help=f"Forget {k}"):
                _delete_fact(k)
                _rerun()

        st.divider()
        if st.button("🗑 Clear all memory", use_container_width=True):
            _clear_all_facts()
            st.session_state.last_freshness = None
            st.session_state.last_filters   = []
            _rerun()
    else:
        st.caption(
            "No stored facts yet.  \n"
            "Ask a location-specific question (e.g. *'Show critical violations "
            "near my home'*) and the agent will ask for your address — then "
            "remember it for future sessions."
        )

    st.divider()

    # ── Panel 2: Data freshness ───────────────────────────────────────────────
    st.subheader("📅 Data freshness")

    if st.session_state.last_freshness:
        st.success(
            f"**Last inspection date in dataset:**  \n"
            f"{st.session_state.last_freshness}"
        )
    else:
        st.caption("Populated after the first query against inspection data.")

    st.divider()

    # ── Panel 3: Filters applied ──────────────────────────────────────────────
    st.subheader("🔍 Filters applied")

    if st.session_state.last_filters:
        for f in st.session_state.last_filters:
            parts = f.split(" — ", 1) if " — " in f else f.split(" -- ", 1)
            if len(parts) == 2:
                st.markdown(f"- **{parts[0].strip()}**  \n  _{parts[1].strip()}_")
            else:
                st.markdown(f"- {f}")
    else:
        st.caption(
            "No implicit filters on last turn. Populated when the agent applies "
            "conditions you did not explicitly request (e.g. default date range, "
            "borough exclusions)."
        )

    st.divider()
    st.caption(
        f"Signed in as **{user_id}**  \n"
        "Authentication managed by Snowflake."
    )


# ══════════════════════════════════════════════════════════════════════════════
# MAIN CHAT AREA
# ══════════════════════════════════════════════════════════════════════════════

st.title("🍽️ NYC Restaurant Intelligence")
st.caption(
    "Ask anything about NYC restaurant inspections or health code regulations. "
    "Powered by Snowflake Cortex Analyst + Cortex Search."
)

# ── Render conversation history ───────────────────────────────────────────────
# st.chat_message requires Streamlit ≥ 1.23 — not available in the SiS runtime.
# Plain markdown with role prefixes is used instead.
for _i, msg in enumerate(st.session_state.messages):
    if msg["role"] == "user":
        st.markdown(f"**🧑 You:** {msg['content']}")
    else:
        st.markdown(f"**🤖 Agent:** {msg['content']}")
        # ── Feedback buttons (only on assistant turns) ────────────────────────
        _fb = st.session_state.feedbacks.get(_i)
        _col_up, _col_down, _col_space = st.columns([1, 1, 10])
        if _col_up.button(
            "👍" if _fb != "up" else "👍 ✓",
            key=f"fb_up_{_i}",
            disabled=_fb is not None,
            help="This answer was helpful",
        ):
            _question = st.session_state.messages[_i - 1]["content"] if _i > 0 else ""
            _save_feedback(_i, _question, msg["content"], "up")
            st.session_state.feedbacks[_i] = "up"
            _rerun()
        if _col_down.button(
            "👎" if _fb != "down" else "👎 ✓",
            key=f"fb_down_{_i}",
            disabled=_fb is not None,
            help="This answer was not helpful",
        ):
            _question = st.session_state.messages[_i - 1]["content"] if _i > 0 else ""
            _save_feedback(_i, _question, msg["content"], "down")
            st.session_state.feedbacks[_i] = "down"
            _rerun()
    st.divider()

# ── Results grid (persists across reruns via session_state) ──────────────────
if st.session_state.get("result_df") is not None:
    import pandas as pd  # noqa: PLC0415
    result_df: pd.DataFrame = st.session_state.result_df
    with st.expander(f"📋 Results table ({len(result_df):,} rows)", expanded=False):
        if st.session_state.get("result_truncated"):
            st.warning(
                "⚠️ Result is truncated — the Snowflake payload limit was reached "
                "and the SQL re-run also failed. Copy the SQL from the debug panel "
                "and run it in a Snowsight worksheet to get all rows.",
                icon="⚠️",
            )
        st.dataframe(result_df, use_container_width=True)
        st.download_button(
            label="📥 Download CSV",
            data=result_df.to_csv(index=False).encode("utf-8"),
            file_name="nyc_inspection_results.csv",
            mime="text/csv",
            use_container_width=True,
        )

# ── Map panel (persists across reruns via session_state) ─────────────────────
if st.session_state.get("map_df") is not None:
    map_df = st.session_state.map_df
    if len(map_df) > 0:
        st.subheader(f"📍 {len(map_df):,} unique restaurant locations with critical violations")
        st.caption(
            "Each point is one restaurant (multiple violation records per restaurant are consolidated). "
            "🟢 Grade A · 🟠 Grade B · 🔴 Grade C · ⚫ Other/pending — hover for details."
        )
        st.pydeck_chart(build_deck(map_df, layer_mode="scatter"), use_container_width=True)
    else:
        st.info("No restaurants with critical violations found for this period.")

# ── Debug panel (persists across reruns via session_state) ────────────────────
if st.session_state.get("debug_events") is not None:
    with st.expander("🐛 Debug: last turn"):
        events_list = st.session_state.debug_events
        if not isinstance(events_list, list):
            st.json(events_list)
        else:
            types = list(dict.fromkeys(e.get("event") for e in events_list if e.get("event")))
            st.write(f"**{len(events_list)} events** — types: {types}")

            # ── SQL queries run by the agent ──────────────────────────────────
            tool_results = [e for e in events_list if e.get("event") == "response.tool_result"]
            if tool_results:
                st.markdown("**SQL generated by Cortex Analyst:**")
                for e in tool_results:
                    data = e.get("data", {})
                    tool_name = data.get("name", "")
                    st.write(f"Tool: `{tool_name}` · status: `{data.get('status','')}`")
                    try:
                        for item in data["content"]:
                            j = item.get("json", {})
                            if j.get("sql"):
                                st.code(j["sql"], language="sql")
                            if j.get("text"):
                                st.caption(j["text"])
                    except Exception:
                        st.json(data)

            # ── Table event summary ───────────────────────────────────────────
            table_evts = [e for e in events_list if e.get("event") == "response.table"]
            st.write(f"**response.table events: {len(table_evts)}**")
            if table_evts:
                rs = table_evts[0].get("data", {}).get("result_set", {})
                num_rows = rs.get("resultSetMetaData", {}).get("numRows", "?")
                cols = [c["name"] for c in rs.get("resultSetMetaData", {}).get("rowType", [])]
                st.write(f"numRows: {num_rows} — columns: {cols}")

# ── Chat input ────────────────────────────────────────────────────────────────
# st.chat_input requires Streamlit ≥ 1.23 — replaced with a form so the input
# field clears automatically after submission (clear_on_submit=True).
with st.form("chat_form", clear_on_submit=True):
    col_input, col_btn = st.columns([8, 1])
    prompt = col_input.text_input(
        "question",
        placeholder="Ask about NYC restaurant inspections…",
        label_visibility="collapsed",
    )
    submitted = col_btn.form_submit_button("Send")

if submitted and prompt:

    # ── Capture pending memory key if previous turn requested personal info ───
    if st.session_state.pending_key:
        _save_fact(
            key         = st.session_state.pending_key,
            value       = prompt,
            source_turn = prompt,
        )
        st.session_state.pending_key = None
        # Don't rerun here — fall through so the agent sees the full conversation
        # history (original question + its own request + this answer) and responds.

    # ── Add user message to history ───────────────────────────────────────────
    st.session_state.messages.append({"role": "user", "content": prompt})

    # ── Build history as Cortex Agent message dicts ───────────────────────────
    history: list[dict] = [
        {"role": m["role"], "content": [{"type": "text", "text": m["content"]}]}
        for m in st.session_state.messages[:-1]
    ]

    # ── Call agent ────────────────────────────────────────────────────────────
    response_placeholder = st.empty()
    full_response = ""

    try:
        with st.spinner("Thinking…"):
            full_response, _raw_events, _table_events = _call_agent_sis(
                question             = _augment_question(prompt),
                conversation_history = history,
            )
        st.session_state.debug_events  = _raw_events
        st.session_state.table_events  = _table_events
        response_placeholder.markdown(f"**🤖 Agent:** {full_response}")

    except RuntimeError as exc:
        full_response = f"⚠️ Agent error: {exc}"
        response_placeholder.error(full_response)

    # ── Parse metadata out of the response ───────────────────────────────────
    parsed = parse_response(full_response)

    if parsed.freshness_date:
        st.session_state.last_freshness = parsed.freshness_date
    st.session_state.last_filters = parsed.filters

    # ── Render final body ─────────────────────────────────────────────────────
    response_placeholder.markdown(
        f"**🤖 Agent:** {parsed.body or full_response}"
    )

    st.session_state.messages.append(
        {"role": "assistant", "content": parsed.body or full_response}
    )

    # ── Result grid: re-run agent SQL to bypass SSE payload truncation ────────
    # The table event only carries ~128 rows. Running the SQL ourselves via
    # session.sql() fetches the complete result set from Snowflake.
    _agent_sql = _extract_agent_sql(_raw_events)
    _expected_rows = next(
        (evt["data"]["result_set"]["resultSetMetaData"]["numRows"]
         for evt in _table_events
         if evt.get("data", {}).get("result_set", {}).get("resultSetMetaData")),
        None,
    )
    _result_truncated = False
    if _agent_sql:
        try:
            st.session_state.result_df = session.sql(_agent_sql).to_pandas()
        except Exception:
            st.session_state.result_df = _build_result_df(_table_events)
            _result_truncated = True
    else:
        st.session_state.result_df = _build_result_df(_table_events)
        _result_truncated = _expected_rows is not None and (
            st.session_state.result_df is None
            or len(st.session_state.result_df) < _expected_rows
        )
    st.session_state.result_truncated = _result_truncated

    # ── Map: load geo data when question has spatial intent ───────────────────
    if _is_geo_question(prompt):
        with st.spinner("Loading map data…"):
            map_df = None

            # Priority 1: table already has lat/lng (ideal — use agent's coords directly)
            map_df = _build_map_df_from_table(_table_events)

            # Priority 2: extract home coords from agent SQL → exact geo-filtered query
            if map_df is None:
                agent_sql = _extract_agent_sql(_raw_events)
                if agent_sql:
                    coords = _extract_home_coords(agent_sql)
                    if coords:
                        map_df = _load_map_data_from_geo(session, coords[0], coords[1])

            # Priority 3: name+borough filter via SEMANTIC_VIEW (imprecise fallback)
            if map_df is None:
                pairs = _extract_name_borough_pairs(_table_events)
                map_df = (
                    _load_map_data_from_pairs(session, pairs)
                    if pairs
                    else _load_map_data(session, critical_only=True)
                )

            st.session_state.map_df = map_df
    else:
        st.session_state.map_df = None

    # ── Check whether the agent requested personal info ───────────────────────
    extraction = memory.check_for_memory_request(parsed.body or full_response)
    if extraction:
        st.session_state.pending_key = extraction["key"]
        st.caption("💾 *I'll remember your answer to use in future sessions.*")

    _rerun()
