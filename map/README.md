# Map Explorer — NYC Restaurant Inspections

Interactive pydeck map of NYC restaurant inspection grades, deployed as a Streamlit in Snowflake app.

**All queries go through `SEMANTIC_VIEW(nyc_restaurant_inspections)` — the same governed layer the Cortex Agent uses. The map produces identical numbers to the agent for the same question.**

---

## Architecture

```
streamlit_map_app.py
        │
        ▼  session.sql("SELECT ... FROM SEMANTIC_VIEW(...)")
SEMANTIC_VIEW(RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections)
        │
        ├── restaurants   → DIM_RESTAURANT  (latitude, longitude, borough, cuisine_type)
        ├── inspections   → FCT_INSPECTIONS (grade, inspection_score, has_critical_violation)
        └── dates         → DIM_DATE        (calendar_date — used for date range filtering)
        │
        ▼
map_engine.py  ← receives a plain DataFrame, knows nothing about the source
        │
        ▼
st.pydeck_chart()
```

`map_engine.py` has no Streamlit imports at the top level and no knowledge of the semantic view — it accepts any DataFrame with the right column names and returns a `pydeck.Deck`. It can be reused by the Phase 5 agent Streamlit app without modification.

---

## Semantic view column mapping

All column names in `streamlit_map_app.py` use semantic names, not underlying table column names.

| Semantic name | Table | Underlying expr | Clause in SEMANTIC_VIEW() |
|---|---|---|---|
| `restaurant_name` | restaurants | `RESTAURANT_NAME` | DIMENSIONS |
| `borough` | restaurants | `BOROUGH` | DIMENSIONS |
| `cuisine_type` | restaurants | `CUISINE_DESCRIPTION` | DIMENSIONS |
| `grade` | inspections | `GRADE` | DIMENSIONS |
| `inspection_date` | inspections | `INSPECTION_DATE` | DIMENSIONS |
| `has_critical_violation` | inspections | `HAS_CRITICAL_VIOLATION` | DIMENSIONS |
| `latitude` | restaurants | `LATITUDE` | **FACTS** |
| `longitude` | restaurants | `LONGITUDE` | **FACTS** |
| `average_score` | inspections | `AVG(INSPECTION_SCORE)` | METRICS |
| `total_inspections` | inspections | `COUNT(FCT_INSPECTION_KEY)` | METRICS |
| `total_restaurants` | restaurants | `COUNT(DISTINCT RESTAURANT_ID)` | METRICS |

`latitude` and `longitude` are declared as **facts** in the semantic view YAML, not dimensions. They must appear in the FACTS clause of every SEMANTIC_VIEW() call that needs coordinates.

---

## Named filters used vs. available

The semantic view declares several named filters. This table documents which are used and why.

| Named filter | Table | Expression | Used in map app? |
|---|---|---|---|
| `graded_inspections` | inspections | `GRADE IN ('A', 'B', 'C')` | No — grade filter is user-controlled via multiselect; applying this named filter would silently hide Z/P/G/N grades |
| `recent_12_months` | inspections | `INSPECTION_DATE >= DATEADD(month,-12,CURRENT_DATE())` | No — date range is user-controlled via date pickers; the default matches this filter but can be changed |
| `initial_inspections` | inspections | `INSPECTION_TYPE ILIKE '%Initial Inspection%'` | No — map shows all inspection types |
| `re_inspections` | inspections | `INSPECTION_TYPE ILIKE '%Re-Inspection%'` | No |
| `closures` | inspections | `ACTION ILIKE '%Establishment Closed%'` | No — not a map filter |
| `clean_inspections` | inspections | `ACTION ILIKE '%No violations%'` | No |
| `critical_only` | **violations** | `IS_CRITICAL_VIOLATION = TRUE` | No — this filter is on the violations table. For the map's inspection-level filter, `has_critical_violation` (a BOOLEAN dimension on the inspections table) is used instead |
| `non_critical_only` | violations | `IS_CRITICAL_VIOLATION = FALSE` | No |

**When to use named filters instead of WHERE clauses:** Pass `FILTER graded_inspections` to any SEMANTIC_VIEW() call that should only count final graded inspections (A, B, C) — for example, a grade-A pass rate chart where pending and ungraded entries would skew the denominator. The map app does not apply this filter by default so all grade values are plottable.

---

## SEMANTIC_VIEW() constraints

Three constraints apply when combining FACTS, DIMENSIONS, and METRICS:

| # | Error | Rule |
|---|---|---|
| 1 | `Facts and metrics cannot be requested in the same query` | FACTS and METRICS cannot coexist in a single `SEMANTIC_VIEW()` call |
| 2 | `All expressions referenced in the query must come from the same entity when both FACTS and DIMENSIONS are specified` | When FACTS are present, every DIMENSION must belong to the same entity as the facts |
| 3 | `Ambiguous semantic expression 'INSPECTION_DATE'` | Dimension names shared across entities must be qualified: `inspections.inspection_date` |

The map app resolves all three with a **three-CTE pattern**:

| CTE | Clauses | Entity constraint | Purpose |
|---|---|---|---|
| `sv_coords` | DIMENSIONS(restaurant_name) + **FACTS**(lat, lon) | `restaurants` only — constraint #2 | Coordinates, one row per restaurant |
| `sv_insp` | DIMENSIONS(…) + **METRICS**(…), no FACTS | entities can mix freely | Inspection dims + aggregates, date/borough/cuisine filtered |
| `per_restaurant` | JOIN on `restaurant_name`, GROUP BY | — | One row per restaurant with latest grade |

---

## What is NOT in the semantic view

The following logic is needed by the map but is not declared as metrics or filters in the semantic view. It is implemented in outer SQL wrapping the SEMANTIC_VIEW() result — never as raw joins against MARTS tables.

| Requirement | Reason not in semantic view | How the map handles it |
|---|---|---|
| Latest grade per restaurant | No `latest_grade` metric exists — semantic view metrics are aggregations (AVG, COUNT), not point-in-time lookups | `MAX_BY(grade, inspection_date)` in the outer `per_restaurant` CTE that wraps the SEMANTIC_VIEW() result |
| One row per restaurant | SEMANTIC_VIEW() returns one row per unique dimension combination; with `grade` and `inspection_date` as dimensions, multiple rows exist per restaurant | Outer GROUP BY on `(restaurant_name, borough, cuisine_type)` with MAX_BY collapses to one row |
| any_critical flag | No per-restaurant "ever had a critical violation" metric | `MAX(has_critical_violation::INT)::BOOLEAN` in the outer CTE |
| Coordinate NULL filtering | Semantic view does not define a filter for restaurants with coordinates | `WHERE latitude IS NOT NULL AND longitude IS NOT NULL` in the outer CTE, after SEMANTIC_VIEW() returns aggregated facts |

All of the above are computed in SQL that wraps the SEMANTIC_VIEW() call — the semantic view itself is always the innermost data source.

---

## Files

```
map/
├── README.md                  ← you are here
├── 01_map_setup.sql           ← semantic view + table grants, stage creation
├── map_engine.py              ← standalone pydeck builder (no Streamlit imports)
├── streamlit_map_app.py       ← Streamlit in Snowflake app
└── requirements.txt           ← pydeck>=0.9.0
```

---

## Setup steps

### Step 1 — Prerequisites

Phases 1–2 must be complete:
- `setup/01_snowflake_setup.sql` executed (warehouse, database, role)
- dbt run completed (`dim_restaurant`, `fct_inspections` populated in MARTS)
- `semantic/02_deploy_semantic.sql` executed (`nyc_restaurant_inspections` semantic view created)

### Step 2 — Run the grants SQL

Open a Snowflake worksheet as **ACCOUNTADMIN** and run `map/01_map_setup.sql`.

This grants `SELECT ON SEMANTIC VIEW` and `SELECT ON ALL TABLES IN MARTS` to `RESTAURANT_LOADER`, creates the stage, and runs two smoke tests:

```sql
-- Borough counts via semantic view (confirms grants are working)
SELECT * FROM SEMANTIC_VIEW(
    RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
    DIMENSIONS borough
    METRICS total_restaurants
)
WHERE borough IS NOT NULL AND borough != '0'
ORDER BY borough;
```

Expected output — 5 rows:
```
BOROUGH       TOTAL_RESTAURANTS
Bronx         ~2,400
Brooklyn      ~7,200
Manhattan     ~8,100
Queens        ~6,500
Staten Island ~1,100
```

If `REFERENCES ON SEMANTIC VIEW` is missing, `DESCRIBE SEMANTIC VIEW` will fail but queries still work. Include it for lineage tooling.

### Step 3 — Install pydeck (local testing only)

The SiS runtime provides `streamlit` and `snowflake-snowpark-python`. For local testing of `map_engine.py`:

```bash
cd cortex && source .venv/bin/activate
pip install -r ../map/requirements.txt
```

### Step 4 — Upload app files to the stage

`snowsql` is not required. Use the upload script (key-pair auth, no MFA prompt):

```bash
cd <repo-root>
source cortex/.venv/bin/activate
python map/upload_to_stage.py
```

Expected output:
```
  PUT   streamlit_map_app.py … UPLOADED
  PUT   map_engine.py … UPLOADED
  PUT   environment.yml … UPLOADED

Stage contents (3 file(s)):
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE/environment.yml
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE/map_engine.py
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE/streamlit_map_app.py
```

Verify independently:
```sql
LIST @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE;
-- Expected: 3 rows — streamlit_map_app.py, map_engine.py, environment.yml
```

### Step 5 — Create the Streamlit app

```sql
CREATE OR REPLACE STREAMLIT RESTAURANT_INTELLIGENCE.RAW.NYC_RESTAURANT_MAP
    ROOT_LOCATION = '@RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE'
    MAIN_FILE     = 'streamlit_map_app.py'
    QUERY_WAREHOUSE = 'RESTAURANT_WH'
    COMMENT = 'NYC Restaurant Inspection Map';

GRANT USAGE ON STREAMLIT RESTAURANT_INTELLIGENCE.RAW.NYC_RESTAURANT_MAP
    TO ROLE RESTAURANT_LOADER;
```

---

## Expected output

```
Sidebar                          Main panel
──────────────────────────────   ─────────────────────────────────────────────
Borough         [multiselect]    Title + caption
Grade           [multiselect]    KPI strip: restaurants / graded / A% / avg score
Cuisine type    [multiselect]    Grade legend (coloured dots + labels)
Date range      [from] [to]      ──────────────────────────────────────────────
Critical only   [checkbox]       pydeck scatter map, colour-coded by latest grade
Layer mode      scatter/heatmap  ──────────────────────────────────────────────
                                 Summary table (500-row cap, sortable)
```

**Functional tests:**

1. Default load: map shows ~20,000+ points, mostly green (Grade A).
2. Grade C + Brooklyn filter: points turn red and cluster in Brooklyn.
3. Heatmap mode: map redraws as density heatmap; tooltip absent (expected — pydeck HeatmapLayer is not pickable).
4. Critical violations only: restaurant count drops; summary "Inspections" count reflects all inspections for those restaurants, not just critical ones.
5. Narrow date to 30 days: KPI strip and summary update.

**Consistency check with the agent:**

Ask the Cortex Agent: *"How many restaurants are in Manhattan?"*

Then check the map's KPI strip with Borough = Manhattan only (all grades, last 12 months). The agent uses `total_restaurants` metric; the map uses `COUNT(restaurant_name)` on the SEMANTIC_VIEW() result. Both should return the same count because both query through the same semantic view.

---

## map_engine.py — reuse in the Phase 5 agent app

`map_engine.py` has no Streamlit imports at the top level. To use it in the agent Streamlit app (`memory/streamlit_app.py`):

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "map"))
from map_engine import build_deck

# df must have: LATITUDE, LONGITUDE, DBA, GRADE, SCORE, BOROUGH
# Add CUISINE for tooltip (optional)
deck = build_deck(df, layer_mode="scatter")
st.pydeck_chart(deck, use_container_width=True)
```

Upload `map_engine.py` to the same SiS stage as the agent app files alongside `memory_manager.py`, `response_parser.py`, and `streamlit_app.py`.

---

## Troubleshooting

**`Insufficient privileges to operate on semantic view`**

`GRANT SELECT ON SEMANTIC VIEW ... TO ROLE RESTAURANT_LOADER` was not run. Re-run `01_map_setup.sql` as ACCOUNTADMIN.

**`Insufficient privileges to operate on table DIM_RESTAURANT`**

The semantic view SELECT grant is present but the underlying table grant is missing. Run:
```sql
GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS
    TO ROLE RESTAURANT_LOADER;
```

**All points are grey on the map**

The `GRADE` column is arriving as `None` or `''`. The date range may not overlap with any graded inspections (`grade IS NOT NULL` — grades are only posted after the full inspection cycle completes). Broaden the date range to 24 months.

**`ModuleNotFoundError: No module named 'map_engine'`**

`map_engine.py` was not uploaded to the stage. Run `LIST @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE;` and verify both files appear. Re-upload if missing.

**`pydeck` import error in SiS**

In Snowsight: open the app editor → **Packages** tab → search `pydeck` → select `0.9.x` → save. The SiS runtime installs it from the Snowflake conda channel.

**`pct_with_coords` is 0%**

The Socrata ingestion omitted coordinate fields. Verify:
```sql
SELECT latitude, longitude
FROM RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW
LIMIT 10;
```
If empty, re-run ingestion with coordinates included.

**Borough filter shows `0` as an option**

`load_boroughs()` filters `borough != '0'` explicitly (per the semantic view YAML's `sql_generation` instruction: *"Always exclude borough = '0' from borough breakdowns"*). If `0` still appears, the SEMANTIC_VIEW() query cache is stale — clear it with `st.cache_data.clear()` or wait for the 3600s TTL.
