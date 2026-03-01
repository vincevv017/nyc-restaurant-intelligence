# NYC Restaurant Intelligence

A reproducible data pipeline built on Snowflake that ingests the full NYC Department of Health restaurant inspection dataset and models it into a star schema ready for Snowflake Intelligence (AI/BI) analysis.

This repository is the technical foundation for the **Snowflake Intelligence** article series demonstrating context-aware AI/BI using progressive context loading.

**Article series:** [LinkedIn — Vincent Vikor](https://www.linkedin.com/in/vincevkor/)  
**Dataset:** [NYC Restaurant Inspection Results](https://data.cityofnewyork.us/Health/NYC-Restaurant-Inspection-Results/gv23-aida/about_data) — ~250k rows, updated daily by NYC DOHMH

---

## Architecture

```
Socrata API (NYC Open Data)
        │
        │  HTTP/JSON, paginated (10k rows/request)
        ▼
ingestion/load_inspections.py
        │
        │  snowflake-connector-python, TRUNCATE + bulk INSERT
        ▼
RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW
        │
        │  dbt Core transformations
        ▼
RESTAURANT_INTELLIGENCE.STAGING
  ├── stg_inspections      ← Typed, cleaned, full grain
  ├── stg_restaurants      ← One row per restaurant (CAMIS)
  └── stg_violations       ← Violation code reference

        │
        │  dbt MARTS models
        ▼
RESTAURANT_INTELLIGENCE.MARTS
  ├── dim_restaurant        ← Restaurant dimension
  ├── dim_violation_type    ← Violation reference dimension
  ├── dim_date              ← Date spine (required for Snowflake Intelligence)
  ├── fct_inspections       ← Inspection events (aggregated)
  └── fct_violations        ← Individual violations cited (granular)
```

**Load strategy:** Full TRUNCATE + reload on every pipeline run. No incremental logic — keeps the demo simple and idempotent. Typical full load timing: ~110s fetching from Socrata (network-bound) + ~17s loading to Snowflake via internal stage = ~127s total.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | ≥ 3.11 | Ingestion script |
| dbt Core | ≥ 1.8 | SQL transformations |
| Snowflake account | Trial or paid | Target database |

A free [Snowflake trial account](https://signup.snowflake.com/) (30 days / $400 credits) is sufficient for this entire project.

---

## Setup — Step by Step

### Step 1 — Clone the repository

```bash
git clone https://github.com/vincevv017/nyc-restaurant-intelligence.git
cd nyc-restaurant-intelligence
```

### Step 2 — Create your environment file

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake credentials:

```dotenv
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=RESTAURANT_WH
SNOWFLAKE_DATABASE=RESTAURANT_INTELLIGENCE
SNOWFLAKE_ROLE=RESTAURANT_LOADER
NYC_APP_TOKEN=your_app_token   # optional but recommended
```

**Finding your account identifier:**

Look at your browser URL when logged into Snowflake — use everything before `.snowflakecomputing.com`:

| Region | Format example |
|--------|---------------|
| AWS us-east-1 | `abc12345` |
| AWS eu-west-1 (Ireland) | `abc12345.eu-west-1.aws` |
| AWS eu-west-3 (Paris) | `abc12345.eu-west-3.aws` |
| Azure West Europe | `abc12345.west-europe.azure` |

> ⚠️ For any region outside AWS us-east-1, you **must** append the region suffix. A 404 error on connection always means a wrong account identifier format.

**Getting a free NYC Open Data app token:**  
Register at [data.cityofnewyork.us](https://data.cityofnewyork.us) → Developer Settings → Create New App Token. Without a token, requests are rate-limited to 1,000 rows/request instead of 1,000,000.

### Step 3 — Run the Snowflake setup script

Open a Snowflake worksheet logged in as **ACCOUNTADMIN**, paste and run `setup/01_snowflake_setup.sql`.

Before running, replace `YOUR_USERNAME` on the last line with your actual Snowflake username.

This creates:
- `RESTAURANT_WH` — XS warehouse, auto-suspends after 60s
- `RESTAURANT_INTELLIGENCE` database with RAW / STAGING / MARTS schemas
- `RESTAURANT_LOADER` role with all required permissions including `CREATE SCHEMA`
- `RAW.INSPECTIONS_RAW` table

### Step 4 — Install Python ingestion dependencies

The ingestion script gets its own virtual environment, kept separate from dbt to avoid dependency conflicts.

```bash
cd ingestion
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 5 — Validate ingestion with a test load

```bash
python load_inspections.py --limit 1000
```

Expected output:
```
NYC Restaurant Intelligence — Data Loader
Started: 2026-03-01 08:47:43 UTC  |  Row limit: 1,000

✅ Fetched 1,000 total records from Socrata
✅ Inserted 1,000 rows into RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW
   Row count confirmed: 1,000
🏁 Pipeline complete in 4.5s
```

### Step 6 — Run the full load

```bash
python load_inspections.py
```

Fetches the complete dataset (~296k rows). Expect ~2 minutes total (~110s Socrata fetch + ~17s Snowflake load).

### Step 7 — Set up dbt

dbt gets its own virtual environment at the **project root** — separate from the ingestion venv.

```bash
cd ..                              # back to project root
python -m venv .venv-dbt
source .venv-dbt/bin/activate      # Windows: .venv-dbt\Scripts\activate
pip install dbt-snowflake
dbt --version                      # verify: shows dbt-core and dbt-snowflake
```

### Step 8 — Configure the dbt profile

dbt credentials live in your home directory, never inside the project.

```bash
mkdir -p ~/.dbt
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
nano ~/.dbt/profiles.yml           # fill in your credentials
```

```yaml
nyc_restaurant_intelligence:
  target: dev
  outputs:
    dev:
      type:      snowflake
      account:   abc12345.eu-west-3.aws   # same format as your .env
      user:      your_username
      password:  your_password
      role:      RESTAURANT_LOADER
      database:  RESTAURANT_INTELLIGENCE
      warehouse: RESTAURANT_WH
      schema:    STAGING
      threads:   4
```

### Step 9 — Validate the dbt connection

```bash
cd dbt
dbt debug
```

All checks must pass before proceeding:
```
profiles.yml file   [OK found and valid]
dbt_project.yml     [OK found and valid]
Connection test:    [OK connection ok]
All checks passed!
```

### Step 10 — Run dbt transformations

```bash
dbt run
```

Expected — 8 models in ~3 seconds:
```
Found 8 models, 1 source, 523 macros
Concurrency: 4 threads

1 of 8 START sql table model MARTS.dim_date .................. [RUN]
2 of 8 START sql view model STAGING.stg_inspections .......... [RUN]
...
Done. PASS=8 WARN=0 ERROR=0 SKIP=0 TOTAL=8
```

### Step 11 — Verify in Snowflake

```sql
SELECT 'RAW'             AS layer, COUNT(*) AS rows FROM RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW
UNION ALL
SELECT 'STG_INSPECTIONS',          COUNT(*) FROM RESTAURANT_INTELLIGENCE.STAGING.STG_INSPECTIONS
UNION ALL
SELECT 'DIM_RESTAURANT',           COUNT(*) FROM RESTAURANT_INTELLIGENCE.MARTS.DIM_RESTAURANT
UNION ALL
SELECT 'FCT_INSPECTIONS',          COUNT(*) FROM RESTAURANT_INTELLIGENCE.MARTS.FCT_INSPECTIONS
UNION ALL
SELECT 'FCT_VIOLATIONS',           COUNT(*) FROM RESTAURANT_INTELLIGENCE.MARTS.FCT_VIOLATIONS;
```

---

## Pipeline Commands Reference

```bash
# ── Ingestion (from ingestion/ with ingestion venv active) ───────────────────
python load_inspections.py                  # Full load (~250k rows)
python load_inspections.py --limit 1000     # Test load
python load_inspections.py --dry-run        # Fetch only, no Snowflake writes

# ── dbt (from dbt/ with dbt venv active) ────────────────────────────────────
dbt debug                                   # Validate connection first
dbt run                                     # Build all models
dbt run --select staging                    # Staging layer only
dbt run --select marts                      # Marts layer only
dbt test                                    # Run data quality tests
dbt docs generate && dbt docs serve         # Interactive lineage documentation
```

---

## Data Model Reference

### Fact Tables

| Table | Grain | Key Measures |
|-------|-------|-------------|
| `fct_inspections` | 1 row per restaurant × inspection date × inspection type | `inspection_score`, `total_violations`, `critical_violations` |
| `fct_violations`  | 1 row per violation cited per inspection | `violation_count` (always 1, additive) |

### Dimension Tables

| Table | Grain | Notes |
|-------|-------|-------|
| `dim_restaurant`     | 1 row per CAMIS | Latest known name, address, cuisine |
| `dim_violation_type` | 1 row per violation code | Critical flag, description |
| `dim_date`           | 1 row per calendar day | 2000-01-01 to 2030-12-31. Mandatory for Snowflake Intelligence time-intelligence queries |

### Grading Logic (NYC DOHMH)

| Grade | Score Range | Meaning |
|-------|-------------|---------|
| A | 0–13 | Passes — minimal violations |
| B | 14–27 | Passes — significant violations |
| C | 28+ | Fails — conditional operating permit |
| Z | — | Grade pending (re-inspection scheduled) |
| P | — | Pre-permit (new establishment) |

Higher score = more violations = worse outcome.

---

## Repository Structure

```
nyc-restaurant-intelligence/
├── .env.example                          ← Credential template (copy to .env)
├── .gitignore
├── README.md
│
├── setup/
│   └── 01_snowflake_setup.sql            ← Run once as ACCOUNTADMIN
│
├── ingestion/
│   ├── .venv/                            ← Ingestion venv (gitignored)
│   ├── requirements.txt
│   ├── config.py                         ← Reads from .env
│   └── load_inspections.py               ← Socrata → Snowflake loader
│
├── .venv-dbt/                            ← dbt venv (gitignored)
│
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml.example              ← Copy to ~/.dbt/profiles.yml
    ├── macros/
    │   └── generate_schema_name.sql      ← Prevents STAGING_STAGING / MARTS_MARTS naming
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── stg_inspections.sql
        │   ├── stg_restaurants.sql
        │   └── stg_violations.sql
        └── marts/
            ├── dim_date.sql
            ├── dim_restaurant.sql
            ├── dim_violation_type.sql
            ├── fct_inspections.sql
            └── fct_violations.sql
```

---

## Troubleshooting

**`EnvironmentError: Missing required environment variable: SNOWFLAKE_ACCOUNT`**  
→ `.env` is missing or not in the project root. Run `ls -la` at the root to verify.

**`404 Not Found` on connection**  
→ Wrong account identifier format. For any AWS region outside us-east-1, the region suffix is required (e.g. `abc12345.eu-west-3.aws`). Check your browser URL when logged into Snowflake.

**`250001: Incorrect username or password`**  
→ Password is wrong or the role hasn't been granted to your user. Verify by logging into the Snowflake UI directly. Re-run the setup script if needed.

**`dbt_project.yml file [ERROR not found]`**  
→ Run `dbt debug` from inside the `dbt/` subfolder, not the project root.

**`Nothing to do` on `dbt run`**  
→ The SQL model files don't exist yet. Run `find . -name "*.sql" | sort` inside `dbt/` — you should see 8 files.

**`Insufficient privileges to operate on database`**  
→ The `RESTAURANT_LOADER` role is missing `CREATE SCHEMA` on the database. This is included in the updated setup script — re-run `setup/01_snowflake_setup.sql` as ACCOUNTADMIN.

**Models land in `STAGING_STAGING` or `MARTS_MARTS` instead of `STAGING` / `MARTS`**  
→ The `generate_schema_name` macro is missing. Create `dbt/macros/generate_schema_name.sql` — see the file in this repo.

**Rate limiting from Socrata (HTTP 429)**  
→ Add `NYC_APP_TOKEN` to your `.env`. Free registration at [data.cityofnewyork.us](https://data.cityofnewyork.us).

---

## Design Notes

**Two separate virtual environments** — `ingestion/.venv` and `.venv-dbt` at the project root. This avoids version conflicts between `snowflake-connector-python` pulled in separately by the ingestion script and by `dbt-snowflake`.

**`macros/generate_schema_name.sql`** — dbt's default behaviour appends the custom schema name to the profile's target schema, producing `STAGING_STAGING` and `MARTS_MARTS`. This macro overrides that so models land in exactly `STAGING` and `MARTS`.

**Why no Snowflake Tasks or Streams?** Deliberately excluded. The article's focus is Snowflake Intelligence, not pipeline orchestration. Manual on-demand execution keeps the reproduction path as simple as possible.

**Production pattern:** In a team environment, replace the manual worksheet step with Snowflake's native Git integration (`CREATE GIT REPOSITORY`), allowing `EXECUTE IMMEDIATE FROM @repo/setup/01_snowflake_setup.sql` directly. For a solo demo on a trial account, the manual approach has fewer prerequisites.

---

## What's Next — Phase 2

Phase 2 adds Snowflake Intelligence (Cortex Agent) on top of this pipeline:

1. **Snowflake Semantic Views** — native semantic layer over the star schema
2. **Progressive context loading** — how AI responses improve as context is added
3. **Conversation memory** — agents that learn from user corrections
4. **Natural language queries** — "Which cuisines have the most critical violations in Brooklyn this year?"

---

## License

MIT — use freely, attribution appreciated.

---

*Built by [Vincent Vikor](https://www.linkedin.com/in/vincevkor/) | Solutions Architect*

---

## Data Quality Notes

These issues were discovered during pipeline validation against the full 296k row dataset. All are handled in `stg_inspections.sql` — documented here so future maintainers understand the source data quirks.

### 1. Duplicate records from Socrata API

The Socrata API returns genuine duplicate rows for some violation records — every column identical including `loaded_at`. Root cause is unknown (likely a source system issue on NYC DOHMH's side). Fixed by deduplication in `stg_inspections` using `QUALIFY ROW_NUMBER()`.

### 2. Score adjudication updates create variant rows

The same violation for the same inspection can appear multiple times with **different scores** — for example SWAY LOUNGE (CAMIS 40911114) on 2017-11-04 has violation `04M` with score 15 AND score 20. This is the adjudication update pattern: the score was revised after the initial citation and both versions persist in Socrata.

Fix: partition on the business key (`restaurant_id`, `inspection_date`, `inspection_type`, `action`, `violation_code`, `is_critical_violation`, `record_date`) and keep `loaded_at DESC` — the most recently ingested record, which reflects the latest adjudicated score.

### 3. Grade 'N' (Not Yet Graded)

NYC DOHMH documentation lists grades A/B/C/Z/P but the actual data also contains `N` (Not Yet Graded) for 2,176 inspection records. Added to accepted values in `schema.yml`.

### 4. New establishments with null inspection_type

Restaurants that have applied for a permit but not yet been inspected appear in the dataset with `inspection_date = 1900-01-01` and `inspection_type IS NULL`. These 3,267 records are filtered out in `fct_inspections` and `fct_violations` since they represent administrative registrations, not inspection events.

### 5. Action field required in surrogate keys

The grain `restaurant × inspection_date × inspection_type` is not unique — the same inspection can have multiple `action` values (e.g. "Violations were cited" vs "Establishment Closed"). The `action` field was added to both fact table surrogate keys to ensure uniqueness.

### Deduplication logic in stg_inspections

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY restaurant_id, inspection_date, inspection_type, action,
                 violation_code, is_critical_violation, record_date
    ORDER BY loaded_at DESC, inspection_score DESC
) = 1
```

Keeps the most recently loaded record per business key, with `inspection_score DESC` as tiebreaker to retain the most conservative (highest) adjudicated score when `loaded_at` is identical.