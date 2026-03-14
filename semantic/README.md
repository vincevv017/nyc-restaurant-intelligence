# semantic/ — Snowflake Semantic View (Phase 2)

This folder contains the semantic view that powers Cortex Analyst — Snowflake's text-to-SQL engine. The YAML encodes domain knowledge the LLM cannot infer from column names alone: what "Grade A pass rate" means, how inspection scores work, which filters apply by default, and how tables join.

**Prerequisite:** Phase 1 complete. Run the verification query in the root README before starting.

---

## Files

| File | Purpose |
|------|---------|
| `nyc_restaurant_inspections.yaml` | Authoritative semantic view — 1,365-line YAML defining all logical tables, dimensions, metrics, filters, and verified queries |
| `02_deploy_semantic.sql` | SQL wrapper for deployment — grants, dry-run validation, creation, and verification commands |

---

## Step 1 — Grant Privileges (ACCOUNTADMIN)

Run as ACCOUNTADMIN. These grants are not included in the Phase 1 setup script because semantic views and Cortex are Phase 2 features.

```sql
-- Semantic view creation
GRANT CREATE SEMANTIC VIEW ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS
  TO ROLE RESTAURANT_LOADER;

-- Cortex Analyst access
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER
  TO ROLE RESTAURANT_LOADER;

-- REQUIRED for EU accounts — data stays in your region, only LLM inference routes cross-region
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

Verify grants took effect:

```sql
SHOW GRANTS TO ROLE RESTAURANT_LOADER;
-- Look for: CREATE SEMANTIC VIEW on RESTAURANT_INTELLIGENCE.MARTS
```

---

## Step 2 — (Optional) Generate SVA Scaffolding

Snowflake's Semantic View Autopilot (SVA) can generate a baseline YAML directly from your DDL — saving the boilerplate of defining every logical table, column, and join from scratch. The output requires expert curation, but it is the fastest starting point for a complex schema.

**How to generate:**

1. Navigate to **Snowsight → AI & ML → Cortex Analyst**
2. Click **Create Semantic View**
3. Select database `RESTAURANT_INTELLIGENCE`, schema `MARTS`
4. Select all five tables: `DIM_RESTAURANT`, `DIM_VIOLATION_TYPE`, `DIM_DATE`, `FCT_INSPECTIONS`, `FCT_VIOLATIONS`
5. Click **Generate** — SVA analyzes the DDL, infers relationships, and produces a YAML scaffold
6. Export what was created:

```sql
SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW(
  'RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS'
);
```

Save the output as your starting point for enrichment.

**What SVA produces:** column definitions, inferred joins, basic dimensions and metrics derived from column names and data types.

**What SVA cannot produce:** metric business definitions (`description` fields explaining what a metric actually means), `sample_values` for filter guidance, `custom_instructions` for score directionality and time defaults, or `verified_queries` for common question patterns. The gap between the scaffold and the 1,365-line YAML in this repository is exactly this enrichment layer — and it is what separates correct answers from plausible-sounding wrong ones.

**If skipping SVA:** Go directly to Step 3 using the authoritative YAML from the repository, then proceed to Step 4 to deploy.

---

## Step 3 — Validate the YAML (Dry Run)

Always run the dry run before deploying. It catches YAML parsing errors without touching anything in Snowflake.

```sql
USE ROLE RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;

-- Third parameter TRUE = dry run (validates only, creates nothing)
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$ <paste entire contents of nyc_restaurant_inspections.yaml here> $$,
  TRUE
);
```

Expected output: `Semantic view validation successful.`

> ⚠️ The YAML is ~1,365 lines. Paste it between the `$$` delimiters in Snowsight. Watch for invisible characters introduced by text editors — they cause silent truncation.

---

## Step 4 — Deploy the Semantic View

Pass `FALSE` as the third parameter to create or replace the semantic view:

```sql
-- Third parameter FALSE = create (or replace if it already exists)
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$ <paste entire contents of nyc_restaurant_inspections.yaml here> $$,
  FALSE
);
```

---

## Step 5 — Verify Deployment

```sql
-- Confirm the object exists
DESCRIBE SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS;

-- Export what Snowflake actually stored — compare against your source YAML
SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW(
  'RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS'
);
```

**Critical check:** Snowflake silently drops unrecognized YAML fields. The exported YAML must match your source. If fields are missing, they were not supported at deployment time and need to be restructured.

---

## Step 6 — Smoke Test with SEMANTIC_VIEW()

```sql
-- Grade A pass rate by borough
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS
  METRICS grade_a_pass_rate, total_inspections
  DIMENSIONS borough
)
ORDER BY grade_a_pass_rate DESC;

-- Average score by cuisine (highest = worst)
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS
  METRICS average_score, total_inspections
  DIMENSIONS cuisine_type
)
ORDER BY average_score DESC
LIMIT 10;

-- Monthly trend
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS
  METRICS total_inspections, average_score
  DIMENSIONS year_number, month_name, month_number
)
ORDER BY year_number DESC, month_number DESC
LIMIT 24;
```

---

## Step 7 — Test Cortex Analyst in Snowsight

Navigate to **Snowsight → AI & ML → Cortex Analyst**, or generate the direct URL:

```sql
SELECT 'https://app.snowflake.com/' ||
       CURRENT_ORGANIZATION_NAME() || '/' ||
       CURRENT_ACCOUNT_NAME() ||
       '/#/studio/analyst/databases/RESTAURANT_INTELLIGENCE/schemas/MARTS/semanticView/NYC_RESTAURANT_INSPECTIONS/edit'
  AS cortex_analyst_url;
```

Test these questions — they map to verified queries in the YAML:

1. "What is the grade distribution by borough?"
2. "What is the Grade A pass rate for each borough?"
3. "What are the most frequently cited critical violations?"
4. "Show me the monthly trend of inspections and average scores"
5. "Which restaurants have the worst inspection scores this year?"

> ⚠️ For question 5 — if the agent returns restaurants with the **highest scores** as "best", the custom instruction controlling score directionality is not being applied. Higher score = more violations = worse outcome. This is counter-intuitive and must be explicitly taught in the YAML.

---

## Step 8 — Updating the Semantic View

To update the semantic view, rerun `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` with `FALSE` — the procedure replaces the existing view in place. A DROP is not required.

```sql
-- Always validate first
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$ <updated YAML> $$,
  TRUE
);

-- Then deploy — replaces the existing view
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$ <updated YAML> $$,
  FALSE
);
```

If you need to remove the view entirely:

```sql
DROP SEMANTIC VIEW IF EXISTS RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS;
```

The only property alterable without redeployment:

```sql
ALTER SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS
  SET COMMENT = 'Updated comment';
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Insufficient privileges to create semantic view` | Missing GRANT | Run Step 1 grants as ACCOUNTADMIN |
| `LLMs not available in your region` | EU account without cross-region | Run `ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'` as ACCOUNTADMIN |
| Dry run passes, fields missing after creation | Unrecognized YAML fields silently dropped | Export with `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` and compare |
| `Referenced key must be primary/unique` on join | Foreign key column missing `unique: true` | Add to the dimension definition in YAML |
| Borough filter returns 0 rows | Case mismatch between YAML `sample_values` and data | Match values exactly — data uses uppercase `BROOKLYN`, not `Brooklyn` |
| Metric returns 100% for small sample cuisines | Join fanout multiplying counts | Use `COUNT(DISTINCT inspection_id)` in the metric expression |
| Verified queries not available in Snowsight | SQL references physical table names | Verified queries must use `__logical_table` double-underscore prefix syntax |

---

## Design Notes

**Why 1,365 lines?** SVA autopilot generates a usable skeleton but cannot produce: metric business definitions, `sample_values` for filter guidance, `custom_instructions` for score directionality, or `verified_queries` for common patterns. Every line added beyond the scaffold is knowledge the LLM cannot derive from the schema.

**Why is metric table placement important?** In Snowflake semantic views, the table where a metric is defined controls the default join path for that metric. Placing metrics on the wrong table creates multi-path ambiguity that breaks query generation. All primary metrics live on `fct_inspections` in this implementation.

**Named filters must be declared on every fact table where they apply.** A filter declared on `fct_inspections` is not automatically available on `fct_violations`. Duplicate the declaration — it is not redundant.
