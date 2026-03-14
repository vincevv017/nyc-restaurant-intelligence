-- =============================================================================
-- NYC Restaurant Inspections — Deploy Semantic View from YAML
-- =============================================================================
--
-- RECOMMENDED deployment path: YAML via SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML
-- The YAML format supports features not available in DDL:
--   - sample_values (guides Cortex Analyst filtering)
--   - is_enum (constrains dimension values to known set)
--   - filters (pre-defined named filter expressions)
--   - module_custom_instructions (business rules for SQL generation)
--
-- Usage:
--   1. Open nyc_restaurant_inspections.yaml
--   2. Copy ALL its contents
--   3. Paste between each $$ ... $$ pair below (replacing the blank space)
--
--   ⚠️  Do NOT include the $$ delimiters — they are already in this script.
--       Just paste the raw YAML content between them.
--
--   OR use Snowsight: AI & ML → Cortex Analyst → Upload YAML directly
--
-- Prerequisites:
--   - setup/01_snowflake_setup.sql executed as ACCOUNTADMIN
--   - Step 0 privileges granted (see below)
--   - Ingestion pipeline completed (load_inspections.py)
--   - dbt transformations completed (dbt run)
--   - All MARTS tables populated
-- =============================================================================


-- =============================================================================
-- Step 0: PRIVILEGES (run once as ACCOUNTADMIN)
-- =============================================================================
-- Three grants required beyond the original setup script:
--
--   1. CREATE SEMANTIC VIEW — distinct privilege, not included in CREATE TABLE
--   2. CORTEX_ANALYST_USER — required to access Cortex Analyst feature
--   3. Cross-region inference — required for EU/non-US regions where
--      Cortex Analyst LLMs are not locally hosted
-- =============================================================================

-- >>> RUN AS ACCOUNTADMIN <<<
-- USE ROLE ACCOUNTADMIN;
--
-- -- Semantic view creation privilege
-- GRANT CREATE SEMANTIC VIEW ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS
--   TO ROLE RESTAURANT_LOADER;
--
-- -- Cortex Analyst feature access
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER
--   TO ROLE RESTAURANT_LOADER;
--
-- -- Cross-region inference (required for EU regions)
-- -- Options: 'ANY_REGION', 'AWS_US_REGIONS', 'AWS_EU_AND_US_REGIONS'
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
--
-- >>> END ACCOUNTADMIN BLOCK <<<


-- =============================================================================
-- Step 1: Validate YAML (dry run)
-- =============================================================================

USE ROLE RESTAURANT_LOADER;
USE DATABASE RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;

-- Paste YAML between $$ ... $$ — validates without creating
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$

  $$,
  TRUE  -- verify_only
);

-- Expected output:
-- YAML file is valid for creating a semantic view. No object has been created yet.


-- =============================================================================
-- Step 2: Create (or replace) the semantic view
-- =============================================================================

-- Paste the same YAML between $$ ... $$
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'RESTAURANT_INTELLIGENCE.MARTS',
  $$

  $$
);


-- =============================================================================
-- Step 3: Verify creation
-- =============================================================================

DESCRIBE SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections;


-- =============================================================================
-- Step 4: Smoke test via SEMANTIC_VIEW() function
-- =============================================================================
-- NOTE: Semantic views use a special query syntax — not regular SQL.
-- You must use SEMANTIC_VIEW() with pre-defined METRICS and DIMENSIONS.
-- Regular SQL (SELECT COUNT(*) FROM ...) does NOT work on semantic views.
-- =============================================================================

-- Grade distribution by borough
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
  METRICS total_inspections
  DIMENSIONS borough, grade
)
WHERE grade IN ('A', 'B', 'C')
ORDER BY borough, grade;

-- Pass rate by borough
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
  METRICS grade_a_pass_rate, total_inspections
  DIMENSIONS borough
)
ORDER BY grade_a_pass_rate DESC;

-- Monthly trend
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
  METRICS total_inspections, average_score, grade_a_pass_rate
  DIMENSIONS year_number, month_name, month_number
)
WHERE year_number = 2025
ORDER BY month_number;

-- Critical violation rate by cuisine
SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
  METRICS average_score, critical_violation_rate
  DIMENSIONS cuisine_type
)
ORDER BY average_score DESC
LIMIT 15;


-- =============================================================================
-- Step 5: Export YAML (for version control / round-trip validation)
-- =============================================================================

SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW(
  'RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections'
);


-- =============================================================================
-- Step 6: Open Cortex Analyst
-- =============================================================================
-- Generate a direct link to Cortex Analyst for this semantic view:

SELECT 'https://app.snowflake.com/' ||
       CURRENT_ORGANIZATION_NAME() || '/' ||
       CURRENT_ACCOUNT_NAME() ||
       '/#/studio/analyst/databases/RESTAURANT_INTELLIGENCE/schemas/MARTS/semanticView/NYC_RESTAURANT_INSPECTIONS/edit'
  AS cortex_analyst_url;


-- =============================================================================
-- OPTIONAL: Grants for separate analyst role
-- =============================================================================
-- Uncomment if you want RBAC separation between loader and analyst roles.
-- Cortex Analyst requires SELECT on BOTH the semantic view AND underlying tables.
-- =============================================================================

-- CREATE ROLE IF NOT EXISTS RESTAURANT_ANALYST;
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE RESTAURANT_ANALYST;
-- GRANT USAGE ON DATABASE RESTAURANT_INTELLIGENCE TO ROLE RESTAURANT_ANALYST;
-- GRANT USAGE ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS TO ROLE RESTAURANT_ANALYST;
-- GRANT USAGE ON WAREHOUSE RESTAURANT_WH TO ROLE RESTAURANT_ANALYST;
-- GRANT SELECT ON SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
--   TO ROLE RESTAURANT_ANALYST;
-- GRANT REFERENCES ON SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
--   TO ROLE RESTAURANT_ANALYST;
-- GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS
--   TO ROLE RESTAURANT_ANALYST;


-- =============================================================================
-- OPTIONAL: Lineage verification
-- =============================================================================
-- Run after querying the semantic view to verify lineage tracking.
-- Requires ACCOUNTADMIN or GOVERNANCE_VIEWER role.
-- =============================================================================

-- SELECT
--   query_id,
--   direct_objects_accessed,
--   objects_modified
-- FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
-- WHERE query_start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
-- ORDER BY query_start_time DESC
-- LIMIT 10;