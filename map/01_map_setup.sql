-- =============================================================================
-- 01_map_setup.sql
-- Map explorer — grants required for the Streamlit app role
--
-- Run once as ACCOUNTADMIN after completing Phase 1 (setup/) and
-- Phase 2 (semantic/) setup.
--
-- The map app queries SEMANTIC_VIEW(nyc_restaurant_inspections) exclusively —
-- it never issues direct SQL against MARTS tables. Two grant types are
-- therefore required:
--
--   1. SELECT ON SEMANTIC VIEW   — to call SEMANTIC_VIEW() at all
--   2. SELECT ON ALL TABLES      — Snowflake resolves the semantic view at
--                                  runtime against the underlying MARTS tables;
--                                  the role must have SELECT on those tables too.
--
-- Both grants must be in place. Missing either produces an error:
--   "Insufficient privileges to operate on semantic view"     (missing #1)
--   "Insufficient privileges to operate on table ..."         (missing #2)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Semantic view — SELECT + REFERENCES
--    SELECT allows SEMANTIC_VIEW() queries.
--    REFERENCES allows the role to see the view definition (DESCRIBE, lineage).
--    Both are required for the SiS runtime to resolve the semantic view.
-- -----------------------------------------------------------------------------
GRANT SELECT ON SEMANTIC VIEW
    RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
    TO ROLE RESTAURANT_LOADER;

GRANT REFERENCES ON SEMANTIC VIEW
    RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
    TO ROLE RESTAURANT_LOADER;

-- -----------------------------------------------------------------------------
-- 2. Underlying MARTS tables
--    Snowflake resolves SEMANTIC_VIEW() against the physical tables at runtime.
--    The querying role must have SELECT on all tables referenced by the view.
--    Safe to run multiple times — grants are idempotent.
-- -----------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS
    TO ROLE RESTAURANT_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS
    TO ROLE RESTAURANT_LOADER;

-- FUTURE grant ensures new MARTS tables added by dbt runs are accessible.
GRANT SELECT ON FUTURE TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS
    TO ROLE RESTAURANT_LOADER;

-- -----------------------------------------------------------------------------
-- 3. Stage for Streamlit app files
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Streamlit in Snowflake files — map explorer';

GRANT READ, WRITE ON STAGE RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE
    TO ROLE RESTAURANT_LOADER;

-- CREATE STREAMLIT is a separate privilege not covered by CREATE TABLE / CREATE STAGE.
-- Without it: "Insufficient privileges to operate on schema 'RAW'".
GRANT CREATE STREAMLIT ON SCHEMA RESTAURANT_INTELLIGENCE.RAW
    TO ROLE RESTAURANT_LOADER;

-- -----------------------------------------------------------------------------
-- 4. Optional: separate analyst role
--    If your SiS app runs under a different role, replace RESTAURANT_LOADER.
--    The semantic/ deploy script (02_deploy_semantic.sql) has the same pattern.
-- -----------------------------------------------------------------------------
-- GRANT USAGE  ON DATABASE  RESTAURANT_INTELLIGENCE              TO ROLE <SIS_ROLE>;
-- GRANT USAGE  ON WAREHOUSE RESTAURANT_WH                        TO ROLE <SIS_ROLE>;
-- GRANT USAGE  ON SCHEMA    RESTAURANT_INTELLIGENCE.MARTS        TO ROLE <SIS_ROLE>;
-- GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS
--     TO ROLE <SIS_ROLE>;
-- GRANT SELECT    ON SEMANTIC VIEW
--     RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections   TO ROLE <SIS_ROLE>;
-- GRANT REFERENCES ON SEMANTIC VIEW
--     RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections   TO ROLE <SIS_ROLE>;

-- -----------------------------------------------------------------------------
-- 5. Verify — semantic view must appear and be queryable
-- -----------------------------------------------------------------------------
SHOW SEMANTIC VIEWS IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS;
-- Expected: one row — nyc_restaurant_inspections

-- Smoke test: borough counts via the governed metric
-- If this returns rows, both grants are working correctly.
USE ROLE RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;

SELECT *
FROM SEMANTIC_VIEW(
    RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
    DIMENSIONS borough
    METRICS total_restaurants
)
WHERE borough IS NOT NULL
  AND borough != '0'
ORDER BY borough;
-- Expected: 5 rows — Bronx, Brooklyn, Manhattan, Queens, Staten Island

-- Coordinate coverage check (facts only — no METRICS clause).
-- Snowflake does not allow FACTS and METRICS in the same SEMANTIC_VIEW() call.
SELECT borough, AVG(latitude) AS avg_lat, AVG(longitude) AS avg_lon
FROM SEMANTIC_VIEW(
    RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections
    DIMENSIONS borough
    FACTS latitude, longitude
)
WHERE borough != '0'
GROUP BY borough
ORDER BY borough;
-- Expected: avg_lat ~40–41, avg_lon ~-74. NULL values mean coordinates were
-- omitted during ingestion — re-run ingestion with Socrata lat/lon fields.
-- The map still renders; restaurants without coordinates are excluded silently.
