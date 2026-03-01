-- =============================================================================
-- 01_snowflake_setup.sql
-- Run this ONCE in your Snowflake trial worksheet (logged in as ACCOUNTADMIN)
-- Creates all infrastructure needed for the NYC Restaurant Intelligence project
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Warehouse
--    X-SMALL is sufficient for trial. AUTO_SUSPEND = 60s prevents credit burn.
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS RESTAURANT_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'NYC Restaurant Intelligence - trial warehouse';

-- -----------------------------------------------------------------------------
-- 2. Database & Schemas
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS RESTAURANT_INTELLIGENCE
    COMMENT = 'NYC Restaurant Inspection data pipeline and AI/BI demo';

CREATE SCHEMA IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW
    COMMENT = 'Raw data as received from Socrata API - all VARCHAR, no transformations';

CREATE SCHEMA IF NOT EXISTS RESTAURANT_INTELLIGENCE.STAGING
    COMMENT = 'Cleaned, typed, deduplicated staging models (dbt)';

CREATE SCHEMA IF NOT EXISTS RESTAURANT_INTELLIGENCE.MARTS
    COMMENT = 'Star schema fact and dimension tables ready for Snowflake Intelligence';

-- -----------------------------------------------------------------------------
-- 3. Role & User permissions
--    We use a dedicated role so the Python script never runs as ACCOUNTADMIN
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RESTAURANT_LOADER
    COMMENT = 'Used by ingestion script and dbt';

GRANT USAGE  ON WAREHOUSE RESTAURANT_WH                       TO ROLE RESTAURANT_LOADER;
GRANT USAGE         ON DATABASE  RESTAURANT_INTELLIGENCE      TO ROLE RESTAURANT_LOADER;
GRANT CREATE SCHEMA ON DATABASE  RESTAURANT_INTELLIGENCE      TO ROLE RESTAURANT_LOADER;

-- RAW schema: loader needs full DML
GRANT USAGE                          ON SCHEMA RESTAURANT_INTELLIGENCE.RAW     TO ROLE RESTAURANT_LOADER;
GRANT CREATE TABLE, CREATE STAGE     ON SCHEMA RESTAURANT_INTELLIGENCE.RAW     TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES  IN SCHEMA RESTAURANT_INTELLIGENCE.RAW TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.RAW TO ROLE RESTAURANT_LOADER;

-- STAGING & MARTS: dbt creates and owns these objects
GRANT USAGE                          ON SCHEMA RESTAURANT_INTELLIGENCE.STAGING  TO ROLE RESTAURANT_LOADER;
GRANT CREATE TABLE, CREATE VIEW      ON SCHEMA RESTAURANT_INTELLIGENCE.STAGING  TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES   IN SCHEMA RESTAURANT_INTELLIGENCE.STAGING TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.STAGING TO ROLE RESTAURANT_LOADER;

GRANT USAGE                          ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS    TO ROLE RESTAURANT_LOADER;
GRANT CREATE TABLE, CREATE VIEW      ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS    TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES   IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS TO ROLE RESTAURANT_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA RESTAURANT_INTELLIGENCE.MARTS TO ROLE RESTAURANT_LOADER;

-- Grant role to your user (replace YOUR_USERNAME with your actual Snowflake username)
-- GRANT ROLE RESTAURANT_LOADER TO USER YOUR_USERNAME;

-- -----------------------------------------------------------------------------
-- 4. RAW table
--    All columns are VARCHAR. The ingestion script writes exactly what the
--    Socrata API returns. Typing happens in the STAGING layer (dbt).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW (
    -- Restaurant identifiers
    CAMIS                       VARCHAR,   -- Unique establishment identifier
    DBA                         VARCHAR,   -- "Doing Business As" name
    BORO                        VARCHAR,   -- Borough
    BUILDING                    VARCHAR,
    STREET                      VARCHAR,
    ZIPCODE                     VARCHAR,
    PHONE                       VARCHAR,
    CUISINE_DESCRIPTION         VARCHAR,

    -- Inspection event
    INSPECTION_DATE             VARCHAR,   -- Typed to DATE in staging
    ACTION                      VARCHAR,   -- Violations cited, re-opened, etc.
    VIOLATION_CODE              VARCHAR,
    VIOLATION_DESCRIPTION       VARCHAR,
    CRITICAL_FLAG               VARCHAR,   -- Critical / Not Critical / Not Applicable
    SCORE                       VARCHAR,   -- Typed to INTEGER in staging
    GRADE                       VARCHAR,   -- A / B / C / Z / P / Not Yet Graded
    GRADE_DATE                  VARCHAR,   -- Typed to DATE in staging
    RECORD_DATE                 VARCHAR,   -- Typed to DATE in staging
    INSPECTION_TYPE             VARCHAR,

    -- Geographic (optional, present in full dataset)
    LATITUDE                    VARCHAR,
    LONGITUDE                   VARCHAR,
    COMMUNITY_BOARD             VARCHAR,
    COUNCIL_DISTRICT            VARCHAR,
    CENSUS_TRACT                VARCHAR,
    BIN                         VARCHAR,
    BBL                         VARCHAR,
    NTA                         VARCHAR,   -- Neighborhood Tabulation Area

    -- Pipeline metadata
    _LOADED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- 5. Verify setup
-- -----------------------------------------------------------------------------
SHOW SCHEMAS IN DATABASE RESTAURANT_INTELLIGENCE;
SHOW TABLES  IN SCHEMA  RESTAURANT_INTELLIGENCE.RAW;