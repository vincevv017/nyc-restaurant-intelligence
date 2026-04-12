-- =============================================================================
-- 01_setup_memory_table.sql
-- Phase 5: Persistent User Memory
--
-- Creates AGENT_USER_MEMORY in RESTAURANT_INTELLIGENCE.RAW.
-- Run once as RESTAURANT_LOADER (or ACCOUNTADMIN).
--
-- Schema rationale:
--   RAW is used because RESTAURANT_LOADER already has full DML privileges
--   there (granted in 01_snowflake_setup.sql). No new grants are required.
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;

-- -----------------------------------------------------------------------------
-- User memory table
--   user_id      – Snowflake username (or any caller-supplied identity)
--   fact_key     – stable snake_case label, e.g. "home_address", "role"
--   fact_value   – the stored value
--   source_turn  – the raw user message that provided the value (audit trail)
--   updated_at   – auto-set on every upsert
--
-- Primary key on (user_id, fact_key) makes MERGE idempotent:
--   the same fact can be updated in-place rather than accumulating duplicates.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY (
    user_id      VARCHAR        NOT NULL,
    fact_key     VARCHAR        NOT NULL,
    fact_value   VARCHAR,
    source_turn  VARCHAR,
    updated_at   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_agent_user_memory PRIMARY KEY (user_id, fact_key)
);

-- Cluster by user_id so per-user lookups scan only the relevant micro-partitions
ALTER TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
    CLUSTER BY (user_id);

-- -----------------------------------------------------------------------------
-- Verify
-- -----------------------------------------------------------------------------
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY;
-- Expected: 5 columns — user_id, fact_key, fact_value, source_turn, updated_at
-- Primary key constraint on (user_id, fact_key)
