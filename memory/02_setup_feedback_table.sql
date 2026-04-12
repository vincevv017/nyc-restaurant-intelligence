-- =============================================================================
-- 02_setup_feedback_table.sql
-- Phase 5: Agent Response Feedback
--
-- Creates AGENT_FEEDBACK in RESTAURANT_INTELLIGENCE.RAW.
-- Run once as RESTAURANT_LOADER (or ACCOUNTADMIN).
--
-- Schema rationale:
--   RAW is used because RESTAURANT_LOADER already has full DML privileges
--   there. No new grants are required.
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;

-- -----------------------------------------------------------------------------
-- Feedback table
--   feedback_id  – surrogate key (UUID)
--   user_id      – Snowflake username (CURRENT_USER() inside SiS)
--   session_id   – UUID generated once per browser session (tracks a conversation)
--   turn_index   – position of the assistant message in the conversation (0-based)
--   question     – the user's question that triggered the response
--   answer       – the agent's response that was rated
--   rating       – 'up' or 'down'
--   created_at   – auto-set on INSERT
--
-- No MERGE / upsert needed — one INSERT per rating click.
-- A user can only rate each turn once (the button is disabled after the first click).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK (
    feedback_id  VARCHAR        DEFAULT UUID_STRING(),
    user_id      VARCHAR        NOT NULL,
    session_id   VARCHAR        NOT NULL,
    turn_index   INT            NOT NULL,
    question     VARCHAR,
    answer       VARCHAR,
    rating       VARCHAR(4)     NOT NULL,   -- 'up' or 'down'
    created_at   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_agent_feedback PRIMARY KEY (feedback_id)
);

-- Cluster by user_id for per-user analytics; secondary sort on session_id
-- keeps the full conversation grouped together in micro-partitions.
ALTER TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
    CLUSTER BY (user_id, session_id);

-- -----------------------------------------------------------------------------
-- Verify
-- -----------------------------------------------------------------------------
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK;
-- Expected: 8 columns — feedback_id, user_id, session_id, turn_index,
--           question, answer, rating, created_at
