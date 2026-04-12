-- =============================================================================
-- 01_memory_vector_table.sql
-- Native Agent Memory: Persistent User Facts with Semantic Embeddings
-- =============================================================================
-- Creates AGENT_MEMORY_VECTORS in RESTAURANT_INTELLIGENCE.RAW.
-- Run once as RESTAURANT_LOADER (or ACCOUNTADMIN).
--
-- This table replaces AGENT_USER_MEMORY (Phase 5) for native agent memory.
-- Phase 5's table stores key-value pairs retrieved via exact PK lookup.
-- This table adds a VECTOR column for semantic similarity retrieval,
-- enabling the agent to find relevant facts via AI_EMBED + cosine similarity.
--
-- Why a new table instead of ALTER TABLE on the Phase 5 table?
--   1. Clean separation — Phase 5 (client-side) and Phase 6 (native) can
--      coexist without migration risk
--   2. The vector column requires backfilling embeddings for existing rows —
--      a new table makes this explicit
--   3. Different access pattern — Phase 5 loads ALL facts; Phase 6 retrieves
--      top-k by similarity
--
-- Embedding model: snowflake-arctic-embed-l-v2.0 → 1024 dimensions
--   Chosen for consistency with the project's Cortex Search service
--   (03_cortex_search_setup.sql). For short personal facts (addresses,
--   preferences), the smaller snowflake-arctic-embed-m-v1.5 (768 dims)
--   would be sufficient and ~30% cheaper per embedding call.
--
-- References:
--   AI_EMBED: https://docs.snowflake.com/en/sql-reference/functions/ai_embed
--   Vector embeddings: https://docs.snowflake.com/en/user-guide/snowflake-cortex/vector-embeddings
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;

-- -----------------------------------------------------------------------------
-- Memory table with semantic vector column
--
--   user_id        – Snowflake username via CURRENT_USER() in EXECUTE AS CALLER
--                    procedures. Scopes memory per user.
--   fact_key       – snake_case label: home_address, preferred_borough, role
--   fact_value     – the stored string value
--   category       – grouping for retrieval filtering: location, preference,
--                    identity, analytical. Agent decides category at store time.
--   fact_embedding – 1024-dim vector from AI_EMBED. Used for semantic retrieval
--                    via VECTOR_COSINE_SIMILARITY. Computed at INSERT/UPDATE time.
--   source_turn    – raw user message that provided the value (audit trail)
--   confidence     – agent's confidence in the extraction (0.0–1.0), reserved
--                    for future use. Default 1.0 for explicit user statements.
--   created_at     – first insertion timestamp (immutable)
--   updated_at     – last upsert timestamp
--
-- Primary key on (user_id, fact_key) makes MERGE idempotent:
--   the same fact can be updated in-place rather than accumulating duplicates.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS (
    user_id        VARCHAR        NOT NULL,
    fact_key       VARCHAR        NOT NULL,
    fact_value     VARCHAR        NOT NULL,
    category       VARCHAR        DEFAULT 'general',
    fact_embedding VECTOR(FLOAT, 1024),
    source_turn    VARCHAR,
    confidence     FLOAT          DEFAULT 1.0,
    created_at     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    updated_at     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_agent_memory_vectors PRIMARY KEY (user_id, fact_key)
);

-- Cluster by user_id — all memory queries are user-scoped
ALTER TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
    CLUSTER BY (user_id);

-- Comment for discoverability
COMMENT ON TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS IS 'Native agent memory — user facts with AI_EMBED vectors for semantic retrieval. Used by STORE_USER_MEMORY and RETRIEVE_USER_MEMORIES stored procedures.';


-- =============================================================================
-- Verify
-- =============================================================================
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS;
-- Expected: 9 columns
--   USER_ID        VARCHAR        NOT NULL
--   FACT_KEY       VARCHAR        NOT NULL
--   FACT_VALUE     VARCHAR        NOT NULL
--   CATEGORY       VARCHAR        DEFAULT 'general'
--   FACT_EMBEDDING VECTOR(FLOAT, 1024)
--   SOURCE_TURN    VARCHAR
--   CONFIDENCE     FLOAT          DEFAULT 1.0
--   CREATED_AT     TIMESTAMP_NTZ
--   UPDATED_AT     TIMESTAMP_NTZ
-- Primary key constraint on (USER_ID, FACT_KEY)


-- =============================================================================
-- Optional: Migrate existing Phase 5 facts into the vector table
-- =============================================================================
-- If you have facts in AGENT_USER_MEMORY from Phase 5, this backfills them
-- with embeddings. Run once after creating the table.
--
-- MERGE INTO RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS AS target
-- USING (
--     SELECT
--         user_id,
--         fact_key,
--         fact_value,
--         'general'  AS category,
--         AI_EMBED('snowflake-arctic-embed-l-v2.0',
--                  fact_key || ': ' || fact_value) AS fact_embedding,
--         source_turn,
--         1.0        AS confidence,
--         updated_at AS created_at,
--         updated_at
--     FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
-- ) AS source
-- ON target.user_id = source.user_id AND target.fact_key = source.fact_key
-- WHEN NOT MATCHED THEN INSERT (
--     user_id, fact_key, fact_value, category, fact_embedding,
--     source_turn, confidence, created_at, updated_at
-- ) VALUES (
--     source.user_id, source.fact_key, source.fact_value, source.category,
--     source.fact_embedding, source.source_turn, source.confidence,
--     source.created_at, source.updated_at
-- );
