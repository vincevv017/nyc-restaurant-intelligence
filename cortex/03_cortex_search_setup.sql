-- =============================================================================
-- 03_cortex_search_setup.sql
-- NYC Restaurant Intelligence — Phase 3: Cortex Search
-- =============================================================================
-- Run as RESTAURANT_LOADER role AFTER:
--   1. load_inspections.py  (populates RAW.INSPECTIONS_RAW)
--   2. dbt run              (populates MARTS tables)
--   3. load_documents.py    (populates RAW.DOCUMENT_CHUNKS)
--
-- What this script does:
--   Step 1  — Verify document chunks loaded correctly
--   Step 2  — Grant Cortex Search privileges
--   Step 3  — Create the Cortex Search service (with tuning)
--   Step 4  — Add named scoring profile (reranker control)
--   Step 5  — Smoke test the service
--   Step 6  — Validate scoring profile side-by-side
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE SCHEMA    RAW;


-- =============================================================================
-- STEP 1 — Verify document chunks are loaded
-- Expected: rows across health_code, inspection_procedures, operator_guide,
--           enforcement doc_types. If this returns 0, run load_documents.py first.
-- =============================================================================

SELECT
    doc_type,
    COUNT(DISTINCT source_url) AS documents,
    COUNT(*)                   AS chunks,
    ROUND(AVG(word_count), 0)  AS avg_words_per_chunk,
    MAX(loaded_at)             AS last_loaded
FROM RESTAURANT_INTELLIGENCE.RAW.DOCUMENT_CHUNKS
GROUP BY doc_type
ORDER BY doc_type;


-- =============================================================================
-- STEP 2 — Grant privileges
-- Run the ACCOUNTADMIN block once; the RESTAURANT_LOADER block is idempotent.
-- =============================================================================

-- ── Run as ACCOUNTADMIN ──────────────────────────────────────────────────────
-- GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA RESTAURANT_INTELLIGENCE.RAW
--   TO ROLE RESTAURANT_LOADER;

-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;
-- ────────────────────────────────────────────────────────────────────────────


-- =============================================================================
-- STEP 3 — Create the Cortex Search service
-- =============================================================================
--
-- Design decisions documented here for article reproducibility:
--
-- EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
--   The default (snowflake-arctic-embed-m-v1.5) uses 768 dimensions.
--   The L v2.0 model uses 1024 dimensions — better for nuanced regulatory
--   text like health code statutes and enforcement procedures.
--   NOTE: Embedding model CANNOT be changed after creation — choose wisely.
--   Check regional availability if you're outside US regions.
--
-- PRIMARY KEY (doc_id)
--   Enables optimized incremental refresh when document chunks change.
--   Reduces cost and latency of index updates. Requires TEXT data type.
--   Our doc_id is a UUID VARCHAR — perfect candidate.
--
-- ATTRIBUTES doc_type, doc_label
--   Only columns the agent should realistically FILTER on.
--   doc_type: category scoping ("search only health_code documents")
--   doc_label: human-readable document name for citations
--   NOT included as ATTRIBUTES (but still in SELECT, so returnable):
--     source_url  — useful in results, but never a useful filter target
--     page_number — same reasoning; nobody asks "search only page 7"
--   Fewer ATTRIBUTES = smaller filter index = less for the agent to misuse.
--
-- TARGET_LAG = '1 day'
--   Documents change rarely (regulatory PDFs). '1 day' is sufficient.
--   Use '1 hour' during active development if re-loading documents frequently.
--
-- Source query filters:
--   chunk_text IS NOT NULL — exclude empty extraction artifacts
--   word_count >= 50       — exclude trivial fragments (headers, page numbers)
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
  ON chunk_text
  PRIMARY KEY (doc_id)
  ATTRIBUTES doc_type, doc_label
  WAREHOUSE  = RESTAURANT_WH
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
        doc_id,
        chunk_text,
        doc_type,
        doc_label,
        source_url,
        page_number,
        chunk_index,
        word_count,
        loaded_at
    FROM RESTAURANT_INTELLIGENCE.RAW.DOCUMENT_CHUNKS
    WHERE chunk_text IS NOT NULL
      AND word_count >= 50          -- exclude near-empty fragments
  );


-- =============================================================================
-- STEP 4 — Add named scoring profile
-- =============================================================================
-- The default Cortex Search pipeline is: vector + keyword + semantic reranker.
-- For our health code documents — highly structured, keyword-dense legal text
-- with linearized PDF tables — the reranker actively HURTS relevance by
-- down-ranking content that matches well on keywords but looks structurally
-- unusual to the reranker's neural model.
--
-- Named scoring profiles (GA October 2025) let us bake this into the service
-- so the agent doesn't need to pass scoring_config in every query.
--
-- We create two profiles:
--   'no_reranker'       — pure hybrid search (vector + keyword)
--   'keyword_heavy'     — bias toward keyword matching for legal text lookups
-- =============================================================================

ALTER CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
  ADD SCORING PROFILE IF NOT EXISTS no_reranker
  '{"reranker": "none"}';

ALTER CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
  ADD SCORING PROFILE IF NOT EXISTS keyword_heavy
  '{
    "reranker": "none",
    "weights": {
      "texts": 3,
      "vectors": 1
    }
  }';

-- drop profiles
-- ALTER CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
--  DROP SCORING PROFILE no_reranker;

-- ALTER CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
--  DROP SCORING PROFILE keyword_heavy;

-- =============================================================================
-- STEP 5 — Smoke test the Cortex Search service
-- Each query tests a different path. Expected: 3 results per call
-- containing relevant text excerpts.
-- =============================================================================

-- Verify service is active and check scoring profiles
SHOW CORTEX SEARCH SERVICES IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;
-- scoring_profile_count should be 2

DESCRIBE CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search;


-- Test A: Broad search — no filter, default scoring
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "what violations lead to restaurant closure",
      "columns": ["chunk_text", "doc_type", "doc_label", "page_number"],
      "limit":   3
    }'
  )
)['results'] AS results;


-- Test B: Filter to health code only
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "temperature control for TCS foods cold storage requirements",
      "columns": ["chunk_text", "doc_type", "doc_label", "page_number"],
      "filter":  {"@eq": {"doc_type": "health_code"}},
      "limit":   3
    }'
  )
)['results'] AS results;


-- Test C: Filter to inspection procedures
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "how are violation points calculated scoring conditions",
      "columns": ["chunk_text", "doc_type", "doc_label", "page_number"],
      "filter":  {"@eq": {"doc_type": "inspection_procedures"}},
      "limit":   3
    }'
  )
)['results'] AS results;


-- Test D: The key agent query — violation code lookup
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "violation code 04L rodent evidence mice",
      "columns": ["chunk_text", "doc_type", "page_number"],
      "limit":   3
    }'
  )
)['results'] AS results;


-- =============================================================================
-- STEP 6 — Compare scoring profiles side-by-side
-- Run the same query with default scoring vs no_reranker vs keyword_heavy.
-- For regulatory documents, no_reranker and keyword_heavy typically outperform
-- the default on table-heavy content (violation condition tables, scoring grids).
-- =============================================================================

-- 6A: Default scoring (with reranker)
SELECT 'default' AS profile, PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "violation 04L mice evidence condition levels points",
      "columns": ["chunk_text", "doc_type", "page_number"],
      "limit":   3
    }'
  )
)['results'] AS results;

-- 6B: No reranker
SELECT 'no_reranker' AS profile, PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "violation 04L mice evidence condition levels points",
      "columns": ["chunk_text", "doc_type", "page_number"],
      "scoring_config": {"reranker": "none"},
      "limit":   3
    }'
  )
)['results'] AS results;

-- 6C: Keyword-heavy
SELECT 'keyword_heavy' AS profile, PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search',
    '{
      "query":   "violation 04L mice evidence condition levels points",
      "columns": ["chunk_text", "doc_type", "page_number"],
      "scoring_config": {"reranker": "none", "weights": {"texts": 3, "vectors": 1}},
      "limit":   3
    }'
  )
)['results'] AS results;


-- =============================================================================
-- REFERENCE — Cortex Agent specification (for Phase 3)
-- =============================================================================
-- Minimal, working agent specification. Key lessons learned:
--
-- 1. LESS IS MORE in tool_resources for Cortex Search.
--    Over-specifying columns_and_descriptions gives the orchestration LLM
--    more opportunities to generate malformed payloads. The search service
--    already knows which columns are searchable (ON chunk_text) and
--    filterable (ATTRIBUTES). Let it work.
--
-- 2. If you DO add columns_and_descriptions, use LOWERCASE column names.
--    Snowflake stores unquoted identifiers in uppercase internally, but
--    Cortex Search returns them in lowercase. The agent generates search
--    payloads using the case you specify — uppercase = silent empty results.
--
-- 3. Never use @and filters requiring a chunk to match ALL doc_types.
--    A chunk belongs to exactly one doc_type. Use @eq for single-value
--    filters, or @or if the agent needs to search across specific categories.
--
-- 4. Reranker control: use named scoring profiles on the service itself
--    rather than relying on the agent to pass scoring_config in every query.
--
-- Minimal spec (recommended starting point):
--
-- ALTER AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
-- MODIFY LIVE VERSION SET SPECIFICATION =
-- $$
-- models:
--   orchestration: claude-sonnet-4-6
-- instructions:
--   response: "You are a NYC public health analytics assistant..."
-- tools:
--   - tool_spec:
--       type: cortex_analyst_text_to_sql
--       name: nyc_inspection_analyst
--       description: "Queries live DOHMH inspection data"
--   - tool_spec:
--       type: cortex_search
--       name: nyc_health_docs_search
--       description: "Searches NYC Health Code PDFs for legal definitions"
-- tool_resources:
--   nyc_inspection_analyst:
--     semantic_view: "RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS"
--   nyc_health_docs_search:
--     name: "RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search"
--     max_results: 5
-- $$
--
-- If the agent makes poor routing decisions, add minimal column descriptions:
--
--     columns_and_descriptions:
--       chunk_text:
--         description: "Full text content from health code documents"
--       doc_type:
--         description: "Document category: health_code, inspection_procedures, operator_guide, enforcement"
--
-- =============================================================================