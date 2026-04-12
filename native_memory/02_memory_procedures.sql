-- =============================================================================
-- 02_memory_procedures.sql
-- Native Agent Memory: Store and Retrieve Stored Procedures
-- =============================================================================
-- Creates two Python stored procedures that the Cortex Agent calls as custom
-- tools (type: "generic") to store and retrieve user facts.
--
-- Prerequisites:
--   ✅ 01_memory_vector_table.sql has been run
--   ✅ RESTAURANT_LOADER role has USAGE on RESTAURANT_WH
--   ✅ SNOWFLAKE.CORTEX_USER database role granted to RESTAURANT_LOADER
--      (required for AI_EMBED)
--
-- Architecture:
--   STORE_USER_MEMORY   — agent calls this when user reveals personal info
--                         Embeds fact_key + fact_value via AI_EMBED, upserts row
--   RETRIEVE_USER_MEMORIES — agent calls this before answering personal questions
--                            Embeds the query, returns top-k facts by similarity
--
-- Both procedures use EXECUTE AS CALLER so CURRENT_USER() returns the actual
-- authenticated user, enabling per-user memory scoping without passing user_id.
--
-- References:
--   Custom tools (stored procedures): https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage
--   AI_EMBED function: https://docs.snowflake.com/en/sql-reference/functions/ai_embed
--   Vector similarity: https://docs.snowflake.com/en/user-guide/snowflake-cortex/vector-embeddings
--   Community guide: https://community.snowflake.com/s/article/How-to-configure-Stored-Procedure-as-a-Custom-Tool-in-Snowflake-Intelligence
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;


-- =============================================================================
-- PART 0 — Grants (run as ACCOUNTADMIN once, if not already in place)
-- =============================================================================
-- These grants may already exist from earlier phases. Run them if you get
-- permission errors on CREATE PROCEDURE or AI_EMBED calls.
--
-- USE ROLE ACCOUNTADMIN;
-- GRANT USAGE ON DATABASE RESTAURANT_INTELLIGENCE TO ROLE RESTAURANT_LOADER;
-- GRANT USAGE ON SCHEMA RESTAURANT_INTELLIGENCE.RAW TO ROLE RESTAURANT_LOADER;
-- GRANT CREATE TABLE ON SCHEMA RESTAURANT_INTELLIGENCE.RAW TO ROLE RESTAURANT_LOADER;
-- GRANT CREATE PROCEDURE ON SCHEMA RESTAURANT_INTELLIGENCE.RAW TO ROLE RESTAURANT_LOADER;
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;  -- required for AI_EMBED


-- =============================================================================
-- PROCEDURE 1: STORE_USER_MEMORY
-- =============================================================================
-- The agent calls this when the user reveals personal information.
-- The orchestration LLM decides the fact_key and fact_value based on the
-- conversation context. The category helps organize facts for future filtering.
--
-- Parameters (all VARCHAR — no OBJECT type, per Snowflake agent constraint):
--   fact_key   – snake_case label, e.g. "home_address", "preferred_borough"
--   fact_value – the value to store, e.g. "245 W 107th St, Manhattan"
--   category   – one of: location, preference, identity, analytical, general
--
-- Returns: VARCHAR status message that the agent reads and may relay to user.
--
-- Embedding strategy: concatenate "fact_key: fact_value" for richer semantics.
--   "home_address: 245 W 107th St" embeds differently from just "245 W 107th St"
--   and retrieves better when the query is "restaurants near my home".
-- =============================================================================

CREATE OR REPLACE PROCEDURE RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY(
    fact_key   VARCHAR,
    fact_value VARCHAR,
    category   VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'store_memory'
EXECUTE AS CALLER
COMMENT = 'Stores a personal fact about the current user with semantic embedding. Called by the Cortex Agent when the user reveals personal information.'
AS
$$
def store_memory(session, fact_key: str, fact_value: str, category: str) -> str:
    """
    Store a user fact with semantic embedding for later retrieval.

    Called by the Cortex Agent as a custom tool (type: generic).
    Uses CURRENT_USER() for user scoping (EXECUTE AS CALLER).
    """
    import json

    # ── Validate inputs ──────────────────────────────────────────────────────
    if not fact_key or not fact_key.strip():
        return "Error: fact_key is required and cannot be empty."
    if not fact_value or not fact_value.strip():
        return "Error: fact_value is required and cannot be empty."

    fact_key = fact_key.strip().lower().replace(" ", "_")
    fact_value = fact_value.strip()
    category = (category or "general").strip().lower()

    valid_categories = {"location", "preference", "identity", "analytical", "general"}
    if category not in valid_categories:
        category = "general"

    # ── Escape single quotes for SQL safety ──────────────────────────────────
    safe_key   = fact_key.replace("'", "''")
    safe_value = fact_value.replace("'", "''")
    safe_cat   = category.replace("'", "''")

    # ── Build embedding text: "key: value" for richer semantics ──────────────
    embed_text = f"{fact_key}: {fact_value}".replace("'", "''")

    # ── MERGE: upsert with embedding ─────────────────────────────────────────
    merge_sql = f"""
    MERGE INTO RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS AS target
    USING (
        SELECT
            CURRENT_USER()  AS user_id,
            '{safe_key}'    AS fact_key,
            '{safe_value}'  AS fact_value,
            '{safe_cat}'    AS category,
            AI_EMBED('snowflake-arctic-embed-l-v2.0', '{embed_text}') AS fact_embedding,
            '{safe_value}'  AS source_turn,
            1.0             AS confidence
    ) AS source
    ON  target.user_id  = source.user_id
    AND target.fact_key = source.fact_key
    WHEN MATCHED THEN UPDATE SET
        fact_value     = source.fact_value,
        category       = source.category,
        fact_embedding = source.fact_embedding,
        source_turn    = source.source_turn,
        confidence     = source.confidence,
        updated_at     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        user_id, fact_key, fact_value, category, fact_embedding,
        source_turn, confidence, created_at, updated_at
    ) VALUES (
        source.user_id, source.fact_key, source.fact_value, source.category,
        source.fact_embedding, source.source_turn, source.confidence,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """

    try:
        session.sql(merge_sql).collect()
        return f"Stored: {fact_key} = '{fact_value}' (category: {category})"
    except Exception as e:
        return f"Error storing memory: {str(e)}"
$$;


-- =============================================================================
-- PROCEDURE 2: RETRIEVE_USER_MEMORIES
-- =============================================================================
-- The agent calls this before answering questions that may reference personal
-- context. The query is embedded via AI_EMBED and compared against stored
-- fact embeddings using VECTOR_COSINE_SIMILARITY.
--
-- Parameters:
--   query       – the user's question or a search phrase, e.g.
--                 "restaurants near my home" or "my food preferences"
--   max_results – how many facts to return (default 5, capped at 10)
--
-- Returns: VARCHAR — a formatted list of relevant facts the agent can
--   incorporate into its answer. Returns "No memories found" if the user
--   has no stored facts, enabling the agent to ask for the information.
--
-- Design decision: returns ALL columns (key, value, category, similarity,
--   updated_at) so the agent can reason about recency and relevance.
--   The agent sees this as text and decides which facts to use.
-- =============================================================================

CREATE OR REPLACE PROCEDURE RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES(
    query       VARCHAR,
    max_results INTEGER DEFAULT 5
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'retrieve_memories'
EXECUTE AS CALLER
COMMENT = 'Retrieves relevant stored facts about the current user via semantic similarity. Called by the Cortex Agent before answering personal/preference questions.'
AS
$$
def retrieve_memories(session, query: str, max_results: int = 5) -> str:
    """
    Retrieve user facts ranked by semantic similarity to the query.

    Called by the Cortex Agent as a custom tool (type: generic).
    Uses CURRENT_USER() for user scoping (EXECUTE AS CALLER).
    """
    import json

    # ── Validate inputs ──────────────────────────────────────────────────────
    if not query or not query.strip():
        return "Error: query is required."

    max_results = max(1, min(max_results or 5, 10))
    safe_query = query.strip().replace("'", "''")

    # ── Check if user has any memories at all ────────────────────────────────
    count_result = session.sql("""
        SELECT COUNT(*) AS cnt
        FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
        WHERE user_id = CURRENT_USER()
    """).collect()

    if not count_result or count_result[0]["CNT"] == 0:
        return "No memories found for this user. You may want to ask the user for the information you need."

    # ── Embed query and retrieve by similarity ───────────────────────────────
    retrieval_sql = f"""
    SELECT
        fact_key,
        fact_value,
        category,
        ROUND(VECTOR_COSINE_SIMILARITY(
            fact_embedding,
            AI_EMBED('snowflake-arctic-embed-l-v2.0', '{safe_query}')
        ), 4) AS similarity,
        TO_VARCHAR(updated_at, 'YYYY-MM-DD HH24:MI') AS last_updated
    FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
    WHERE user_id = CURRENT_USER()
      AND fact_embedding IS NOT NULL
    ORDER BY similarity DESC
    LIMIT {max_results}
    """

    try:
        rows = session.sql(retrieval_sql).collect()

        if not rows:
            return "No memories found for this user."

        # ── Format results for the agent ─────────────────────────────────────
        facts = []
        for row in rows:
            sim = row["SIMILARITY"]
            # Only include facts above a minimum relevance threshold
            if sim >= 0.3:
                facts.append(
                    f"- {row['FACT_KEY']}: {row['FACT_VALUE']} "
                    f"(category: {row['CATEGORY']}, "
                    f"relevance: {sim}, "
                    f"updated: {row['LAST_UPDATED']})"
                )

        if not facts:
            return (
                "No relevant memories found for this query. "
                "The user may not have shared this information yet."
            )

        header = f"Found {len(facts)} relevant fact(s) for user {session.sql('SELECT CURRENT_USER()').collect()[0][0]}:"
        return header + "\n" + "\n".join(facts)

    except Exception as e:
        return f"Error retrieving memories: {str(e)}"
$$;


-- =============================================================================
-- Grant USAGE on procedures to the agent's role
-- =============================================================================
-- The agent runs under the role of the user who invokes it. If using
-- RESTAURANT_LOADER, these grants are implicit (owner = RESTAURANT_LOADER).
-- For multi-role setups, grant explicitly:
--
-- GRANT USAGE ON PROCEDURE RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY(VARCHAR, VARCHAR, VARCHAR)
--     TO ROLE <agent_user_role>;
-- GRANT USAGE ON PROCEDURE RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES(VARCHAR, INTEGER)
--     TO ROLE <agent_user_role>;


-- =============================================================================
-- Smoke tests — run these manually to verify before agent integration
-- =============================================================================

-- Test 1: Store a fact
CALL RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY(
    'home_address',
    '245 W 107th St, Manhattan',
    'location'
);
-- Expected: "Stored: home_address = '245 W 107th St, Manhattan' (category: location)"

-- Test 2: Verify the row and embedding exist
SELECT
    user_id,
    fact_key,
    fact_value,
    category,
    fact_embedding IS NOT NULL AS has_embedding,
    ROUND(SQRT(VECTOR_INNER_PRODUCT(fact_embedding, fact_embedding)), 4) AS embedding_norm,
    updated_at
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
WHERE user_id = CURRENT_USER();
-- Expected: 1 row, has_embedding = TRUE, embedding_norm > 0
-- Note: VECTOR_L2_NORM does not exist in Snowflake. The L2 norm is computed
-- as SQRT(VECTOR_INNER_PRODUCT(v, v)) — the square root of the dot product
-- of a vector with itself.

-- Test 3: Retrieve by similarity
CALL RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES(
    'restaurants near my home',
    5
);
-- Expected: returns home_address with high similarity score (> 0.5)

-- Test 4: Store a second fact and test retrieval ranking
CALL RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY(
    'favorite_cuisine',
    'Japanese',
    'preference'
);

CALL RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES(
    'what kind of food does the user like',
    5
);
-- Expected: favorite_cuisine ranked higher than home_address

-- Test 5: Upsert — update an existing fact
CALL RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY(
    'home_address',
    '500 8th Avenue, Midtown Manhattan',
    'location'
);

SELECT fact_value, updated_at
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
WHERE user_id = CURRENT_USER() AND fact_key = 'home_address';
-- Expected: fact_value updated, updated_at changed, still only 1 row

-- Cleanup smoke test data (optional)
-- DELETE FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
-- WHERE user_id = CURRENT_USER();
