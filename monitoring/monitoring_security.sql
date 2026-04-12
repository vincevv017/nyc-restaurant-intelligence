-- =============================================================================
-- monitoring_security.sql
-- NYC Restaurant Intelligence — Phase 4: Observability & Security
-- =============================================================================
-- Run order:
--   STEP 1  — Grant ACCOUNT_USAGE access (ACCOUNTADMIN)
--   STEP 2  — Cortex Agent cost (CORTEX_AGENT_USAGE_HISTORY — primary source)
--   STEP 3  — Cortex Search cost (CORTEX_SEARCH_DAILY_USAGE_HISTORY)
--   STEP 4  — Invoice-level AI cost rollup (METERING_DAILY_HISTORY)
--   STEP 5  — Warehouse compute cost (WAREHOUSE_METERING_HISTORY)
--   STEP 6  — Authentication policy — MFA enforcement (ACCOUNTADMIN)
--   STEP 7  — SSO integration template (ACCOUNTADMIN)
--   STEP 8  — Network policy template (ACCOUNTADMIN)
--   STEP 9  — Audit trail queries
--
-- Prerequisites: Phase 3 complete — Cortex Agent deployed and returning answers.
--
-- Billing architecture for this project:
--   CORTEX_AGENT_USAGE_HISTORY     → agent orchestration + Cortex Analyst calls
--   CORTEX_SEARCH_DAILY_USAGE_HISTORY → search serving + index embedding (separate)
--   METERING_DAILY_HISTORY         → invoice-aligned rollup across all AI services
--   WAREHOUSE_METERING_HISTORY     → compute credits for dbt, ingestion, SQL queries
--
-- Note on CORTEX_ANALYST_USAGE_HISTORY: NOT used here. When Cortex Analyst runs
--   inside the agent, cost appears in CORTEX_AGENT_USAGE_HISTORY. Querying both
--   double-counts. Only relevant for standalone Cortex Analyst outside the agent.
--
-- Ref: https://docs.snowflake.com/en/sql-reference/account-usage/cortex_agent_usage_history
--      (GA February 25, 2026)
-- =============================================================================


-- =============================================================================
-- STEP 1 — Grant ACCOUNT_USAGE access
-- Required for all queries below. Run as ACCOUNTADMIN.
--
-- Verify it works after granting:
--   SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY;
--   0 rows with no error = grant OK but no agent requests logged yet (45 min latency).
-- =============================================================================

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE RESTAURANT_LOADER;

USE ROLE RESTAURANT_LOADER;


-- =============================================================================
-- STEP 2 — Cortex Agent cost: the authoritative end-to-end source
-- Each row = one agent request. Includes orchestration LLM + all tool calls.
-- Does NOT include Snowflake Intelligence requests.
-- =============================================================================

-- 2A: Daily agent cost summary — the primary operational metric
SELECT
    DATE_TRUNC('day', START_TIME)                       AS usage_date,
    AGENT_NAME,
    COUNT(*)                                            AS total_requests,
    SUM(TOKENS)                                         AS total_tokens,
    SUM(TOKEN_CREDITS)                                  AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 3.00, 4)                AS estimated_cost_usd,
    ROUND(SUM(TOKENS) / NULLIF(COUNT(*), 0), 0)        AS avg_tokens_per_request
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- 2B: Cost split by service type — orchestration LLM vs Cortex Analyst tool calls
-- CREDITS_GRANULAR structure:
--   ARRAY of objects, each object = { "request_uuid": { "service_type": { "model": {credits} }, "start_time": "..." } }
-- Four flatten levels required: array → request UUID → service_type → model
-- Filter out 'start_time' key which appears alongside service_type keys at the same level.
SELECT
    DATE_TRUNC('day', h.START_TIME)                     AS usage_date,
    svc.key                                             AS service_type,
    mdl.key                                             AS model,
    SUM(COALESCE(mdl.value:input::NUMBER, 0)
      + COALESCE(mdl.value:output::NUMBER, 0)
      + COALESCE(mdl.value:cache_read_input::NUMBER, 0)
      + COALESCE(mdl.value:cache_write_input::NUMBER, 0)) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY h,
     LATERAL FLATTEN(input => h.CREDITS_GRANULAR)      arr,  -- array element
     LATERAL FLATTEN(input => arr.value)               req,  -- req.key = request UUID
     LATERAL FLATTEN(input => req.value)               svc,  -- svc.key = service_type or 'start_time'
     LATERAL FLATTEN(input => svc.value)               mdl   -- mdl.key = model name
WHERE h.START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND svc.key != 'start_time'                               -- exclude the timestamp field
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;


-- 2C: Response latency per day — spot slow multi-turn SQL loops
SELECT
    DATE_TRUNC('day', START_TIME)                               AS usage_date,
    COUNT(*)                                                    AS requests,
    ROUND(AVG(DATEDIFF('millisecond', START_TIME, END_TIME)) / 1000.0, 1)
                                                                AS avg_latency_sec,
    ROUND(MAX(DATEDIFF('millisecond', START_TIME, END_TIME)) / 1000.0, 1)
                                                                AS max_latency_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- 2D: Per-user request volume and cost
SELECT
    USER_NAME,
    COUNT(*)                                    AS total_requests,
    SUM(TOKEN_CREDITS)                          AS credits_consumed,
    ROUND(SUM(TOKEN_CREDITS) * 3.00, 4)        AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 3 DESC;


-- =============================================================================
-- STEP 3 — Cortex Search cost: billed separately from agent
-- CONSUMPTION_TYPE:
--   'SERVING'           → charged per query (every agent request that uses Cortex Search)
--   'embed_text_tokens' → charged when the index is built or rebuilt (CREATE OR REPLACE)
--                         Only appears after a reindex — not visible during normal operation.
-- =============================================================================

SELECT
    USAGE_DATE,
    SERVICE_NAME,
    CONSUMPTION_TYPE,
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE DESC, SERVICE_NAME;


-- =============================================================================
-- STEP 4 — Invoice-level AI cost rollup
-- The number that appears on your Snowflake invoice.
-- Covers all AI services: agent + search + any other Cortex functions.
-- This is what finance will ask about.
-- =============================================================================

SELECT
    USAGE_DATE,
    SERVICE_TYPE,
    SUM(CREDITS_USED)                       AS credits_used,
    ROUND(SUM(CREDITS_USED) * 3.00, 4)     AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE IN ('AI_SERVICES', 'AI_INFERENCE')
  AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- =============================================================================
-- STEP 5 — Warehouse compute cost
-- AI credits are only one side of the bill. Every dbt run, ingestion load,
-- and SQL query against the star schema burns RESTAURANT_WH credits.
-- This query shows where compute actually goes — often more than AI services
-- on a project like this.
-- =============================================================================

-- 5A: Daily warehouse usage for this project's warehouse
SELECT
    DATE_TRUNC('day', START_TIME)       AS usage_date,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED)                   AS credits_used,
    ROUND(SUM(CREDITS_USED) * 3.00, 4) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'RESTAURANT_WH'
GROUP BY 1, 2
ORDER BY 1 DESC;


-- 5B: Total compute vs AI cost comparison — the full picture
SELECT
    'warehouse_compute'             AS cost_category,
    ROUND(SUM(CREDITS_USED), 6)     AS total_credits,
    ROUND(SUM(CREDITS_USED) * 3.00, 4) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'RESTAURANT_WH'

UNION ALL

SELECT
    'cortex_agent'                  AS cost_category,
    ROUND(SUM(TOKEN_CREDITS), 6)    AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 3.00, 4) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())

UNION ALL

SELECT
    'cortex_search'                 AS cost_category,
    ROUND(SUM(CREDITS), 6)          AS total_credits,
    ROUND(SUM(CREDITS) * 3.00, 4)   AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())

ORDER BY total_credits DESC;


-- =============================================================================
-- STEP 6 — Authentication policy: MFA enforcement
-- Mandates MFA enrollment before password-based login.
-- Key-pair authentication (Python agent / JWT) is explicitly excluded.
-- Run as ACCOUNTADMIN.
-- =============================================================================

-- ⚠️  Apply in a non-production account first.
--     Service accounts still using password auth will be locked out
--     after Duo enrollment becomes mandatory. Verify all service accounts
--     use key-pair before enforcing account-wide.

CREATE AUTHENTICATION POLICY IF NOT EXISTS restaurant_production_auth_policy
    AUTHENTICATION_METHODS      = ('PASSWORD', 'KEYPAIR')
    MFA_AUTHENTICATION_METHODS  = ('PASSWORD')               -- MFA required for password only
    MFA_ENROLLMENT              = REQUIRED                   -- block login if not enrolled
    CLIENT_TYPES                = ('SNOWFLAKE_UI',
                                   'DRIVERS',
                                   'SNOWSQL')
    COMMENT = 'Production auth policy: MFA required for UI access, keypair allowed for service accounts';

ALTER ACCOUNT SET AUTHENTICATION POLICY restaurant_production_auth_policy;
SHOW AUTHENTICATION POLICIES;


-- =============================================================================
-- STEP 7 — SSO integration template (SAML2)
-- Replace placeholder values with your IdP metadata before running.
-- Run as ACCOUNTADMIN.
-- =============================================================================

-- CREATE SECURITY INTEGRATION snowflake_sso_integration
--     TYPE                = SAML2
--     ENABLED             = TRUE
--     SAML2_ISSUER        = 'https://your-idp.example.com'
--     SAML2_SSO_URL       = 'https://your-idp.example.com/sso'
--     SAML2_PROVIDER      = 'OKTA'                          -- or 'ADFS', 'CUSTOM'
--     SAML2_X509_CERT     = '...'                           -- IdP signing certificate
--     SAML2_SP_INITIATED_LOGIN_PAGE_LABEL = 'Corporate SSO';


-- =============================================================================
-- STEP 8 — Network policy template
-- Replace ALLOWED_IP_LIST with your office / CI-CD ranges before running.
-- Run as ACCOUNTADMIN.
-- =============================================================================

-- CREATE NETWORK POLICY restaurant_network_policy
--     ALLOWED_IP_LIST = ('203.0.113.0/24', '10.0.0.0/8')
--     BLOCKED_IP_LIST = ()
--     COMMENT         = 'Production network restriction for restaurant intelligence project';

-- ALTER ACCOUNT SET NETWORK_POLICY = restaurant_network_policy;


-- =============================================================================
-- STEP 9 — Audit trail queries
-- =============================================================================

-- 9A: Password-only logins — no MFA second factor
SELECT
    USER_NAME,
    EVENT_TIMESTAMP,
    REPORTED_CLIENT_TYPE,
    IS_SUCCESS,
    ERROR_MESSAGE,
    CLIENT_IP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND FIRST_AUTHENTICATION_FACTOR  = 'PASSWORD'
  AND SECOND_AUTHENTICATION_FACTOR IS NULL
ORDER BY EVENT_TIMESTAMP DESC;


-- 9B: Users with password auth and no registered key pair
SELECT
    name,
    login_name,
    has_password,
    has_rsa_public_key,
    disabled
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE has_password       = TRUE
  AND has_rsa_public_key = FALSE
  AND disabled           = FALSE
ORDER BY name;


-- 9C: Key-pair verification for the agent user
DESCRIBE USER RESTAURANT_LOADER;
-- RSA_PUBLIC_KEY_FP must be populated.


-- =============================================================================
-- STEP 10 — Feedback monitoring
--
-- Two complementary sources:
--
--   A) AGENT_FEEDBACK (custom)
--      Thumbs up/down captured by the Streamlit app (Phase 5).
--      Stores question + full answer + rating. Immediately queryable.
--      Created by: memory/02_setup_feedback_table.sql
--
--   B) SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS (platform)
--      Native Cortex Agent feedback events from the Snowflake Intelligence
--      interface. Requires AI Observability to be enabled and the agent
--      accessed through the platform UI (not the custom SiS app).
--      Useful as a secondary signal if the agent is exposed via Snowsight.
--
--   C) SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS_V (platform)
--      Per-request Cortex Analyst feedback. The 'feedback' column carries
--      any rating attached to an analyst sub-request inside the agent turn.
--      Separate grant required (see 10A).
--
-- Note on session_id vs agent request correlation:
--   Our session_id (UUID, client-generated) is not present in
--   CORTEX_AGENT_USAGE_HISTORY. Correlation with cost uses USER_NAME +
--   DATE_TRUNC('hour', ...) as a best-effort time window join.
-- =============================================================================


-- =============================================================================
-- STEP 10A — Grants (run as ACCOUNTADMIN)
-- =============================================================================

-- Required for GET_AI_OBSERVABILITY_EVENTS and CORTEX_ANALYST_REQUESTS_V
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;

-- Required to read Cortex Agent observability events for our specific agent
-- GRANT MONITOR ON AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
--   TO ROLE RESTAURANT_LOADER;

-- Required for CORTEX_ANALYST_REQUESTS_V (read-only analyst request history)
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_REQUESTS_VIEWER
--   TO ROLE RESTAURANT_LOADER;


-- =============================================================================
-- STEP 10B — Custom feedback: overall satisfaction rate (last 30 days)
-- The headline metric: what % of answers were rated positively?
-- =============================================================================

SELECT
    COUNT(*)                                                         AS total_ratings,
    SUM(CASE WHEN rating = 'up'   THEN 1 ELSE 0 END)                AS thumbs_up,
    SUM(CASE WHEN rating = 'down' THEN 1 ELSE 0 END)                AS thumbs_down,
    ROUND(
        100.0 * SUM(CASE WHEN rating = 'up' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    )                                                                AS satisfaction_pct
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP());


-- =============================================================================
-- STEP 10C — Custom feedback: daily trend
-- Spot days with a spike in negative ratings — correlate with deploys or
-- data changes in the ingestion pipeline.
-- =============================================================================

SELECT
    DATE_TRUNC('day', created_at)                                   AS feedback_date,
    COUNT(*)                                                        AS total_ratings,
    SUM(CASE WHEN rating = 'up'   THEN 1 ELSE 0 END)               AS thumbs_up,
    SUM(CASE WHEN rating = 'down' THEN 1 ELSE 0 END)               AS thumbs_down,
    ROUND(
        100.0 * SUM(CASE WHEN rating = 'up' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    )                                                               AS satisfaction_pct
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- =============================================================================
-- STEP 10D — Custom feedback: satisfaction rate per user
-- Identifies users who are consistently unhappy — likely hitting a specific
-- gap in the semantic view or health code document coverage.
-- =============================================================================

SELECT
    user_id,
    COUNT(*)                                                        AS total_ratings,
    SUM(CASE WHEN rating = 'up'   THEN 1 ELSE 0 END)               AS thumbs_up,
    SUM(CASE WHEN rating = 'down' THEN 1 ELSE 0 END)               AS thumbs_down,
    ROUND(
        100.0 * SUM(CASE WHEN rating = 'up' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    )                                                               AS satisfaction_pct
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
HAVING COUNT(*) >= 3                        -- exclude users with very few ratings
ORDER BY satisfaction_pct ASC, total_ratings DESC;


-- =============================================================================
-- STEP 10E — Custom feedback: all thumbs-down questions, most recent first
-- The primary list to review when improving the agent.
-- Low-rated questions reveal: missing data, misrouted tool calls,
-- poor answer formatting, or hallucinated facts.
-- =============================================================================

SELECT
    created_at,
    user_id,
    session_id,
    turn_index,
    question,
    answer,
    DATEDIFF('second',
        LAG(created_at) OVER (PARTITION BY session_id ORDER BY turn_index),
        created_at
    )                                                               AS seconds_since_prev_turn
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE rating = 'down'
ORDER BY created_at DESC
LIMIT 100;


-- =============================================================================
-- STEP 10F — Custom feedback: worst-rated questions by text similarity cluster
-- Groups near-duplicate questions that all received thumbs-down.
-- Reveals systematic gaps (e.g. all proximity queries fail, or penalty
-- questions always get negative feedback).
-- Simple version: group by LEFT(question, 60) to bucket common question stems.
-- =============================================================================

SELECT
    LEFT(TRIM(question), 80)                                        AS question_prefix,
    COUNT(*)                                                        AS negative_ratings,
    MIN(created_at)                                                 AS first_seen,
    MAX(created_at)                                                 AS last_seen,
    COUNT(DISTINCT user_id)                                         AS distinct_users
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE rating = 'down'
  AND question IS NOT NULL
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY negative_ratings DESC, last_seen DESC;


-- =============================================================================
-- STEP 10G — Custom feedback: cross-reference with agent cost
-- For each session that produced negative feedback, show the token cost
-- of that session's agent calls (best-effort: matched on user + hour window).
-- Useful to spot expensive sessions that also produced bad answers —
-- a compound problem worth fixing first.
-- =============================================================================

SELECT
    f.user_id,
    f.session_id,
    DATE_TRUNC('hour', f.created_at)                               AS session_hour,
    COUNT(f.feedback_id)                                           AS rated_turns,
    SUM(CASE WHEN f.rating = 'down' THEN 1 ELSE 0 END)            AS thumbs_down,
    ROUND(
        100.0 * SUM(CASE WHEN f.rating = 'up' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    )                                                              AS satisfaction_pct,
    COALESCE(SUM(u.TOKENS), 0)                                     AS tokens_in_window,
    ROUND(COALESCE(SUM(u.TOKEN_CREDITS), 0) * 3.00, 4)            AS estimated_cost_usd
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK f
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY u
    ON  u.USER_NAME  = f.user_id
    AND DATE_TRUNC('hour', u.START_TIME) = DATE_TRUNC('hour', f.created_at)
WHERE f.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY thumbs_down DESC, tokens_in_window DESC;


-- =============================================================================
-- STEP 10H — Platform feedback: Cortex Agent observability events
-- Reads native CORTEX_AGENT_FEEDBACK events written by the Snowflake platform
-- when the agent is used through the Snowsight Intelligence interface.
-- RECORD_ATTRIBUTES contains the feedback payload and sentiment.
--
-- Prerequisites: STEP 10A grants must be applied.
-- Note: only populated when the agent is accessed via the platform UI,
--       NOT through our custom SiS app (which uses the REST API directly).
-- =============================================================================

-- 10H-i: Raw feedback events — inspect RECORD_ATTRIBUTES structure first
SELECT
    TIMESTAMP                                                       AS event_time,
    RECORD_ATTRIBUTES:user_id::STRING                              AS user_id,
    RECORD_ATTRIBUTES:feedback::STRING                             AS feedback_sentiment,
    RECORD_ATTRIBUTES                                              AS raw_attributes
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    'RESTAURANT_INTELLIGENCE',
    'MARTS',
    'NYC_RESTAURANT_AGENT',
    'CORTEX AGENT'
))
WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
ORDER BY TIMESTAMP DESC
LIMIT 50;

-- 10H-ii: Platform feedback summary — positive vs negative count
SELECT
    DATE_TRUNC('day', TIMESTAMP)                                   AS event_date,
    RECORD_ATTRIBUTES:feedback::STRING                             AS sentiment,
    COUNT(*)                                                       AS count
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    'RESTAURANT_INTELLIGENCE',
    'MARTS',
    'NYC_RESTAURANT_AGENT',
    'CORTEX AGENT'
))
WHERE RECORD:name       = 'CORTEX_AGENT_FEEDBACK'
  AND TIMESTAMP        >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- =============================================================================
-- STEP 10I — Cortex Analyst request-level feedback
-- CORTEX_ANALYST_REQUESTS_V.feedback is an ARRAY of feedback objects
-- attached to individual Cortex Analyst sub-requests made by the agent.
-- Useful to isolate whether negative feedback is due to SQL generation
-- specifically (rather than the orchestration or search layers).
--
-- Prerequisites: STEP 10A grants must be applied.
-- 1–2 minute lag between request and visibility in the view.
-- =============================================================================

-- 10I-i: Requests that received explicit feedback
SELECT
    timestamp,
    user_id,
    semantic_model_name,
    latest_question,
    generated_sql,
    response_status_code,
    feedback
FROM SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS_V
WHERE feedback IS NOT NULL
  AND ARRAY_SIZE(feedback) > 0
  AND timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC;

-- 10I-ii: Analyst requests that came from our agent (linked via agent_request_id)
-- source column = JSON object with agent_request_id field when request
-- originated from a Cortex Agent tool call.
SELECT
    timestamp,
    user_id,
    latest_question,
    generated_sql,
    source:agent_request_id::STRING                                AS agent_request_id,
    response_status_code,
    feedback
FROM SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS_V
WHERE source:agent_request_id IS NOT NULL
  AND timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY timestamp DESC
LIMIT 200;