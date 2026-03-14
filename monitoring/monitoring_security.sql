-- =============================================================================
-- monitoring_security.sql
-- NYC Restaurant Intelligence — Phase 4: Observability & Security
-- =============================================================================
-- Run order:
--   STEP 1  — Grant ACCOUNT_USAGE access (ACCOUNTADMIN)
--   STEP 2  — Cost monitoring queries (RESTAURANT_LOADER or ACCOUNTADMIN)
--   STEP 3  — Authentication policy — MFA enforcement (ACCOUNTADMIN)
--   STEP 4  — SSO integration template (ACCOUNTADMIN)
--   STEP 5  — Network policy template (ACCOUNTADMIN)
--   STEP 6  — Audit trail queries
--
-- Prerequisites: Phase 3 complete — Cortex Agent deployed and returning answers.
-- =============================================================================


-- =============================================================================
-- STEP 1 — Grant ACCOUNT_USAGE access
-- Required for all cost and login history queries below.
-- Run as ACCOUNTADMIN.
-- =============================================================================

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE RESTAURANT_LOADER;


-- =============================================================================
-- STEP 2 — Cost monitoring
-- A Cortex Agent deployment has three distinct cost components tracked separately.
-- All three views must be queried for a complete picture.
-- =============================================================================

USE ROLE RESTAURANT_LOADER;   -- or ACCOUNTADMIN

-- 2A: Cortex Analyst — per message, not per token
-- Charges per successful response (HTTP 200).
-- When invoked through a Cortex Agent, token counts appear here.
-- Columns are REQUEST_COUNT and CREDITS — not TOKENS_INPUT / CREDITS_USED.
SELECT
    DATE_TRUNC('day', START_TIME)           AS usage_date,
    SUM(REQUEST_COUNT)                      AS total_messages,
    SUM(CREDITS)                            AS credits_consumed,
    ROUND(SUM(CREDITS) * 3.00, 4)          AS estimated_cost_usd  -- verify your contracted rate
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- 2B: Cortex Search — serving vs. embedding cost breakdown
-- Two cost types: embedding (index build/refresh) and serving (per query).
-- CONSUMPTION_TYPE values: 'serving' or 'embed_text_tokens'
SELECT
    USAGE_DATE,
    SERVICE_NAME,
    CONSUMPTION_TYPE,
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE DESC, SERVICE_NAME;


-- 2C: Invoice-level rollup — what aligns with your Snowflake bill
-- This is the number finance will ask about. Use SERVICE_TYPE filter.
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


-- 2D: Combined daily AI cost view (Snowsight dashboard tile)
-- Single query covering all three components for a unified cost trend chart.
SELECT
    USAGE_DATE,
    'cortex_analyst'  AS component,
    SUM(CREDITS_USED) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE = 'AI_SERVICES'
  AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2

UNION ALL

SELECT
    USAGE_DATE,
    LOWER(SERVICE_NAME) || '_search_' || LOWER(CONSUMPTION_TYPE),
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())

ORDER BY 1 DESC, 2;


-- =============================================================================
-- STEP 3 — Authentication policy: MFA enforcement
-- Mandates MFA enrollment before password-based login.
-- Key-pair authentication (Python agent / JWT) is explicitly excluded.
-- Run as ACCOUNTADMIN.
-- =============================================================================

-- ⚠️  Apply in a non-production account first.
--     Any service account still using password auth will be locked out
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

-- Apply account-wide
ALTER ACCOUNT SET AUTHENTICATION POLICY restaurant_production_auth_policy;

-- Verify
SHOW AUTHENTICATION POLICIES;


-- =============================================================================
-- STEP 4 — SSO integration template (SAML2)
-- For organisations with Okta, Azure AD, or Microsoft Entra.
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
-- STEP 5 — Network policy template
-- Restricts Snowflake access to known IP ranges.
-- Replace ALLOWED_IP_LIST with your office / CI-CD ranges before running.
-- Run as ACCOUNTADMIN.
-- =============================================================================

-- CREATE NETWORK POLICY restaurant_network_policy
--     ALLOWED_IP_LIST = ('203.0.113.0/24', '10.0.0.0/8')   -- replace with your ranges
--     BLOCKED_IP_LIST = ()
--     COMMENT         = 'Production network restriction for restaurant intelligence project';

-- ALTER ACCOUNT SET NETWORK_POLICY = restaurant_network_policy;


-- =============================================================================
-- STEP 6 — Audit trail queries
-- Use these to verify MFA enforcement and detect anomalous access patterns.
-- Run as RESTAURANT_LOADER or ACCOUNTADMIN.
-- =============================================================================

-- 6A: Password-only logins — no MFA second factor
-- Any rows here after the policy is applied = MFA not yet enforced for this user.
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


-- 6B: Users with password auth and no registered key pair
-- These accounts are relying solely on passwords. Remediate before production.
SELECT
    name,
    login_name,
    has_password,
    has_rsa_public_key,
    disabled
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE has_password      = TRUE
  AND has_rsa_public_key = FALSE
  AND disabled           = FALSE
ORDER BY name;


-- 6C: Key-pair verification for the agent service account
DESCRIBE USER RESTAURANT_LOADER;
-- RSA_PUBLIC_KEY_FP must be populated.
-- PASSWORD_LAST_SET_TIME should be NULL or very old for a pure service account.
