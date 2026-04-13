# monitoring/ — Observability & Security (Phase 4)

This folder covers Phase 4: making the Cortex Agent production-ready. Three dimensions matter — **observability** (can you see what's happening and what it costs?), **security** (can you trust who's accessing it?), and **quality** (are users getting useful answers?). The agent built in Phase 3 is a demo without all three.

**Prerequisite:** Phase 3 complete. The agent must be deployed and answering questions before adding the observability layer.

---

## Files

| File | Purpose |
|------|---------|
| `monitoring_security.sql` | Cost monitoring, warehouse monitoring, feedback monitoring, authentication policy, SSO template, network policy template, and audit trail queries |
| `../memory/02_setup_feedback_table.sql` | Creates `AGENT_FEEDBACK` table — run before deploying the Phase 5 Streamlit app |

---

## Part 1 — Cost Monitoring

**Grant required — run once as ACCOUNTADMIN:**
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE RESTAURANT_LOADER;
```

### CORTEX_AGENT_USAGE_HISTORY — the authoritative source

`CORTEX_AGENT_USAGE_HISTORY` (GA February 25, 2026) captures end-to-end agent cost in a single view: the orchestration LLM plus every Cortex Analyst tool call within each request.

```sql
SELECT
    DATE_TRUNC('day', START_TIME)                       AS usage_date,
    AGENT_NAME,
    COUNT(*)                                            AS total_requests,
    SUM(TOKEN_CREDITS)                                  AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 3.00, 4)                AS estimated_cost_usd,
    ROUND(SUM(TOKENS) / NULLIF(COUNT(*), 0), 0)        AS avg_tokens_per_request
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC;
```

> ⚠️ **Do not also query `CORTEX_ANALYST_USAGE_HISTORY`.** When Cortex Analyst is called through the agent, cost appears in `CORTEX_AGENT_USAGE_HISTORY` under `service_type = 'cortex_analyst'`. Querying both double-counts.

### Cortex Search — billed separately

```sql
SELECT
    USAGE_DATE,
    CONSUMPTION_TYPE,
    SUM(CREDITS) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC;
```

In normal operation only `SERVING` (per-query cost) appears. `embed_text_tokens` only shows up after a full reindex (`CREATE OR REPLACE CORTEX SEARCH SERVICE`).

### Invoice-level rollup

```sql
SELECT
    USAGE_DATE,
    SERVICE_TYPE,
    SUM(CREDITS_USED)                       AS credits_used,
    ROUND(SUM(CREDITS_USED) * 3.00, 4)     AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE IN ('AI_SERVICES', 'AI_INFERENCE')
  AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### Warehouse compute

AI credits are only one side of the bill. Every `dbt run`, ingestion load, and SQL query burns compute on `RESTAURANT_WH` — often more than the AI services themselves on a project like this.

```sql
-- Full cost picture: warehouse vs agent vs search
SELECT
    'warehouse_compute'                     AS cost_category,
    ROUND(SUM(CREDITS_USED), 6)             AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'RESTAURANT_WH'
UNION ALL
SELECT 'cortex_agent', ROUND(SUM(TOKEN_CREDITS), 6)
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'cortex_search', ROUND(SUM(CREDITS), 6)
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY total_credits DESC;
```

> For the NYC Restaurant Intelligence project running ~50 test questions, total daily AI spend stays well under $0.10. The value is the trendline — not the absolute number — when someone asks "what does this cost at 500 daily users?"

---

## Part 2 — Snowsight Dashboard

> ⚠️ **Legacy Dashboards are being retired.** Snowflake has announced the permanent removal of Legacy Dashboards from Snowsight:
> - **April 20, 2026** — creation of new dashboards is blocked
> - **June 22, 2026** — all existing dashboards are permanently removed
>
> **Recommended alternative:** Run the seven tile queries below as individual worksheets in Snowsight → **Projects → Worksheets**. Each query is self-contained and runs identically as a worksheet. For a persistent dashboard experience, deploy a Streamlit in Snowflake app (a starter Streamlit app is already set up in Phase 5) or migrate to a third-party BI tool of your choice. See [BCR-2260](https://docs.snowflake.com/en/release-notes/bcr-bundles/un-bundled/bcr-2260) for the official migration guide.

The seven tile queries below remain valid for worksheets or Streamlit apps. The chart type annotations are kept for reference if you migrate to another visualization layer.

---

### Tile 1 — Daily Agent Requests & Cost (bar chart)

*Is usage growing? Are costs tracking proportionally?*

New tile → Run → chart icon → **Bar chart** → X axis: `USAGE_DATE`, Y axis: `TOTAL_CREDITS`:

```sql
SELECT
    DATE_TRUNC('day', START_TIME)                   AS usage_date,
    COUNT(*)                                        AS total_requests,
    ROUND(SUM(TOKEN_CREDITS), 6)                    AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

---

### Tile 2 — 30-Day AI Cost by Service (scorecard)

*What is the total bill this month across all AI services?*

New tile → **Scorecard** → Value: `ESTIMATED_COST_USD`. Gives the invoice-aligned number finance will ask about:

```sql
SELECT
    SERVICE_TYPE,
    ROUND(SUM(CREDITS_USED), 6)             AS credits_used,
    ROUND(SUM(CREDITS_USED) * 3.00, 4)     AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE IN ('AI_SERVICES', 'AI_INFERENCE')
  AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 2 DESC;
```

---

### Tile 3 — Response Latency Trend (line chart)

*Is the agent getting slower? Are multi-turn SQL loops increasing response time?*

New tile → **Line chart** → X axis: `USAGE_DATE`, Y axis: `AVG_LATENCY_SEC`:

```sql
SELECT
    DATE_TRUNC('day', START_TIME)                           AS usage_date,
    ROUND(AVG(DATEDIFF('millisecond', START_TIME, END_TIME)) / 1000.0, 1)
                                                            AS avg_latency_sec,
    ROUND(MAX(DATEDIFF('millisecond', START_TIME, END_TIME)) / 1000.0, 1)
                                                            AS max_latency_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

---

### Tile 4 — Cortex Search Serving Cost (bar chart)

*What is document retrieval costing per day?*

New tile → **Bar chart** → X axis: `USAGE_DATE`, Y axis: `CREDITS`. Note: `embed_text_tokens` only appears after a reindex — during normal operation only `SERVING` rows are present.

```sql
SELECT
    USAGE_DATE,
    CONSUMPTION_TYPE,
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE, CONSUMPTION_TYPE;
```

---

### Tile 5 — Warehouse vs AI Cost Comparison (bar chart)

*Where are credits actually going — compute or AI services?*

This is usually the most surprising tile. New tile → **Bar chart** → X axis: `COST_CATEGORY`, Y axis: `TOTAL_CREDITS`:

```sql
SELECT
    'warehouse_compute'                     AS cost_category,
    ROUND(SUM(CREDITS_USED), 6)             AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'RESTAURANT_WH'
UNION ALL
SELECT 'cortex_agent', ROUND(SUM(TOKEN_CREDITS), 6)
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'cortex_search', ROUND(SUM(CREDITS), 6)
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY total_credits DESC;
```

---

### Tile 6 — Daily Satisfaction Rate (line chart)

*Is answer quality improving or degrading over time?*

New tile → **Line chart** → X axis: `FEEDBACK_DATE`, Y axis: `SATISFACTION_PCT`. Overlay `TOTAL_RATINGS` as a secondary series to distinguish a bad day from a day with one rating:

```sql
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
ORDER BY 1;
```

---

### Tile 7 — Security Signal: Password Logins Without MFA (table)

*Are all users covered by the authentication policy?*

New tile → **Table** view. Zero rows = policy is working. Any row = an account not yet enrolled in MFA:

```sql
SELECT
    USER_NAME,
    COUNT(*)                    AS login_attempts,
    MAX(EVENT_TIMESTAMP)        AS last_attempt,
    BOOLOR_AGG(NOT IS_SUCCESS)  AS had_failure
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND FIRST_AUTHENTICATION_FACTOR  = 'PASSWORD'
  AND SECOND_AUTHENTICATION_FACTOR IS NULL
GROUP BY 1
ORDER BY 2 DESC;
```

---

> **ACCOUNT_USAGE latency:** Views like `CORTEX_AGENT_USAGE_HISTORY` have up to 45 minutes of latency — requests made this morning may not appear until mid-afternoon. When running these as worksheets, allow for this lag before concluding a query returned no results.

---

## Part 3 — Authentication Security

### Two access paths, two risk profiles

| Path | Method | Risk Level | Production Ready? |
|------|--------|------------|-------------------|
| Python agent | JWT key-pair | Low — private key never transmitted | ✅ Yes |
| Snowsight | Username + password | High without MFA | ⚠️ Only with MFA enforced |

### Enforce MFA — one SQL statement most teams never run

```sql
-- Run as ACCOUNTADMIN
CREATE AUTHENTICATION POLICY IF NOT EXISTS restaurant_production_auth_policy
    AUTHENTICATION_METHODS      = ('PASSWORD', 'KEYPAIR')
    MFA_AUTHENTICATION_METHODS  = ('PASSWORD')
    MFA_ENROLLMENT              = REQUIRED
    CLIENT_TYPES                = ('SNOWFLAKE_UI', 'DRIVERS', 'SNOWSQL')
    COMMENT = 'MFA required for UI access, keypair allowed for service accounts';

ALTER ACCOUNT SET AUTHENTICATION POLICY restaurant_production_auth_policy;
SHOW AUTHENTICATION POLICIES;
```

`MFA_AUTHENTICATION_METHODS = ('PASSWORD')` means MFA is required for password logins only. Key-pair authentication (the Python agent) is unaffected.

> ⚠️ Apply in a non-production account first. Any service account still using password authentication will be locked out. Verify all service accounts use key-pair before enforcing account-wide.

### SSO integration (template)

```sql
CREATE SECURITY INTEGRATION snowflake_sso_integration
    TYPE                = SAML2
    ENABLED             = TRUE
    SAML2_ISSUER        = 'https://your-idp.example.com'
    SAML2_SSO_URL       = 'https://your-idp.example.com/sso'
    SAML2_PROVIDER      = 'OKTA'
    SAML2_X509_CERT     = '...'
    SAML2_SP_INITIATED_LOGIN_PAGE_LABEL = 'Corporate SSO';
```

### Network policy (template)

```sql
CREATE NETWORK POLICY restaurant_network_policy
    ALLOWED_IP_LIST = ('203.0.113.0/24', '10.0.0.0/8')
    BLOCKED_IP_LIST = ()
    COMMENT         = 'Production network restriction';

ALTER ACCOUNT SET NETWORK_POLICY = restaurant_network_policy;
```

---

## Part 4 — Audit Trail

```sql
-- Password logins without MFA
SELECT USER_NAME, EVENT_TIMESTAMP, REPORTED_CLIENT_TYPE, IS_SUCCESS, CLIENT_IP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND FIRST_AUTHENTICATION_FACTOR  = 'PASSWORD'
  AND SECOND_AUTHENTICATION_FACTOR IS NULL
ORDER BY EVENT_TIMESTAMP DESC;

-- Users with password auth and no key pair — remediate before production
SELECT name, login_name, has_password, has_rsa_public_key, disabled
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE has_password = TRUE AND has_rsa_public_key = FALSE AND disabled = FALSE
ORDER BY name;
```

---

## Part 5 — Feedback Monitoring

User feedback closes the loop between cost data and answer quality. A session that consumed 10,000 tokens is fine if the user rated it positively — and a problem if they didn't.

### Two feedback sources

| Source | What it captures | When it's populated |
|--------|-----------------|---------------------|
| `AGENT_FEEDBACK` (custom) | Thumbs up/down from the Streamlit app, with full question and answer text | Immediately, on every button click |
| `GET_AI_OBSERVABILITY_EVENTS` (platform) | Native Cortex Agent feedback events | Only when the agent is accessed via the Snowsight Intelligence UI, not the custom SiS app |
| `CORTEX_ANALYST_REQUESTS_V.feedback` (platform) | Feedback on individual Cortex Analyst sub-requests | 1–2 minute lag; only when a rating is attached to an analyst call |

The custom table is the primary signal for this project. The platform sources become relevant if the agent is also exposed via Snowsight.

### Grants required (run as ACCOUNTADMIN)

```sql
-- For GET_AI_OBSERVABILITY_EVENTS and CORTEX_ANALYST_REQUESTS_V
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;

-- For native Cortex Agent feedback events
GRANT MONITOR ON AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
  TO ROLE RESTAURANT_LOADER;

-- For Cortex Analyst request history
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_REQUESTS_VIEWER
  TO ROLE RESTAURANT_LOADER;
```

### Overall satisfaction rate

```sql
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
```

### Reviewing thumbs-down answers

The most direct use of the feedback table: pull every negatively-rated turn and read the question and answer together.

```sql
SELECT
    created_at,
    user_id,
    question,
    answer
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE rating = 'down'
ORDER BY created_at DESC
LIMIT 100;
```

Common failure patterns to look for:
- **Wrong row count** — agent said "358 restaurants" when it meant 358 violation records
- **Stale data** — answer cited an inspection date months older than `MAX(inspection_date)` in the dataset
- **Tool misrouting** — a penalty question went to Cortex Analyst instead of Cortex Search
- **Proximity radius** — answer used a 1-mile radius when the rule says 0.5 miles

### Systematic gaps — clustered bad questions

Groups near-duplicate questions that all received thumbs-down. Reveals categories of failure rather than one-off issues:

```sql
SELECT
    LEFT(TRIM(question), 80)                                        AS question_prefix,
    COUNT(*)                                                        AS negative_ratings,
    COUNT(DISTINCT user_id)                                         AS distinct_users,
    MAX(created_at)                                                 AS last_seen
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK
WHERE rating = 'down'
  AND question IS NOT NULL
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY negative_ratings DESC;
```

### Cost × quality cross-reference

Expensive sessions that also produced bad answers are the highest-priority fixes — they waste credits and disappoint users.

```sql
SELECT
    f.user_id,
    f.session_id,
    DATE_TRUNC('hour', f.created_at)                               AS session_hour,
    SUM(CASE WHEN f.rating = 'down' THEN 1 ELSE 0 END)            AS thumbs_down,
    ROUND(
        100.0 * SUM(CASE WHEN f.rating = 'up' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    )                                                              AS satisfaction_pct,
    ROUND(COALESCE(SUM(u.TOKEN_CREDITS), 0) * 3.00, 4)            AS estimated_cost_usd
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_FEEDBACK f
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY u
    ON  u.USER_NAME  = f.user_id
    AND DATE_TRUNC('hour', u.START_TIME) = DATE_TRUNC('hour', f.created_at)
WHERE f.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY thumbs_down DESC, estimated_cost_usd DESC;
```

> **session_id vs agent request_id:** The Streamlit app generates its own `session_id` (UUID). This does not appear in `CORTEX_AGENT_USAGE_HISTORY`, which uses Snowflake-assigned request UUIDs. The join above uses `USER_NAME + DATE_TRUNC('hour', ...)` as a best-effort time-window match, not an exact key.

### Platform-level feedback (Snowsight UI only)

```sql
SELECT
    TIMESTAMP                                                       AS event_time,
    RECORD_ATTRIBUTES:user_id::STRING                              AS user_id,
    RECORD_ATTRIBUTES:feedback::STRING                             AS sentiment,
    RECORD_ATTRIBUTES                                              AS raw_attributes
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    'RESTAURANT_INTELLIGENCE',
    'MARTS',
    'NYC_RESTAURANT_AGENT',
    'CORTEX AGENT'
))
WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
ORDER BY TIMESTAMP DESC;
```

### Cortex Analyst sub-request feedback

Isolates whether negative ratings are specifically caused by SQL generation, rather than the orchestration or search layers:

```sql
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
ORDER BY timestamp DESC;
```

---

## Security Checklist Before Production

**Cost & observability**
- [ ] `GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` in place
- [ ] `CORTEX_AGENT_USAGE_HISTORY` returning rows (Step 2 verify query)
- [ ] Network policy configured for production IP ranges (if applicable)

**Feedback monitoring**
- [ ] `memory/02_setup_feedback_table.sql` executed — `AGENT_FEEDBACK` table exists
- [ ] Streamlit app deployed with thumbs up/down buttons visible
- [ ] Feedback grants applied if using platform observability (`CORTEX_USER`, `MONITOR ON AGENT`, `CORTEX_ANALYST_REQUESTS_VIEWER`)
- [ ] At least one test rating recorded and visible in `AGENT_FEEDBACK`

**Security**
- [ ] All human accounts enrolled in Duo MFA
- [ ] Authentication policy applied and verified (`SHOW AUTHENTICATION POLICIES`)
- [ ] Service accounts use key-pair authentication — no passwords
- [ ] Private key stored outside the project directory (not committed to Git)
- [ ] SSO integration configured (if IdP available)
- [ ] Zero rows in password-only user query above

---

## Design Notes

**Private key in production.** In this demo the `.pem` path is in `.env`. In production, retrieve the key at runtime from a secrets manager (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault) — the file disappears from the deployment artifact entirely.

**MFA does not affect the Python agent.** JWT key-pair authentication is explicitly excluded from the MFA requirement in the policy above. Enforcing MFA account-wide does not break programmatic access.