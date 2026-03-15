# monitoring/ — Observability & Security (Phase 4)

This folder covers Phase 4: making the Cortex Agent production-ready. Two dimensions matter — **observability** (can you see what's happening and what it costs?) and **security** (can you trust who's accessing it?). The agent built in Phase 3 is a demo without both.

**Prerequisite:** Phase 3 complete. The agent must be deployed and answering questions before adding the observability layer.

---

## Files

| File | Purpose |
|------|---------|
| `monitoring_security.sql` | All cost monitoring queries, warehouse monitoring, authentication policy, SSO template, network policy template, and audit trail queries |

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

**Navigate to:** Snowsight → **Dashboards** → **+ Dashboard** → name it `Cortex Agent Monitoring`

Six tiles covering cost, performance, and security in one view.

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

### Tile 6 — Security Signal: Password Logins Without MFA (table)

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

> **Dashboard refresh:** Snowsight dashboards do not auto-refresh. Set a schedule via **Dashboard settings → Auto-refresh**. `ACCOUNT_USAGE` views have up to 45 minutes of latency — requests made this morning may not appear until mid-afternoon.

> **Upgrading to interactive date filters:** All six tile queries use hardcoded `DATEADD` ranges. Snowsight provides a built-in system filter `:daterange` that replaces these with an interactive date picker shared across all tiles. To enable it, replace `WHERE col >= DATEADD('day', -N, ...)` with `WHERE col = :daterange` in each query — using `=` (the only supported comparator). Columns must be `TIMESTAMP` type; `DATE` columns like `USAGE_DATE` require a cast: `USAGE_DATE::TIMESTAMP_NTZ = :daterange`. Once added to any tile query, a date range dropdown appears at the top of the dashboard automatically. A companion filter `:datebucket` can replace `DATE_TRUNC('day', col)` in GROUP BY clauses to let users toggle between daily, weekly, and monthly views. Both filters default to "Last day" — change the default to "Last 7 days" and select Save so all users open the dashboard on the same range. Ref: [Snowsight filter documentation](https://docs.snowflake.com/en/user-guide/ui-snowsight-filters).

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

## Security Checklist Before Production

- [ ] All human accounts enrolled in Duo MFA
- [ ] Authentication policy applied and verified (`SHOW AUTHENTICATION POLICIES`)
- [ ] Service accounts use key-pair authentication — no passwords
- [ ] Private key stored outside the project directory (not committed to Git)
- [ ] `GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` in place
- [ ] `CORTEX_AGENT_USAGE_HISTORY` returning rows (Step 1 verify query)
- [ ] Network policy configured for production IP ranges (if applicable)
- [ ] SSO integration configured (if IdP available)
- [ ] Zero rows in password-only user query above

---

## Design Notes

**Private key in production.** In this demo the `.pem` path is in `.env`. In production, retrieve the key at runtime from a secrets manager (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault) — the file disappears from the deployment artifact entirely.

**MFA does not affect the Python agent.** JWT key-pair authentication is explicitly excluded from the MFA requirement in the policy above. Enforcing MFA account-wide does not break programmatic access.