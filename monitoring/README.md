# monitoring/ — Observability & Security (Phase 4)

This folder covers Phase 4: making the Cortex Agent production-ready. Two dimensions matter — **observability** (can you see what's happening and what it costs?) and **security** (can you trust who's accessing it?). The agent built in Phase 3 is a demo without both.

**Prerequisite:** Phase 3 complete. The agent must be deployed and answering questions before adding the observability layer.

---

## Files

| File | Purpose |
|------|---------|
| `monitoring_security.sql` | Creates `AGENT_INTERACTIONS` table, authentication policy (MFA enforcement), SSO integration template, network policy template, and all cost monitoring queries |

---

## Part 1 — Cost Monitoring

A Cortex Agent deployment has three distinct cost components, each tracked in a separate Snowflake view. Querying only one gives you an incomplete picture.

**Grant required — run as ACCOUNTADMIN:**
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE RESTAURANT_LOADER;
```

### Cortex Analyst — per message, not per token

Cortex Analyst charges per successful response (HTTP 200). When invoked through a Cortex Agent, token counts appear in the view — standalone usage bills per request regardless of token count.

```sql
-- Run as RESTAURANT_LOADER or ACCOUNTADMIN
SELECT
    DATE_TRUNC('day', START_TIME)           AS usage_date,
    SUM(REQUEST_COUNT)                      AS total_messages,
    SUM(CREDITS)                            AS credits_consumed,
    ROUND(SUM(CREDITS) * 3.00, 4)          AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;
```

### Cortex Search — serving vs. embedding

Two cost types: embedding (charged when the index is built or refreshed) and serving (charged per query). Both appear in the same view.

```sql
SELECT
    USAGE_DATE,
    SERVICE_NAME,
    CONSUMPTION_TYPE,       -- 'serving' or 'embed_text_tokens'
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE DESC, SERVICE_NAME;
```

### Invoice-level rollup

The number that aligns with your actual Snowflake invoice and what finance will ask about:

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
ORDER BY 1 DESC, 2;
```

> For the NYC Restaurant Intelligence project running ~50 test questions, total daily AI spend stays well under $0.10. The value is the trendline — not the absolute number — when someone asks "what does this cost at 500 daily users?"

---

## Part 2 — Snowsight Cost Dashboard

The three cost queries above can be pinned as tiles in a Snowsight dashboard, giving you a live cost view without running queries manually. Here is how to build it.

**Navigate to:** Snowsight → **Dashboards** → **+ Dashboard** → name it `AI Cost Monitoring`

### Tile 1 — Daily Cortex Analyst Usage (bar chart)

New tile → paste the query below → Run → click the chart icon → Bar chart → X axis: `USAGE_DATE`, Y axis: `CREDITS_CONSUMED`:

```sql
SELECT
    DATE_TRUNC('day', START_TIME)  AS usage_date,
    SUM(REQUEST_COUNT)             AS total_messages,
    SUM(CREDITS)                   AS credits_consumed
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

### Tile 2 — Cortex Search Cost Breakdown (bar chart, stacked by type)

New tile → paste query → Bar chart → X axis: `USAGE_DATE`, Y axis: `CREDITS`, Color: `CONSUMPTION_TYPE`:

```sql
SELECT
    USAGE_DATE,
    CONSUMPTION_TYPE,
    SUM(CREDITS) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1;
```

### Tile 3 — Invoice-Aligned AI Spend (scorecard)

New tile → paste query → Scorecard → Value: `CREDITS_USED`. This gives you the 30-day total that aligns with your Snowflake invoice:

```sql
SELECT
    ROUND(SUM(CREDITS_USED), 4)             AS credits_used,
    ROUND(SUM(CREDITS_USED) * 3.00, 2)     AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE IN ('AI_SERVICES', 'AI_INFERENCE')
  AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE());
```

### Tile 4 — Password-Only Logins (security signal)

New tile → paste query → Table view. Any rows here after MFA enforcement is applied indicate accounts not yet covered by the policy:

```sql
SELECT
    USER_NAME,
    COUNT(*)        AS login_attempts,
    MAX(EVENT_TIMESTAMP) AS last_seen,
    MIN(IS_SUCCESS::VARCHAR) AS had_failure
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND FIRST_AUTHENTICATION_FACTOR  = 'PASSWORD'
  AND SECOND_AUTHENTICATION_FACTOR IS NULL
GROUP BY 1
ORDER BY 2 DESC;
```

> **Dashboard refresh:** Snowsight dashboards do not auto-refresh. Use the refresh button manually, or set a schedule via **Dashboard settings → Auto-refresh**. `ACCOUNT_USAGE` views have a latency of up to 45 minutes — cost data from this morning may not appear until mid-afternoon.

---

## Part 3 — Authentication Security

### Two access paths, two risk profiles

| Path | Method | Risk Level | Production Ready? |
|------|--------|------------|-------------------|
| Python agent | JWT key-pair | Low — private key never transmitted | ✅ Yes |
| Snowsight | Username + password | High without MFA | ⚠️ Only with MFA enforced |

### Enforce MFA — one SQL statement most teams never run

Snowflake's authentication policies mandate MFA enrollment before login. This enforces it at the session layer — not suggesting MFA, blocking login without a second factor.

```sql
-- Run as ACCOUNTADMIN
CREATE AUTHENTICATION POLICY IF NOT EXISTS restaurant_production_auth_policy
    AUTHENTICATION_METHODS      = ('PASSWORD', 'KEYPAIR')
    MFA_AUTHENTICATION_METHODS  = ('PASSWORD')        -- MFA required only for password logins
    MFA_ENROLLMENT              = REQUIRED             -- block login if MFA not enrolled
    CLIENT_TYPES                = ('SNOWFLAKE_UI', 'DRIVERS', 'SNOWSQL')
    COMMENT = 'MFA required for UI access, keypair allowed for service accounts';

-- Apply account-wide
ALTER ACCOUNT SET AUTHENTICATION POLICY restaurant_production_auth_policy;

-- Verify
SHOW AUTHENTICATION POLICIES;
```

`MFA_AUTHENTICATION_METHODS = ('PASSWORD')` means: when a user authenticates with a password, require Duo. Key-pair authentication (the Python agent) is unaffected — service accounts continue to work without interruption.

> ⚠️ Apply in a non-production account first. Any service account still using password authentication will be locked out after MFA enrollment becomes mandatory. Verify all service accounts use key-pair before enforcing account-wide.

### SSO integration (template)

For organizations with an IdP (Okta, Azure AD, Microsoft Entra):

```sql
-- Replace placeholder values with your IdP metadata
CREATE SECURITY INTEGRATION snowflake_sso_integration
    TYPE                = SAML2
    ENABLED             = TRUE
    SAML2_ISSUER        = 'https://your-idp.example.com'
    SAML2_SSO_URL       = 'https://your-idp.example.com/sso'
    SAML2_PROVIDER      = 'OKTA'                      -- or 'ADFS', 'CUSTOM'
    SAML2_X509_CERT     = '...'
    SAML2_SP_INITIATED_LOGIN_PAGE_LABEL = 'Corporate SSO';
```

### Network policy (template)

```sql
CREATE NETWORK POLICY restaurant_network_policy
    ALLOWED_IP_LIST = ('203.0.113.0/24', '10.0.0.0/8')   -- replace with your ranges
    BLOCKED_IP_LIST = ()
    COMMENT         = 'Production network restriction';

ALTER ACCOUNT SET NETWORK_POLICY = restaurant_network_policy;
```

---

## Part 4 — Audit Trail

Monitoring and security converge at the audit trail. `LOGIN_HISTORY` records every authentication attempt. Combined with `AGENT_INTERACTIONS`, this gives you anomaly detection baseline:

```sql
-- Password logins without MFA — flag for remediation before production
SELECT
    USER_NAME,
    EVENT_TIMESTAMP,
    REPORTED_CLIENT_TYPE,
    IS_SUCCESS,
    CLIENT_IP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND FIRST_AUTHENTICATION_FACTOR = 'PASSWORD'
  AND SECOND_AUTHENTICATION_FACTOR IS NULL
ORDER BY EVENT_TIMESTAMP DESC;

-- Users with password auth and no registered key pair — remediate these
SELECT name, login_name, has_password, has_rsa_public_key, disabled
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE has_password = TRUE
  AND has_rsa_public_key = FALSE
  AND disabled = FALSE
ORDER BY name;
```

---

## Security Checklist Before Production

- [ ] All human accounts enrolled in Duo MFA
- [ ] Authentication policy applied and verified (`SHOW AUTHENTICATION POLICIES`)
- [ ] Service accounts use key-pair authentication — no passwords
- [ ] Private key stored outside the project directory (not committed to Git)
- [ ] `GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` in place for cost monitoring
- [ ] Network policy configured for production IP ranges (if applicable)
- [ ] SSO integration configured (if IdP available)
- [ ] Zero rows in password-only user query above

---

## Design Notes

**Private key in production.** In this demo the `.p8` path is in `.env`. In production, retrieve the key at runtime from a secrets manager (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault) — the file disappears from the deployment artifact entirely.

**MFA does not affect the Python agent.** JWT key-pair authentication is explicitly excluded from the MFA requirement in the policy above. Enforcing MFA account-wide does not break programmatic access.
