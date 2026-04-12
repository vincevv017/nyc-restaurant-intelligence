# Phase 5 — Persistent Memory

> **Prerequisite:** Phases 1–4 complete. `cortex/cortex_agent.py` must be returning responses successfully before starting this phase.

> **Paid account required.** The Cortex Agent REST API (`/api/v2/cortex/agent:run`) returns error 399504 ("Access denied") on Snowflake trial accounts. All supporting components (`memory_manager.py`, `response_parser.py`, the memory table) work on trial and can be validated independently. The full end-to-end flow requires a paid account.

This phase adds persistent user memory to the Cortex Agent. Facts a user shares — home address, preferred borough, role — are stored in a Snowflake table and injected into every future session automatically. The agent stops asking for information it already knows.

Two deployment targets ship from the same `memory/` folder:

| Target | File | Auth | Best for |
|--------|------|------|----------|
| CLI | `agent_with_memory.py` | JWT key-pair (same `.pem` as `cortex_agent.py`) | Developers, programmatic access |
| Streamlit in Snowflake | `streamlit_app.py` | `get_active_session()` — Snowflake manages identity | Browser-based users |

---

## Architecture

```
CLI path                               SiS path
──────────────────────────────         ──────────────────────────────────────
agent_with_memory.py                   streamlit_app.py
  │                                      │
  │  imports SYSTEM_PROMPT,              │  imports SYSTEM_PROMPT,
  │  call_cortex_agent, get_jwt_token    │  CORTEX_TOOLS, _parse_sse
  │  from cortex_agent.py               │  from cortex_agent.py (on stage)
  │                                      │
  ▼                                      ▼  get_active_session() → OAuth token
MemoryManager.load()               MemoryManager (facts via session.sql)
  │                                      │
  ▼  prepend facts to question           ▼  prepend facts to question
call_cortex_agent(augmented_q,     _call_agent_sis(augmented_q,
  token, history)                    history)
  │                                      │
  ▼  SSE stream                          ▼  SSE stream
response_parser.parse_response()   response_parser.parse_response()
  │                                      │
  ▼  print body to terminal             ▼  render in st.chat_message
  [📅 / 🔍 not produced — see note]     sidebar panels (📅 / 🔍)
```

**Memory injection:** `MemoryManager.to_system_context()` serialises stored facts into a directive string prepended to the user's question before calling the agent. There is no `system_context` parameter on `call_cortex_agent` — injection is via question text.

---

## Files

```
memory/
├── memory_README.md             ← you are here
├── 01_setup_memory_table.sql    ← creates AGENT_USER_MEMORY
├── memory_manager.py            ← MemoryManager class (load / save / inject)
├── response_parser.py           ← strips 📅 / 🔍 metadata from agent responses
├── agent_with_memory.py         ← CLI agent with memory
├── streamlit_app.py             ← Streamlit in Snowflake UI
└── requirements.txt             ← Phase 5 Python dependencies
```

---

## Supported memory facts

All fact capture and deletion is rule-based regex — no LLM, zero latency, zero cost per turn.

### Capture: what the agent can remember

A fact is saved when the agent's response matches a detection pattern **and** the user's next message passes the answer-guard (no `?`, does not start with a question/directive word).

| Fact key | What is stored | Example agent question that triggers capture | Example user answer |
|---|---|---|---|
| `home_address` | Home address | "Where do you live?" / "Could you share your home address?" | "245 W 107th St, Manhattan" |
| `work_location` | Work address | "Where is your office?" / "Tell me your work location" | "30 E 33rd St, Midtown" |
| `neighborhood` | Specific neighbourhood | "Which neighbourhood are you in?" | "Upper West Side" |
| `preferred_borough` | Borough preference | "Which borough do you prefer?" | "Manhattan" |
| `favorite_cuisine` | Cuisine preference | "Which cuisine is your favourite?" / "What food do you prefer?" | "Japanese" |
| `violation_focus` | Violation code / type being tracked | "Which violation code are you focusing on?" | "04L" / "critical violations" |
| `grade_focus` | Grade filter preference | "Which grades are you focused on?" | "C" / "B and C" |
| `inspection_type` | Initial vs re-inspection interest | "Initial or re-inspections?" / "Which type of inspection?" | "Initial only" |
| `score_threshold` | Score cutoff of interest | "What score threshold are you tracking?" | "28 and above" |
| `time_window` | Preferred date range | "What date range do you prefer?" / "How many months back?" | "Last 6 months" |
| `role` | User's job role | "What is your role?" / "What do you do?" | "Food safety inspector" |
| `organization` | User's employer | "Which agency are you with?" / "Your organisation?" | "NYC DOHMH" |

### Deletion: what the user can remove

Say any phrase matching `(remove|delete|forget|clear|erase) … <keyword>` in a single message. The deletion is handled locally — the agent is not called.

| To delete | Example phrases |
|---|---|
| `home_address` | "remove my home address", "forget where I live", "clear my address" |
| `work_location` | "delete my work location", "forget my office address" |
| `neighborhood` | "remove my neighborhood", "clear my area" |
| `preferred_borough` | "forget my borough preference" |
| `favorite_cuisine` | "delete my favorite cuisine", "forget my food preference" |
| `violation_focus` | "clear my violation focus", "remove violation code" |
| `grade_focus` | "delete my grade filter", "forget my grade focus" |
| `inspection_type` | "remove inspection type preference" |
| `score_threshold` | "clear my score threshold", "forget my score cutoff" |
| `time_window` | "remove my date range", "clear my time window" |
| `role` | "delete my role", "forget my job title" |
| `organization` | "remove my organization", "forget my agency" |

### Limitations

- **Pattern-based only**: capture fires only when the agent's response matches a known phrase pattern. If the agent asks for information in an unexpected way (e.g., "Mind telling me what cuisine you enjoy?"), the pattern will not fire and no `💾` prompt will appear.
- **One fact per turn**: only the first matching pattern is saved per agent response. If the agent asks two questions at once, only the first match is captured.
- **Answer guard is heuristic**: the guard skips saving if the user's reply contains `?` or starts with a question word. A reply like "show me Japanese restaurants" would be skipped even though it implies a cuisine preference — the user would need to say "Japanese" plainly.
- **No update confirmation**: if a fact already exists, `memory.save()` silently overwrites it via `MERGE`. The `💾 Saved:` line shows the new value.
- **`time_window`, `score_threshold`, `grade_focus`, `inspection_type`**: these are stored as free-text strings exactly as the user typed them. The agent receives them as context but must interpret them — there is no enforcement that "last 6 months" is applied to every query.

---

## Note on `📅 / 🔍` metadata blocks

`response_parser.py` is written to strip `📅 Data through:` and `🔍 Filters applied:` lines from agent responses and route them to the Streamlit sidebar. However, the current `SYSTEM_PROMPT` in `cortex_agent.py` does **not** instruct the agent to produce these blocks.

- **CLI (`cortex_agent.py`, `agent_with_memory.py`):** these blocks are never produced. `response_parser.py` returns the full response unmodified (`freshness_date=None`, `filters=[]`, `body=<full text>`). This is correct behaviour — there is nothing to strip.

- **Streamlit app (`streamlit_app.py`):** the sidebar `📅 Data freshness` and `🔍 Filters applied` panels are rendered but will remain empty because the same `SYSTEM_PROMPT` is used.

To activate the sidebar panels, add output format instructions to `SYSTEM_PROMPT` in `cortex_agent.py` telling the agent to append those blocks to every structured-data response. Because `streamlit_app.py` imports `SYSTEM_PROMPT` directly, the change propagates to both targets automatically.

---

## Step 1 — Create the memory table

Open a Snowflake worksheet as **RESTAURANT_LOADER** (or ACCOUNTADMIN) and run `memory/01_setup_memory_table.sql`:

```sql
USE ROLE      RESTAURANT_LOADER;
USE DATABASE  RESTAURANT_INTELLIGENCE;
USE WAREHOUSE RESTAURANT_WH;

CREATE TABLE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY (
    user_id      VARCHAR        NOT NULL,
    fact_key     VARCHAR        NOT NULL,
    fact_value   VARCHAR,
    source_turn  VARCHAR,
    updated_at   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_agent_user_memory PRIMARY KEY (user_id, fact_key)
);

ALTER TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
    CLUSTER BY (user_id);
```

Verify:
```sql
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY;
-- Expected: 5 columns — USER_ID, FACT_KEY, FACT_VALUE, SOURCE_TURN, UPDATED_AT
-- Primary key constraint on (USER_ID, FACT_KEY)
```

---

## Step 2 — Install Python dependencies

The memory phase shares the `cortex/` virtual environment:

```bash
cd cortex && source .venv/bin/activate
pip install -r ../memory/requirements.txt
```

The only addition over Phase 3 is `streamlit>=1.35.0` for local testing of `streamlit_app.py`. `snowflake-ml-python` is **not** required — `check_for_memory_request()` uses regex, not a Cortex LLM call.

---

## Step 3 — Run the CLI agent with memory

From the repo root (cortex venv active):

```bash
python memory/agent_with_memory.py
```

**Session 1 — first run, no stored memory:**

```
══════════════════════════════════════════════════════════════════════
  NYC Restaurant Intelligence — Cortex Agent + Memory  (multi-turn)
  User: VINCEVV11712
  Type 'memory' to review stored facts, 'exit' to quit.
══════════════════════════════════════════════════════════════════════

  [No stored memory for this user]

You: Show me recent critical violations near my home

Agent: I don't have your home address on file. Could you share it so I
       can filter inspections to your neighbourhood?

  💾 I'll remember your answer to use in future sessions.

You: 245 West 107th Street, Manhattan, NY 10025
  💾 Saved: home_address = '245 West 107th Street, Manhattan, NY 10025'

Agent: Here are critical violations within 0.5 miles of 245 West 107th
       Street in the last 12 months: [results]

You: exit
```

**Session 2 — new process, memory loaded automatically:**

```bash
python memory/agent_with_memory.py
```

```
  🧠 Memory loaded (1 fact(s)):
     • home address: 245 West 107th Street, Manhattan, NY 10025

You: Show me critical violations near my home

Agent: Based on your address at 245 West 107th Street, Manhattan, here
       are critical violations within 0.5 miles in the last 12 months: [results]
```

No follow-up question. The memory context was prepended to the question before the agent was called.

**Utility flags:**

```bash
python memory/agent_with_memory.py --show-memory
python memory/agent_with_memory.py --clear-memory
python memory/agent_with_memory.py --user analyst_01 --clear-memory
```

---

## Step 4 — Deploy the Streamlit in Snowflake app

### 4a — Create a stage

```sql
-- Run as ACCOUNTADMIN
GRANT CREATE STREAMLIT ON SCHEMA RESTAURANT_INTELLIGENCE.RAW
    TO ROLE RESTAURANT_LOADER;

-- Run as RESTAURANT_LOADER
CREATE STAGE IF NOT EXISTS RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Streamlit in Snowflake files — Phase 5 memory app';

GRANT READ, WRITE ON STAGE RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE
    TO ROLE RESTAURANT_LOADER;
```

### 4b — Upload app files

`streamlit_app.py` is fully self-contained — `CORTEX_TOOLS`, `CORTEX_TOOL_RESOURCES`, `SYSTEM_PROMPT`, and `_parse_sse` are inlined. Only `memory_manager.py` and `response_parser.py` are needed alongside it. `cortex_agent.py` is **not** uploaded to the stage.

`snowsql` is not required. Use the upload script (key-pair auth, no MFA prompt):

```bash
cd <repo-root>
source cortex/.venv/bin/activate
python memory/upload_to_stage.py
```

Expected output:
```
  PUT   streamlit_app.py … UPLOADED
  PUT   memory_manager.py … UPLOADED
  PUT   response_parser.py … UPLOADED

Stage contents (3 file(s)):
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE/memory_manager.py
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE/response_parser.py
  @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE/streamlit_app.py
```

Verify independently:
```sql
LIST @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE;
-- Expected: 3 rows
```

### 4c — Create the Streamlit app

```sql
USE ROLE RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;

CREATE OR REPLACE STREAMLIT RESTAURANT_INTELLIGENCE.RAW.NYC_RESTAURANT_MEMORY_APP
    ROOT_LOCATION = '@RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE'
    MAIN_FILE     = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'RESTAURANT_WH'
    COMMENT = 'NYC Restaurant Intelligence — Phase 5: Persistent Memory';

GRANT USAGE ON STREAMLIT RESTAURANT_INTELLIGENCE.RAW.NYC_RESTAURANT_MEMORY_APP
    TO ROLE RESTAURANT_LOADER;
```

---

## Step 5 — Validate memory state in SQL

```sql
-- All stored facts for a user
SELECT fact_key, fact_value, updated_at, source_turn
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
WHERE user_id = 'VINCEVV11712'
ORDER BY updated_at DESC;

-- Correct a fact
UPDATE RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
SET    fact_value = '500 8th Avenue, Manhattan, NY 10018',
       updated_at = CURRENT_TIMESTAMP()
WHERE  user_id  = 'VINCEVV11712'
  AND  fact_key = 'home_address';

-- Erase all facts for a user
DELETE FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
WHERE user_id = 'VINCEVV11712';
```

---

## Troubleshooting

**`MFA authentication is required for programmatic authentication`**

`memory_manager.py` uses key-pair auth — it reads `SNOWFLAKE_PRIVATE_KEY_PATH` from `.env` (default: `~/.ssh/snowflake_rsa_key.pem`). Password auth is not used. If you see this error, verify the key path exists and the public key is registered on your user:
```sql
DESC USER VINCEVV11712;  -- RSA_PUBLIC_KEY_FP should be non-null
```

**`ModuleNotFoundError: No module named 'cortex_agent'` in SiS**

`cortex_agent.py` was not uploaded to the stage alongside `streamlit_app.py`. Run `LIST @RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MEMORY_STAGE;` and verify all 4 files are present.

**`📅 Data freshness` and `🔍 Filters applied` sidebar panels are always empty**

Expected — the current `SYSTEM_PROMPT` in `cortex_agent.py` does not instruct the agent to produce those metadata blocks. `response_parser.py` is in place and ready. To activate the panels, add format instructions to `SYSTEM_PROMPT` telling the agent to end every structured-data response with `📅 Data through: <date>` and `🔍 Filters applied:` lines.

**Memory is never captured — `check_for_memory_request` never fires**

`check_for_memory_request()` uses regex patterns. It matches when the agent's response contains phrases like "home address", "which borough", "your role". If the agent rephrases the request in a way the patterns don't cover, edit `_MEMORY_REQUEST_PATTERNS` in `memory_manager.py` to match the actual wording.

**`CURRENT_USER()` returns the same value for all users (shared service account)**

In shared environments all users may run under one Snowflake login — they would share a single memory space. Add an explicit user selector widget to the sidebar and pass it as `user_id` to `MemoryManager`. Not relevant for a personal account.

---

## Design Notes

**Why key-pair auth for the CLI but `get_active_session()` for SiS?**

`cortex_agent.py` and `memory_manager.py` run outside Snowflake (local terminal). Snowflake enforces MFA on password-based `snowflake.connector` connections for paid accounts, making password auth non-interactive and therefore broken for scripts. Key-pair auth (`private_key=<DER bytes>`) bypasses MFA — the connector verifies identity cryptographically against the registered public key.

In Streamlit in Snowflake, Snowflake manages authentication at the platform level. `get_active_session()` returns a trusted session; `CURRENT_USER()` returns the verified user identity. No private key file is needed on the SiS side.

**Why regex and not an LLM for memory extraction?**

`check_for_memory_request()` classifies whether the agent asked for personal information. The patterns are simple and stable (the agent asks for home address, borough, role in predictable phrasing). A regex runs at zero latency and zero cost. An LLM call would add ~300ms per turn with no accuracy benefit for a binary classification over a small, known pattern set.

**Why a relational table and not Cortex Search for memory?**

Cortex Search is a similarity index for fuzzy retrieval over document corpora. Retrieving "what is this user's home address?" via a vector query can surface approximate or wrong results. A primary-key lookup on `(user_id, fact_key)` is exact. For identity facts that must be precise — addresses, role names, preferences — a relational table with `MERGE`-based upserts is the correct structure.

**Why `RAW` schema and not a dedicated `MEMORY` schema?**

`RESTAURANT_LOADER` already has full DML on `RESTAURANT_INTELLIGENCE.RAW` from Phase 1. Creating a new schema would require additional `ACCOUNTADMIN` grants. For a project this size, one extra table in `RAW` is simpler and equally correct.

---

## Phase Navigation

| Phase | Folder | What it covers |
|-------|--------|----------------|
| 1 | *(root README)* | Socrata ingestion, dbt star schema, data quality |
| 2 | [`semantic/`](../semantic/README.md) | Semantic view, Cortex Analyst benchmarking |
| 3 | [`cortex/`](../cortex/README.md) | PDF indexing, Cortex Agent, JWT REST client |
| 4 | [`monitoring/`](../monitoring/README.md) | Cost tracking, audit trail |
| 5 | **`memory/`** ← you are here | Persistent memory, Streamlit in Snowflake UI |
| Map | [`map/`](../map/README.md) | Interactive pydeck map via SEMANTIC_VIEW |
