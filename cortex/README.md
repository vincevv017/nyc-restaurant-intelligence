# cortex/ — Cortex Search + Cortex Agent (Phase 3)

This folder covers Phase 3: indexing NYC Health Code PDFs with Cortex Search and combining them with the semantic view through a Cortex Agent. The agent answers questions that neither structured data nor documents could handle alone — "which cuisines have the highest rate of 04L violations, and what does the health code say about enforcement thresholds that trigger closure?"

**Prerequisite:** Phase 2 complete. The semantic view `NYC_RESTAURANT_INSPECTIONS` must exist and pass smoke tests before adding the agent on top.

---

## Files

| File | Purpose |
|------|---------|
| `table_aware_extraction.py` | PDF extraction using `pdfplumber` — converts structured appendix tables into narrative prose chunks |
| `load_documents.py` | Downloads PDFs directly from NYC DOHMH public URLs, extracts text, and loads chunks into `RAW.DOCUMENT_CHUNKS` |
| `03_cortex_search_setup.sql` | Verifies chunks, creates the Cortex Search service, adds named scoring profiles, runs smoke tests |
| `04_cortex_agent_setup.sql` | Creates and configures the Cortex Agent with both Cortex Analyst and Cortex Search tools |
| `cortex_agent.py` | Python REST API client — JWT auth, SSE streaming, client-side SQL execution loop, three run modes |
| `requirements.txt` | `PyJWT`, `cryptography`, `requests`, `pdfplumber`, `snowflake-connector-python`, `python-dotenv` |

---

## Python Environment

This folder uses its own virtual environment, separate from the ingestion venv to avoid dependency conflicts.

```bash
cd cortex
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Step 1 — Load Health Code Documents

`load_documents.py` handles the full pipeline: it downloads the NYC DOHMH PDFs directly from their public URLs, extracts and chunks the text, and loads the chunks into `RESTAURANT_INTELLIGENCE.RAW.DOCUMENT_CHUNKS`.

```bash
# From the cortex/ folder with .venv active

# Full load — all document types (health_code, inspection_procedures, operator_guide, enforcement)
python load_documents.py

# Load a single doc type
python load_documents.py --doc-type health_code

# Dry run — download and parse only, no Snowflake writes
python load_documents.py --dry-run

# Skip OpenLineage emission
python load_documents.py --no-lineage
```

Expected output — a summary table per doc type showing chunk count and average word count.

**Chunking strategy:** 800-word target chunks with 80-word overlap between adjacent chunks. Chunks below 50 words (headers, page numbers) are discarded. Each chunk stores `doc_type`, `source_url`, and `page_number` for agent-side filtering.

---

## Step 2 — Create the Cortex Search Service

Run `03_cortex_search_setup.sql` as `RESTAURANT_LOADER`. The script is self-contained and walks through six steps. Key decisions documented in the SQL comments:

**Verify chunks first:**
```sql
SELECT doc_type, COUNT(*) AS chunks, MAX(loaded_at) AS last_loaded
FROM RESTAURANT_INTELLIGENCE.RAW.DOCUMENT_CHUNKS
GROUP BY doc_type ORDER BY doc_type;
-- If this returns 0 rows, run load_documents.py first
```

**Grant (ACCOUNTADMIN — run once):**
```sql
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA RESTAURANT_INTELLIGENCE.RAW
  TO ROLE RESTAURANT_LOADER;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;
```

**Create the service:**
```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search
  ON chunk_text
  PRIMARY KEY (doc_id)
  ATTRIBUTES doc_type, doc_label
  WAREHOUSE  = RESTAURANT_WH
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT doc_id, chunk_text, doc_type, doc_label, source_url, page_number, chunk_index, word_count, loaded_at
    FROM RESTAURANT_INTELLIGENCE.RAW.DOCUMENT_CHUNKS
    WHERE chunk_text IS NOT NULL AND word_count >= 50
  );
```

Three decisions worth noting for reproducibility:

- **`EMBEDDING_MODEL`** — `snowflake-arctic-embed-l-v2.0` (1024 dimensions) outperforms the default `m` model (768 dimensions) on nuanced regulatory text. This cannot be changed after creation — the service must be recreated to change it.
- **`PRIMARY KEY (doc_id)`** — enables optimised incremental refresh when document chunks change, reducing reindex cost.
- **`ATTRIBUTES doc_type, doc_label`** — only columns the agent should realistically filter on. Fewer attributes means a smaller filter index and fewer opportunities for the agent to generate malformed payloads.

**Verify the service is active:**
```sql
SHOW CORTEX SEARCH SERVICES IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;
-- Status must be ACTIVE before proceeding
```

> ⚠️ `SUSPEND`/`RESUME` does not force a reindex. With `TARGET_LAG = '1 day'` and incremental refresh, the only way to trigger a full rebuild after replacing document chunks is `CREATE OR REPLACE CORTEX SEARCH SERVICE`. Confirm rebuild completion via `source_data_num_rows` in `SHOW CORTEX SEARCH SERVICES`.

---

## Step 3 — Generate RSA Key Pair for JWT Authentication

The Python agent uses key-pair authentication — no password is transmitted.

```bash
# Generate private key
openssl genrsa -out ~/.ssh/snowflake_rsa_key.pem 2048

# Extract public key body (no headers, single line)
openssl rsa -in ~/.ssh/snowflake_rsa_key.pem -pubout \
  | grep -v "PUBLIC KEY" | tr -d '\n' > /tmp/sf_pub.key

chmod 600 ~/.ssh/snowflake_rsa_key.pem
```

Register with Snowflake:
```sql
-- Run as ACCOUNTADMIN
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<paste content of /tmp/sf_pub.key>';

-- Verify
DESCRIBE USER <your_username>;
-- RSA_PUBLIC_KEY_FP must show: SHA256:xxxxx
```

Add the key path to `.env` at the project root:
```dotenv
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.pem
```

> ⚠️ Never commit key files. The `.gitignore` in this repo excludes `*.pem`, `*.p8`, and `rsa_key.*` — but verify with `git status` before any commit.

---

## Step 4 — Create and Configure the Cortex Agent

Run `04_cortex_agent_setup.sql` as `RESTAURANT_LOADER`.

**After creation, always verify the spec was stored correctly:**
```sql
DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT;
-- Inspect the agent_spec column
```

**The Snowsight edit trap.** Saving any change through the Snowsight UI rewrites the `tool_resources` section of the agent spec. Two things happen that break Cortex Search: the service name gets wrapped in escaped lowercase quotes (`\"nyc_health_docs_search\"`), creating a case-sensitive mismatch with the actual service stored in uppercase; and `columns_and_descriptions` gets re-inserted with uppercase column names that produce silent empty results. The error that surfaces — "service does not exist" — gives no indication that a save operation modified the spec.

**Treat Snowsight as read-only for agents.** Use it to test and view; make all configuration changes via SQL:

```sql
ALTER AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
MODIFY LIVE VERSION SET SPECIFICATION = $$ <your spec> $$;
```

Keep the authoritative spec in Git alongside the semantic view YAML. If the agent breaks after a Snowsight interaction, `DESCRIBE AGENT` to inspect the spec, then re-apply from your versioned file.

---

## Step 5 — Configure and Run the Python Agent

All configuration is read from `.env` via `python-dotenv`. No edits to `cortex_agent.py` are needed — add the required values to your `.env` file at the project root:

```dotenv
SNOWFLAKE_ACCOUNT=ORGNAME-ACCOUNTNAME
SNOWFLAKE_USER=your_username
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.pem
```

> The account identifier uses the `ORGNAME-ACCOUNTNAME` format — find it in Snowsight under **Admin → Accounts**, or run `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()`. The JWT is derived from this value automatically — no separate variable needed.

> `SNOWFLAKE_USER` must match `LOGIN_NAME` from `DESCRIBE USER <username>`, not the display name.

The agent has three run modes:

```bash
# Single question — non-interactive
python cortex_agent.py --question "What is the Grade A pass rate by borough?"

# Progressive context demo — 3 questions, one per tool path
# Q1: Cortex Analyst only (structured data)
# Q2: Cortex Search only (document lookup)
# Q3: Both tools combined — the money shot
python cortex_agent.py --demo

# Interactive multi-turn session
python cortex_agent.py
```

Expected single-question output:
```
Generating JWT … ✅

You: What is the Grade A pass rate by borough?
🔧 [nyc_inspection_analyst] Executing query...
📊 Results:
...
Agent: Based on the inspection data, here are the Grade A pass rates by borough...
```

---

## Step 6 — Debug Mode

If the agent returns unexpected results — wrong tool called, empty search results, SQL errors — pass `--debug` to print the raw SSE events to stderr alongside normal output:

```bash
python cortex_agent.py --question "What does violation 04L mean?" --debug
```

**What is SSE?** Server-Sent Events is the streaming protocol the Cortex Agent API uses to push partial results to the client as they are generated, rather than waiting for the full response. The agent sends a sequence of named events — `response.text.delta` for text chunks, `response.tool_use` when it decides to call a tool, `response.tool_result` when a client-side tool completes. The Python client processes these events in order to assemble the final answer and handle the SQL execution loop for Cortex Analyst.

Debug output shows each SSE event as it arrives:

```
[DEBUG turn=0 event=response.tool_use] {"name": "nyc_health_docs_search", "client_side_execute": false, ...}
[DEBUG turn=0 event=response.text.delta] {"content_index": 0, "text": "According to..."}
```

This answers two diagnostic questions directly: which tool did the agent actually call, and what did Snowflake send back before the text was assembled?

If `response.tool_use` shows `nyc_inspection_analyst` when you expected `nyc_health_docs_search`, the routing instructions need adjustment. If `response.tool_result` is missing for a search query, the search service is returning no results for that query — run `SEARCH_PREVIEW` directly in Snowflake to inspect retrieval quality.

---

## Step 7 — Test the Full Question Matrix

Run through these questions in both Snowsight and via the Python agent and document the differences:

| # | Question | Expected Tool | What to Check |
|---|----------|---------------|---------------|
| 1 | "What is the Grade A pass rate by borough for the last 12 months?" | Cortex Analyst | Numbers match `SEMANTIC_VIEW()` smoke test |
| 2 | "Which cuisines have the highest critical violation rate?" | Cortex Analyst | Violation-level metric, cuisine breakdown |
| 3 | "What exactly is violation 04L and how are its condition levels determined?" | Cortex Search | Condition I through V with point values from Article 81 |
| 4 | "What enforcement action is required for more than 100 mouse droppings?" | Cortex Search | Condition V — inspector must call office |
| 5 | "Which cuisines have the highest rate of 04L violations, and what does the health code say about enforcement thresholds that trigger closure?" | Both | Structured data + document context in one answer |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `401 Unauthorized` | JWT issue | Check account identifier format (`ORGNAME-ACCOUNTNAME`); `SNOWFLAKE_USER` matches `LOGIN_NAME`; `RSA_PUBLIC_KEY_FP` is populated |
| `403 Forbidden` | Role missing Cortex privilege | `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER` |
| `error 399502 — service does not exist` | Service name case mismatch after Snowsight save | Re-apply spec via `ALTER AGENT ... SET SPECIFICATION` with service name exactly as stored; check with `SHOW CORTEX SEARCH SERVICES` |
| Agent breaks after Snowsight interaction | Spec rewritten on save | Re-apply from Git via SQL; use `DESCRIBE AGENT` to confirm |
| Search returns empty for appendix questions | Reranker down-scoring linearised table chunks | Run `SEARCH_PREVIEW` with `no_reranker` profile to compare; confirm `table_aware_extraction.py` was used for PDF ingestion |
| Agent routes document questions to Cortex Analyst | Orchestration routing decision | Add sharper routing examples to `orchestration` instructions; use `--debug` to confirm which tool was called |
| Agent applies borough filter but ignores date defaults | Agent orchestration runs before Cortex Analyst | Duplicate the date default rule in agent `orchestration` instructions — the semantic view rule alone is not sufficient inside the agent loop |
| `SUSPEND`/`RESUME` didn't reindex new chunks | Known behaviour with incremental refresh | Use `CREATE OR REPLACE CORTEX SEARCH SERVICE` to force full rebuild |
| `FileNotFoundError` on private key | Path in `.env` not found | Check `SNOWFLAKE_PRIVATE_KEY_PATH`; `~` is expanded automatically |

---

## Design Notes

**Why two separate venvs?** `snowflake-connector-python` pulled in by `dbt-snowflake` pins to different versions than the standalone connector used by `load_documents.py` and `cortex_agent.py`. Separate venvs avoid silent dependency conflicts.

**Why `tool_resources` is a separate top-level key.** A common mistake is nesting `tool_resources` inside `tools`. In the Cortex Agent REST API, `tool_resources` is a separate top-level dictionary — mixing them causes HTTP 400 error 399504.

**`cortex_analyst_text_to_sql` is client-side.** The agent returns SQL; the Python client executes it via `snowflake.connector` and feeds the result table back as a `tool_result` message. The agent loop runs up to 5 turns to handle this round-trip before synthesising the final answer. `cortex_search` is server-side — Snowflake executes it internally and the agent receives the retrieved content directly.

**Instruction precedence: agent vs. semantic view.** When Cortex Analyst runs inside a Cortex Agent, the agent's orchestration LLM makes filtering decisions before the question reaches Cortex Analyst. Business rules defined only in the semantic view's `sql_generation` (like "default to last 12 months") may be bypassed. Any rule that must be enforced reliably must also appear in the agent's `orchestration` instructions. Keep it in both places — the semantic view serves standalone Cortex Analyst; the agent spec enforces rules inside the agent loop.
