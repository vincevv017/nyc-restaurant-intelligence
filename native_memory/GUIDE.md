# Native Agent Memory — Phase Guide

> **Prerequisite:** Phases 1–4 complete. The Cortex Agent must be returning responses via Snowsight or the REST API before starting this phase.

> **Paid account required.** Custom tools (stored procedures) in Cortex Agents require a paid Snowflake account. The stored procedures and memory table can be created and tested independently on trial accounts, but end-to-end agent integration requires a paid account with `CORTEX_USER` privileges.

This phase replaces Phase 5's client-side memory with **server-side native memory**. The agent autonomously decides when to store and retrieve user facts using custom tools — no Python wrapper, no regex, no Streamlit. Memory becomes a first-class tool alongside Cortex Analyst and Cortex Search.

**Why this matters:** Snowflake's Cortex Agents already support *threads*¹ for short-term conversation context — the agent remembers what you said earlier in the same chat session. But threads are ephemeral: close the session, start a new thread, and the agent forgets everything. True long-term memory — remembering "my home address is 245 W 107th St" across sessions, days, and arbitrary phrasing — is still a gap in most native Cortex Agent implementations. This phase closes that gap using only Snowflake-native components, with no data leaving your governed environment.

---

## Technical Concepts: Vectors, Embeddings, and Similarity

Understanding three Snowflake capabilities is essential before reading the implementation. If you're already familiar with vector embeddings, skip to [Architecture](#architecture).

### What is a vector embedding?

A vector embedding is a list of numbers (a "vector") that represents the *meaning* of a piece of text. The embedding model — a neural network trained on massive text corpora — reads text and outputs a fixed-length array of floating-point numbers. Texts with similar meanings produce vectors that point in similar directions in high-dimensional space.

For example, `"home_address: 245 W 107th St"` and `"where does the user live"` are different strings but carry related meaning. Their embedding vectors will be geometrically close — and that geometric closeness is what makes retrieval work.

### AI_EMBED: turning text into vectors inside Snowflake

`AI_EMBED`² is Snowflake's function for generating embeddings. It runs entirely within Snowflake — no external API calls, no data leaving your account.

```sql
-- Returns a VECTOR(FLOAT, 1024) — 1024 numbers representing the meaning of the text
SELECT AI_EMBED('snowflake-arctic-embed-l-v2.0', 'home_address: 245 W 107th St');
```

The first argument is the embedding model. `snowflake-arctic-embed-l-v2.0` is Snowflake's large embedding model (1024 dimensions, multilingual). The model choice is locked at table design time — the vector column dimension must match exactly (`VECTOR(FLOAT, 1024)`), and all vectors in the table must come from the same model, otherwise similarity scores are meaningless.

**Why we need it here:** When the agent stores a fact like `"home_address: 245 W 107th St"`, we embed it so that later, when the user asks `"restaurants near my home"`, we can find the stored fact by meaning — not by exact keyword match. The user never says "home_address" — they say "my home", "where I live", "near me". Embeddings bridge that vocabulary gap.

### VECTOR_COSINE_SIMILARITY: finding related facts

Once facts are stored as vectors, retrieval becomes a geometry problem. `VECTOR_COSINE_SIMILARITY`³ measures how closely two vectors point in the same direction, returning a score between -1.0 and 1.0 (in practice, text embeddings range from ~0.0 to ~0.8).

```sql
-- Embed the user's question, then find which stored facts are most similar
SELECT fact_key, fact_value,
       VECTOR_COSINE_SIMILARITY(fact_embedding, AI_EMBED('snowflake-arctic-embed-l-v2.0', 'restaurants near my home')) AS similarity
FROM AGENT_MEMORY_VECTORS
WHERE user_id = CURRENT_USER()
ORDER BY similarity DESC
LIMIT 5;
```

This is "top-k retrieval" — retrieve the k most similar facts, ranked by cosine similarity. The agent gets back the most relevant facts for its current question, not every fact ever stored. A user with 20 stored facts about their address, cuisine preferences, role, and violation focus gets only the facts relevant to *this specific question*.

**Why cosine similarity and not exact match?** An exact `WHERE fact_key = 'home_address'` lookup requires knowing the key in advance. The agent doesn't know which fact_key to look for — it only has the user's natural language question. Cosine similarity lets the question itself ("restaurants near me") find the relevant fact ("home_address: 245 W 107th St") without the agent needing to guess the key name.

### How it fits together

```
User: "restaurants near my home"
  │
  ▼  AI_EMBED('snowflake-arctic-embed-l-v2.0', 'restaurants near my home')
  │  → query_vector [0.12, -0.34, 0.56, ... 1024 floats]
  │
  ▼  VECTOR_COSINE_SIMILARITY(each stored fact_embedding, query_vector)
  │  → home_address:   similarity = 0.62  ← highest match
  │  → favorite_cuisine: similarity = 0.25
  │  → role:            similarity = 0.11
  │
  ▼  Return top result: "home_address: 245 W 107th St"
  │
  ▼  Agent uses this to build a proximity query via Cortex Analyst
```

---

## Architecture

### Phase 5 (Client-Side) vs. Phase 6 (Native) — What Changed and Why

| Dimension | Phase 5 (Client-Side) | Phase 6 (Native) |
|---|---|---|
| **Who decides to store** | Python regex on agent response | Agent orchestration LLM |
| **Who decides to retrieve** | Python client (loads ALL facts) | Agent orchestration LLM (semantic query) |
| **Storage** | Key-value table, exact PK lookup | Key-value table + vector embeddings |
| **Retrieval** | `SELECT * WHERE user_id = ?` | `VECTOR_COSINE_SIMILARITY` top-k |
| **Client code needed** | `memory_manager.py`, `response_parser.py` | None — agent handles everything |
| **Latency per turn** | ~0ms (regex is instant) | ~3,200ms (AI_EMBED + vector search) |
| **Cost per turn** | Zero (no LLM call for memory) | ~0.001 credits (AI_EMBED call) |
| **Accuracy** | Limited to predefined regex patterns | Agent understands arbitrary phrasing |
| **Failure mode** | Silent miss if phrasing doesn't match regex | Agent may over-retrieve on ambiguous questions |

**Why both approaches have value:** Phase 5's regex approach has zero latency and zero cost — it's perfect when memory patterns are predictable and few. Phase 6's native approach handles arbitrary phrasing ("I usually get sushi near Herald Square") that no regex would catch. The right choice depends on the use case: if you control the conversation flow (Streamlit app with predefined inputs), Phase 5 is simpler. If users interact freely (Snowsight chat, API), Phase 6 is more robust.

### Component Diagram

```
User question
    │
    ▼
Cortex Agent (orchestration LLM: Claude Sonnet 4.6)
    │
    ├── "restaurants near me" ─────────────────────┐
    │   detected personal context                  │
    │                                              ▼
    │                                 retrieve_user_memories (stored procedure)
    │                                   │  AI_EMBED query → vector search
    │                                   │  returns: "home_address: 245 W 107th St"
    │                                   ▼
    │                                 Agent uses address in analyst query
    │                                              │
    ├── data question ─────────────────────────────┼──► nyc_inspection_analyst
    │                                              │      (Cortex Analyst)
    ├── regulatory question ───────────────────────┼──► nyc_health_docs_search
    │                                              │      (Cortex Search)
    ├── chart request ─────────────────────────────┼──► data_to_chart
    │                                              │
    └── user shares personal info ─────────────────┘
        "I live at 245 W 107th St"
            │
            ▼
        store_user_memory (stored procedure)
          │  AI_EMBED "home_address: 245 W 107th St"
          │  MERGE into AGENT_MEMORY_VECTORS
          ▼
        Agent confirms: "I'll remember that."
```

### How the Agent Decides

The orchestration LLM (Claude Sonnet 4.6) makes tool selection decisions based on three signals:

1. **Tool descriptions** — the `description` field in each `tool_spec`. These are the primary signal. A vague description causes missed invocations; an overly broad one causes over-invocation.

2. **Orchestration instructions** — the `instructions.orchestration` block. This provides explicit routing rules ("RETRIEVE first when the question references personal context").

3. **Input schema descriptions** — the `description` fields in `input_schema.properties`. These tell the LLM what to put in each parameter.

**This is the key difference from Phase 5:** no deterministic regex decides when to store or retrieve. The LLM reads the tool descriptions and orchestration rules, interprets the user's message, and decides. This means the agent can handle "I usually grab lunch at the ramen place on 33rd" (implicit cuisine + location preference) — something no regex would catch.

**The tradeoff:** The LLM may occasionally call `retrieve_user_memories` on a factual question where memory isn't relevant, adding ~3,200ms latency and a small cost. The orchestration instructions mitigate this with explicit "do NOT retrieve for purely factual questions" rules, but it's not guaranteed. Budget has been increased to `seconds: 45, tokens: 20000` to accommodate the extra tool calls.

---

## Files

```
native_memory/
├── GUIDE.md                           ← you are here
├── 01_memory_vector_table.sql         ← CREATE TABLE with VECTOR(FLOAT, 1024)
├── 02_memory_procedures.sql           ← STORE_USER_MEMORY + RETRIEVE_USER_MEMORIES
├── 03_agent_with_native_memory.sql    ← CREATE OR REPLACE AGENT (full spec)
├── 04_testing_strategy.sql            ← 62 structured test questions across 5 categories
├── test_results.yaml                  ← YAML template for recording test results + iteration log
```

---

## Embedding Strategy

### Why "fact_key: fact_value" concatenation

When the store procedure embeds a fact, it concatenates the key and value: `"home_address: 245 W 107th St"`. This matters because:

- Embedding just `"245 W 107th St"` would match queries about addresses in general, not specifically the user's *home* address.
- Embedding just `"home_address"` would lose the actual value.
- The concatenation `"home_address: 245 W 107th St"` creates a vector that's semantically close to queries like "where does the user live" AND "245 W 107th St" AND "home address".

When the retrieve procedure embeds the query `"restaurants near my home"`, the cosine similarity between "restaurants near my home" and "home_address: 245 W 107th St" is high because the embedding model understands the semantic relationship between "my home" and "home_address".

### Model choice

`snowflake-arctic-embed-l-v2.0` produces 1024-dimensional vectors. This is the same model used by the project's Cortex Search service (`03_cortex_search_setup.sql`), maintaining consistency.

For personal facts — which are short strings (10-50 words) — the smaller `snowflake-arctic-embed-m-v1.5` (768 dimensions) would be sufficient and approximately 30% cheaper per embedding call. The L model is chosen here for consistency, not necessity. If deploying this pattern in a cost-sensitive environment with high memory write volume, consider the M model.

### Similarity threshold

The retrieve procedure filters results at `similarity >= 0.3`. This threshold was chosen to balance:
- **Too high (>0.7):** misses legitimately relevant facts when the query phrasing differs from the stored key:value format
- **Too low (<0.2):** returns irrelevant facts that add noise to the agent's context
- **0.3:** captures semantically related facts while filtering obvious non-matches

This threshold should be tuned based on actual retrieval quality in your dataset. The smoke tests in `02_memory_procedures.sql` provide a baseline for evaluating threshold quality.

---

## Deployment: Step by Step

### Step 1 — Create the memory table

Open a Snowflake worksheet as **RESTAURANT_LOADER** and run `01_memory_vector_table.sql`.

Verify:
```sql
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS;
-- Expected: 9 columns, PK on (user_id, fact_key)
```

### Step 2 — Create the stored procedures

Run `02_memory_procedures.sql` in the same worksheet.

Verify both procedures exist:
```sql
SHOW PROCEDURES LIKE 'STORE_USER_MEMORY' IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;
SHOW PROCEDURES LIKE 'RETRIEVE_USER_MEMORIES' IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;
```

Run the smoke tests at the bottom of the script. All five tests should produce the expected output.

### Step 3 — Deploy the memory-enhanced agent

Run `03_agent_with_native_memory.sql`.

This creates a **new** agent (`NYC_RESTAURANT_MEMORY_AGENT`) alongside the original `NYC_RESTAURANT_AGENT`. Both agents share the same semantic view and Cortex Search service but are independent objects. The original is untouched — useful for side-by-side comparison in the article and as a safe rollback.

Verify both agents exist:
```sql
DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT;        -- original
DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_MEMORY_AGENT; -- new
```

Check the `agent_spec` column of the memory agent against the checklist in Part 3 of the SQL file.

### Step 4 — End-to-end test

Open the memory agent in Snowsight (AI & ML → Agents → **NYC Restaurant Intelligence (Memory)**). The original agent "NYC Restaurant Intelligence" remains available for comparison.

**Test 1 — Store:**
```
You: I live at 245 W 107th St in Manhattan
Agent: [calls store_user_memory] I'll remember that for next time.
```

**Test 2 — Retrieve + Analyst:**
```
You: Show me Grade A restaurants near my home
Agent: [calls retrieve_user_memories → gets address → calls nyc_inspection_analyst with proximity filter]
Agent: Here are Grade A restaurants near 245 W 107th St...
```

**Test 3 — Cross-session persistence:**
Close the agent chat. Open a new chat (new thread).
```
You: What restaurants are near me?
Agent: [calls retrieve_user_memories → gets stored address → queries]
```

**Test 4 — No memory fires on factual questions:**
```
You: What does violation code 04L mean?
Agent: [calls nyc_health_docs_search directly — no retrieve_user_memories call]
```

### Step 5 — Validate memory state in SQL

```sql
-- All stored facts for the current user
SELECT fact_key, fact_value, category,
       fact_embedding IS NOT NULL AS has_embedding,
       updated_at
FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
WHERE user_id = CURRENT_USER()
ORDER BY updated_at DESC;

-- Correct a fact directly
UPDATE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
SET    fact_value     = '500 8th Avenue, Midtown Manhattan',
       fact_embedding = AI_EMBED('snowflake-arctic-embed-l-v2.0', 'home_address: 500 8th Avenue, Midtown Manhattan'),
       updated_at     = CURRENT_TIMESTAMP()
WHERE  user_id  = CURRENT_USER()
  AND  fact_key = 'home_address';

-- Erase all facts for the current user
DELETE FROM RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS
WHERE user_id = CURRENT_USER();
```

**Note:** When correcting a fact via SQL, you must also recompute the embedding. Updating `fact_value` without updating `fact_embedding` creates a mismatch — the vector still represents the old value, so retrieval will return the old semantics even though the stored text is new.

---

## Testing Strategy

A working agent is not a tested agent. Tool routing, memory storage, cross-session persistence, scope boundaries, and multi-tool orchestration all need systematic validation — not ad-hoc spot checks. The test suite in `04_testing_strategy.sql` covers 62 questions across five categories, adapted from Snowflake's own agent testing methodology⁹ ¹⁰.

### Why structured testing matters

The orchestration LLM is non-deterministic. A spec change that improves memory retrieval might silently degrade baseline data questions. The only way to catch this is to re-run the full suite after every `ALTER AGENT` or `CREATE OR REPLACE AGENT`.

### Test categories

| Category | Count | Purpose |
|---|---|---|
| **1. Baseline data** | 15 | Original agent capabilities — memory should NOT fire |
| **2. Memory store & retrieve** | 15 | Store facts, retrieve by similarity, cross-session persistence |
| **3. Out-of-scope** | 10 | Questions the agent should decline (medical, financial, non-NYC) |
| **4. Edge cases** | 12 | Empty memory, apostrophes, prompt injection, ambiguous "my", upserts |
| **5. Multi-tool coordination** | 10 | Memory → Analyst → Search → Chart chains (2, 3, and 4-tool sequences) |

### How to run

1. Open Snowsight → AI & ML → Agents → **NYC Restaurant Intelligence (Memory)**
2. Run each question, note the tool calls (visible in the agent's reasoning trace)
3. Compare against the Expected columns in the SQL file
4. For regression check: run Category 1 against the original agent too — results should be identical

### Key regression signals

- **Category 1 failure**: Memory instructions are too broad, causing unnecessary `retrieve_user_memories` calls on purely factual questions
- **Category 2 failure (Q2.09–Q2.12)**: Cross-session persistence broken — check that the memory table has rows with `CURRENT_USER()` and that the agent spec still includes the custom tools
- **Category 4 failure (Q4.08)**: SQL injection via apostrophe in stored procedure — check the `safe_value = value.replace("'", "''")`  escaping
- **Category 5 failure**: Budget too tight for multi-tool chains — increase `seconds` and `tokens` in the orchestration budget

### Recording results

`test_results.yaml` is a structured template for tracking results across both agents and iteration cycles. For each question it records the expected tools, actual tools observed, response summary, and pass/fail. Key design choices:

- **Dual-agent columns for Category 1** — every baseline question has separate `original` and `memory` blocks, making regression immediately visible
- **`memory_tools_fired` flag** — for baseline and out-of-scope questions where memory should NOT fire, this boolean catches over-retrieval regressions
- **`verify_sql` fields** — on questions that modify state (upserts, stores), the YAML includes a SQL query to verify the table state independently
- **`critical` flag** — marks the two tests that are non-negotiable: Q2.09 (cross-session persistence) and Q4.08 (apostrophe SQL injection)
- **Iteration log at the bottom** — after each run, record what failed, what spec change you made, and which questions to re-test. This creates an auditable improvement trail — useful for the article and for anyone reproducing the work

Workflow: copy `test_results.yaml` as `test_results_run_001.yaml` for each run. Diff between runs in Git to see what improved and what regressed.

---

## Scaling to a Team

This implementation scopes memory by `CURRENT_USER()`, which works for single-user demos and environments where each person has their own Snowflake login. For team deployments:

**Shared service account problem:** If all users access the agent through a shared Snowflake login (common in some Streamlit deployments), `CURRENT_USER()` returns the same value for everyone. All users share a single memory space.

**Solutions:**
1. **Individual Snowflake users** — the cleanest approach. Each person's `CURRENT_USER()` is unique. Works natively with the stored procedures as written.
2. **Explicit user_id parameter** — modify both stored procedures to accept `user_id` as a parameter instead of using `CURRENT_USER()`. The client application (Streamlit, API wrapper) passes the authenticated user identity. This requires trusting the client to pass the correct identity.
3. **Session variable** — set a session variable (`ALTER SESSION SET user_identity = 'analyst_01'`) before agent calls. The stored procedures read it with `CURRENT_SESSION()`. More complex but doesn't require procedure changes.

For regulated environments, consider adding row-level security via masking policies so users cannot query other users' memory even with direct SQL access.

---

## Mapping to Snowflake's Agent Context Layer Architecture

Klahr & Samdani's six-layer Agent Context Layer architecture (March 2026) defines these layers:

| Layer | This Implementation | Status |
|---|---|---|
| **Analytic** (structured data queries) | Cortex Analyst + semantic view | ✅ Complete (Phase 2) |
| **Relationship/Identity** (entity resolution) | User memory + fact keys | ✅ Partial — stores user identity facts; no entity graph |
| **Event/Decision Memory** | `AGENT_MEMORY_VECTORS` table | ✅ **Deployed** — previously a gap |
| **Provenance/Explainability** | `📅 Data through` + `🔍 Filters applied` | ✅ Complete (Phase 3) |
| **Policy/Entitlements** | RBAC via RESTAURANT_LOADER role | ✅ Basic (Phase 1) |
| **Operational Playbooks** | Agent orchestration instructions | ✅ Partial — routing rules, not full playbooks |

This implementation closes the **Event/Decision Memory gap** identified in the previous article. The memory table stores user-provided facts (events) that the agent uses to make future decisions (personalized queries). This is not full decision logging — the agent does not record *which* queries it ran or *what* it decided — but it captures the persistent context that shapes future decisions.

---

## Honest Assessment: What Doesn't Work (Yet)

### 1. The orchestration LLM is not deterministic

The agent *may* call `retrieve_user_memories` when it's not needed, or skip it when it is. The orchestration instructions mitigate this, but there's no guarantee. In testing, Claude Sonnet 4.6 correctly routes ~90% of the time with well-crafted tool descriptions and orchestration instructions. The remaining ~10% are edge cases where the question is ambiguous ("show me the data" — personal or general?).

**Mitigation:** The retrieve procedure adds ~3,200ms per turn. Over-retrieval wastes a few seconds but doesn't produce wrong answers — the agent simply gets extra context it may not use. Under-retrieval is worse — the agent asks for information it already has — but this is rare with the current trigger phrases in the orchestration instructions.

### 2. Memory deletion works via soft-delete

In testing, natural language deletion worked without any additional tool or procedure change:

```
User:  "Forget my home address"
Agent: "Done! I've cleared your home address from memory."

User:  "What is my home address?"
Agent: "I don't have a home address stored for you — it was previously cleared from memory.
        If you'd like, you can share your address and I'll remember it for future questions."

User:  "My new home address is 500 8th Avenue, Midtown"
Agent: "Got it! Your home address has been saved as 500 8th Avenue, Midtown, NYC."
```

The agent handles deletion by calling `store_user_memory` with `fact_value: "REMOVE"`. The `STORE_USER_MEMORY` procedure upserts the row — the `fact_key` is preserved, the value becomes `"REMOVE"`, and `source_turn` is also set to `"REMOVE"`. On the next retrieve, the agent reads back `home_address: REMOVE` and correctly treats it as cleared, asking the user for their address rather than using the stale value. A subsequent store call upserts over `"REMOVE"` with the new value, restoring the fact cleanly.

This is emergent behavior — no explicit deletion logic was written. The agent generalizes from the upsert semantics of `store_user_memory` to implement a soft-delete pattern. The row is not physically removed from `AGENT_MEMORY_VECTORS`; it remains with `fact_value = 'REMOVE'` as an audit trail.

**Note:** Direct SQL deletion remains available for hard deletes (see Step 5). The soft-delete approach is sufficient for conversational use — the `"REMOVE"` sentinel value is unlikely to conflict with any real fact value, and the agent reliably interprets it as "not set."

### 3. SQL injection surface in stored procedures

The current procedures use string concatenation with single-quote escaping (`value.replace("'", "''")`). This handles the most common injection vector (apostrophes in addresses and names) but is not parameterized. Snowpark's `session.sql()` supports bind variables (`params=[...]`) for SELECT queries, but MERGE statements with complex expressions (including function calls like `AI_EMBED`) are harder to parameterize.

**Risk level:** Low in this context — the agent constructs the parameters, not the user directly. The user's input passes through the agent's orchestration LLM, which reformats it before passing it to the stored procedure. A direct SQL injection via the agent would require the user to craft a prompt that causes the LLM to pass malicious SQL as a `fact_value` — possible in theory but unlikely with current LLM behavior.

### 4. Embedding cost at scale

Every store operation calls `AI_EMBED` once. Every retrieve operation calls `AI_EMBED` once (to embed the query) plus reads the stored vectors. For a single user storing ~10 facts, this is negligible. For 1,000 users each storing 50 facts, the vector table grows to 50,000 rows and each retrieval scans all rows for the user (filtered by `user_id`, then sorted by similarity). Clustering by `user_id` helps, but at very large scale, consider:
- **A dedicated Cortex Search service over the memory table** — this is the strongest scaling option. Instead of manual `AI_EMBED` + `VECTOR_COSINE_SIMILARITY` in the retrieve procedure, create a Cortex Search service with `ON fact_text` and `ATTRIBUTES user_id, category`. The retrieve procedure becomes a Cortex Search query with a `@eq` filter on `user_id`. Snowflake manages the embedding, indexing, and ranking. The tradeoff: you lose fine-grained control over the similarity threshold and ranking, but gain automatic index management and better performance at scale.
- Partitioning by category to reduce scan scope
- Caching embeddings for frequently repeated queries

### 5. No thread awareness

The stored procedures use `CURRENT_USER()` but not `CURRENT_THREAD()`. All facts are session-independent, which is the desired behavior for persistent memory. However, thread-scoped memory (facts that only apply to a specific conversation) is not supported. Adding a `thread_id` parameter to the procedures would enable this, but the current design intentionally avoids it — persistent cross-session memory is the goal.

### 6. No conflict resolution between stored facts

The agent stores facts independently without checking for semantic contradictions. In testing, a user with `home_address: 100 Broadway, Financial District` stored later in the same session triggered storage of `home_borough: Brooklyn` — a direct conflict. The MERGE procedure correctly upserts by `fact_key`, so two facts with different keys (e.g., `home_address` and `home_borough`) coexist without either being overwritten. The agent has no logic to detect that a new location fact contradicts an existing one.

**Mitigation:** The retrieve procedure returns the top-k most semantically similar facts. If both facts are retrieved for a location query, the agent will typically use the more specific one (a full address beats a borough name). However, this is not guaranteed — the agent may construct queries using the wrong fact.

**Future option:** Add a pre-store step that calls `retrieve_user_memories` with the same query before storing, and includes any conflicting existing facts in the `store_user_memory` call so the agent can decide whether to update or add.

### 7. Opinions stored as preferences

The orchestration LLM occasionally interprets opinionated statements as personal preferences and stores them. In testing, "Japanese restaurants are the best" triggered storage of `favorite_cuisine: Japanese` — even though this is a general opinion, not a stated personal preference. The distinction between "I prefer Japanese food" (store) and "Japanese restaurants are the best" (do not store) requires the agent to assess intent, which it does not reliably do.

**Mitigation:** The store tool description specifies "when the user shares personal information" and lists explicit examples, which reduces but does not eliminate this. The orchestration instructions note "Do NOT store analytical results, query outputs, or general conversation" but opinions expressed as superlatives ("X is the best") are a grey area.

**Future option:** Add an explicit instruction: "Do NOT store general opinions or superlative statements about a category ('X is the best', 'I love X') unless the user explicitly says they personally prefer it ('I prefer X', 'my favorite is X', 'I usually choose X')."

---

## Design Notes

**Why stored procedures and not UDFs?**

UDFs are simpler (single SQL expression) but cannot perform multi-step logic like "check if the user has any memories, then embed the query, then run similarity search, then format results." Stored procedures support procedural logic, error handling, and multiple SQL statements — essential for the retrieve workflow.

**Why EXECUTE AS CALLER?**

Two options exist: `EXECUTE AS OWNER` (procedure runs with the owner's privileges) and `EXECUTE AS CALLER` (runs with the calling user's privileges). Caller's rights means:
1. `CURRENT_USER()` returns the actual user (correct for memory scoping)
2. The user must have privileges on the memory table (enforceable via RBAC)
3. No privilege escalation — the procedure can't access objects the user can't access directly

**Why a 0.3 similarity threshold?**

Cosine similarity between `snowflake-arctic-embed-l-v2.0` vectors ranges from -1.0 to 1.0 (though in practice, text embeddings rarely go below 0.0). A threshold of 0.3 means "moderately related." In testing:
- "restaurants near my home" vs "home_address: 245 W 107th St" → similarity ~0.55-0.65
- "restaurants near my home" vs "favorite_cuisine: Japanese" → similarity ~0.20-0.30
- "what does violation 04L mean" vs "home_address: 245 W 107th St" → similarity ~0.10-0.15

The 0.3 threshold correctly includes the home address for location queries and excludes it for regulatory queries. The cuisine preference lands near the boundary — which is appropriate, since "restaurants near my home" *could* benefit from knowing cuisine preference.

**Why budget increase to 45s / 20k tokens?**

A memory-augmented turn can involve up to four tool calls:
1. `retrieve_user_memories` (~3,200ms — AI_EMBED call + vector search)
2. `nyc_inspection_analyst` (~2-5s SQL generation + execution)
3. `nyc_health_docs_search` (~1-2s if regulatory context needed)
4. `data_to_chart` (~1s if visualization requested)

The original 30s / 16k budget was tight for two tools. Four tools with memory context needs headroom. 45s / 20k provides this without being wasteful.

---

## Phase Navigation

| Phase | Folder | What it covers |
|-------|--------|----------------|
| 1 | *(root README)* | Socrata ingestion, dbt star schema, data quality |
| 2 | [`semantic/`](../semantic/README.md) | Semantic view, Cortex Analyst benchmarking |
| 3 | [`cortex/`](../cortex/README.md) | PDF indexing, Cortex Agent, JWT REST client |
| 4 | [`monitoring/`](../monitoring/README.md) | Cost tracking, audit trail |
| 5 | [`memory/`](../memory/memory_README.md) | Client-side memory (regex), Streamlit in Snowflake |
| 6 | **`native_memory/`** ← you are here | Server-side memory (custom tools, AI_EMBED, vector retrieval) |
| Map | [`map/`](../map/README.md) | Interactive pydeck map via SEMANTIC_VIEW |

---

## References

¹ Cortex Agents Threads: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-threads
² AI_EMBED function: https://docs.snowflake.com/en/sql-reference/functions/ai_embed
³ Vector embeddings & similarity functions: https://docs.snowflake.com/en/user-guide/snowflake-cortex/vector-embeddings
⁴ Cortex Agents overview & custom tools: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents
⁵ Configure agents & add custom tools: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage
⁶ Best practices for building Cortex Agents: https://www.snowflake.com/en/developers/guides/best-practices-to-building-cortex-agents/
⁷ Community: stored procedure as custom tool: https://community.snowflake.com/s/article/How-to-configure-Stored-Procedure-as-a-Custom-Tool-in-Snowflake-Intelligence
⁸ Klahr & Samdani — Agent Context Layer architecture: https://www.snowflake.com/en/blog/agent-context-layer-trustworthy-data-agents/
⁹ Jia, T. — Optimize Snowflake Intelligence Cortex Agent Setup: https://medium.com/snowflake/optimize-snowflake-intelligence-cortex-agent-setup-a-complete-ai-powered-guide-f01383ac6969
¹⁰ Izmit, S. — Agent Instruction Best Practices for Snowflake Intelligence: https://medium.com/snowflake/agent-instruction-best-practices-for-snowflake-intelligence-dfbe71a07722
