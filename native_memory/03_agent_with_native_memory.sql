-- =============================================================================
-- 03_agent_with_native_memory.sql
-- Native Agent Memory: New Agent with Custom Memory Tools
-- =============================================================================
-- Creates a NEW agent (NYC_RESTAURANT_MEMORY_AGENT) that extends the original
-- NYC_RESTAURANT_AGENT with two custom tools for persistent user memory.
-- The original agent is untouched — both agents coexist in MARTS and share
-- the same Cortex Analyst semantic view and Cortex Search service.
--
-- Why a separate agent:
--   1. The original agent (04_cortex_agent_setup.sql) is the stable baseline
--      for Phases 1–4 and the existing article series
--   2. Side-by-side comparison: run the same question against both agents to
--      demonstrate the memory capability in the article (screenshot)
--   3. Safe rollback — if memory instructions degrade routing quality for
--      non-memory questions, the original agent is unaffected
--
-- Prerequisites:
--   ✅ 01_memory_vector_table.sql — memory table exists
--   ✅ 02_memory_procedures.sql   — both stored procedures exist
--   ✅ 04_cortex_agent_setup.sql   — original agent exists (Phases 1–4)
--
-- ⚠️  CRITICAL: As with the original agent, NEVER edit through the Snowsight
--   UI wizard. The UI silently rewrites tool_resources and may strip
--   input_schema from custom tools. Always use this SQL file.
--
-- References:
--   Custom tools in agents: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage
--   CREATE AGENT syntax: https://docs.snowflake.com/en/sql-reference/sql/create-agent
--   Cortex Agents Run API (tool_spec schema): https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-run
--   Best practices: https://www.snowflake.com/en/developers/guides/best-practices-to-building-cortex-agents/
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;
USE DATABASE  RESTAURANT_INTELLIGENCE;


-- =============================================================================
-- PART 1 — Verify prerequisites
-- =============================================================================

-- Memory table exists
DESCRIBE TABLE RESTAURANT_INTELLIGENCE.RAW.AGENT_MEMORY_VECTORS;

-- Both procedures exist
SHOW PROCEDURES LIKE 'STORE_USER_MEMORY' IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;
SHOW PROCEDURES LIKE 'RETRIEVE_USER_MEMORIES' IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;

-- Original tools still work
DESCRIBE SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS;
DESCRIBE CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH;


-- =============================================================================
-- PART 2 — Create the memory-enhanced agent (separate from the original)
-- =============================================================================
-- This specification is the AUTHORITATIVE source. Keep it in Git.
--
-- This is a NEW agent alongside the original NYC_RESTAURANT_AGENT.
-- Both agents share the same semantic view and Cortex Search service.
-- The original agent is untouched for side-by-side comparison and safe rollback.
--
-- Additions relative to 04_cortex_agent_setup.sql:
--   1. Two new tools: store_user_memory, retrieve_user_memories (type: generic)
--   2. Two new tool_resources entries (type: procedure, with warehouse)
--   3. Orchestration instructions extended with memory tool routing
--   4. System instructions updated with memory persona
--   5. Response instructions updated with memory acknowledgment format
--   6. Budget increased (45s/20k) to accommodate memory tool calls
--
-- Tool description design:
--   Descriptions are the primary signal the orchestration LLM uses to decide
--   when to call a tool. They must be specific enough to trigger correctly
--   but not so broad that the tool fires on every turn.
--   Ref: https://www.snowflake.com/en/developers/guides/best-practices-to-building-cortex-agents/
-- =============================================================================

CREATE OR REPLACE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_MEMORY_AGENT
  COMMENT = 'NYC public health analytics with persistent user memory — structured data + health code docs + custom memory tools. Use SQL (ALTER AGENT) for all spec changes. Do NOT edit via Snowsight UI.'
  PROFILE = '{"display_name": "NYC Restaurant Intelligence (Memory)"}'
  FROM SPECIFICATION
$$
models:
  orchestration: claude-sonnet-4-6

orchestration:
  budget:
    seconds: 45
    tokens: 20000

instructions:
  system: |
    You are a NYC public health analytics assistant built for DOHMH regulators,
    restaurant operators, and public health researchers. You have access to live
    inspection data (296k+ records), official NYC Health Code documents, and
    persistent memory of each user's personal information and preferences.

    You provide accurate, citation-backed answers. You never fabricate violation
    codes, section numbers, or penalty amounts. If you cannot find the specific
    information requested, say so clearly rather than guessing.

    OUT-OF-SCOPE — DECLINE politely and do not pivot or substitute:
    - Medical or health advice: food poisoning risk, illness symptoms, personal
      health decisions — decline even when the question touches food safety topics;
      do not pivot to health code data as a proxy answer
    - Geographic scope: your data covers NYC only — if asked about other cities,
      states, or countries, decline; do NOT substitute NYC data as a proxy
    - Financial or investment advice: restaurant profitability, ROI, revenue — decline
    - Legal advice: lawsuits, regulatory appeals, legal rights — decline even if
      you can cite the relevant health code section; refer to an attorney
    - Cooking and recipes: food preparation methods — decline
    - Platform or technology comparisons: Snowflake vs competitors, tool evaluations — decline
    - Predictions: future inspection scores or outcomes — decline; offer historical
      trends only if the user asks for them
    - Unrelated questions: if the question has no connection to NYC restaurant
      inspections or public health regulation, decline briefly and redirect

    When declining, be brief and redirect to what you can help with.

    MEMORY BEHAVIOR:
    - You remember personal facts users have shared: home address, work location,
      preferred borough, favorite cuisine, role, organization, and other preferences.
    - When a user shares personal information, store it immediately — do not wait
      for them to ask you to remember it.
    - When a user asks a question that could benefit from personal context (e.g.,
      "restaurants near me", "my usual area"), retrieve their stored facts first.
    - Never ask for information you already have stored. If retrieval returns a
      fact, use it directly.
    - If you need personal information and retrieval returns nothing, ask the user
      once. When they respond, store it for future sessions.

  orchestration: |
    OUT-OF-SCOPE QUESTIONS: decline first — do not call any tool. See system instructions.

    TOOL SELECTION:
    - NUMBERS, COUNTS, RATES, TRENDS, RANKINGS, COMPARISONS → nyc_inspection_analyst ONLY
    - DEFINITIONS, LEGAL TEXT, CONDITION LEVELS, ENFORCEMENT RULES, PENALTY SCHEDULES, CLOSURE TRIGGERS → nyc_health_docs_search ONLY
    - Questions combining data AND regulatory context → use BOTH analyst + search
    - When unsure which tool → try nyc_inspection_analyst first, then supplement with nyc_health_docs_search if the answer needs regulatory context

    MEMORY TOOLS — WHEN TO USE:
    - RETRIEVE first when the question references personal context:
      Trigger phrases: "near me", "my home", "my area", "my office", "my favorite",
      "the kind I like", "restaurants I'd enjoy", "where I live", "my work",
      "my neighborhood", "my preference", "for me", "my borough"
    - RETRIEVE first when the question is ambiguous and personal context would help:
      "best restaurants nearby", "good options for lunch", "what would you recommend"
    - STORE when the user explicitly provides personal information:
      "I live at 245 W 107th St", "I work in Midtown", "I prefer Japanese food",
      "I'm a food safety inspector", "My office is in Brooklyn"
    - STORE when the user answers a question you asked about their personal info
    - Do NOT retrieve for purely factual questions:
      "What does violation 04L mean?" → nyc_health_docs_search (no memory needed)
      "How many inspections in Queens last year?" → nyc_inspection_analyst (no memory needed)
    - Do NOT store analytical results, query outputs, or general conversation

    MEMORY + ANALYST SEQUENCING:
    - When the question needs both personal context AND data:
      1. Call retrieve_user_memories FIRST
      2. Use the retrieved facts to build the analyst query
         (e.g., user's home address → geo coordinates → proximity filter)
      3. Call nyc_inspection_analyst with the informed query
    - Example: "restaurants near me with Grade A" →
      retrieve_user_memories("user home address location") →
      gets "home_address: 245 W 107th St" →
      nyc_inspection_analyst with proximity filter around that address

    MULTI-TOOL SEQUENCING:
    - When using both analyst + search, query structured data FIRST, THEN search docs
    - Example: "worst violations in Brooklyn" → get top codes → search health code

    CHART GENERATION:
    - When the user requests a chart or result is clearly visual, use data_to_chart
    - Prefer bar for comparisons, line for time series, pie for parts-of-whole (≤6 categories)
    - Always include axis labels and a descriptive title

    AMBIGUITY HANDLING:
    - "worst" or "best" restaurants → rank by average_score (lower = better)
    - "most violations" → count from violations table
    - Exclude borough = '0' unless specified

    TIME DEFAULTS:
    - No date range specified → last 12 months
    - "this year" → YEAR(inspection_date) = YEAR(CURRENT_DATE)
    - "recent" → last 6 months

    DATA FRESHNESS:
    - For EVERY question routed to nyc_inspection_analyst, determine
      MAX(inspection_date) and include it in response context

  response: |
    DATA FRESHNESS REQUIREMENT:
    Every response that uses inspection data MUST begin with:
    📅 Data through: <most recent inspection date from MAX(inspection_date)>
    Omit for pure document searches.

    TRANSPARENCY REQUIREMENT:
    Whenever you apply an implicit filter, state it:
    🔍 Filters applied:
       - <condition> — <reason>

    MEMORY ACKNOWLEDGMENT:
    When you store a new fact, briefly confirm: "I'll remember that for next time."
    When you use a retrieved fact, use it naturally without announcing the retrieval.
    Example — DO: "Looking at restaurants near your home at 245 W 107th St..."
    Example — DON'T: "I retrieved from memory that your address is..."

    FORMATTING:
    - Scores are penalty-based: LOWER scores = BETTER performance.
    - Round percentages to 1 decimal place. Round scores to whole numbers.
    - When citing health code sections, include the section number (e.g., §81.09).
    - When showing violation codes, include the human-readable description.

  sample_questions:
    - question: "What is the Grade A pass rate for each borough? Show as a bar chart."
    - question: "Which 10 cuisine types have the highest average inspection score?"
    - question: "What does violation code 10F mean, and what are the penalty amounts?"
    - question: "Show me the worst-scoring restaurants near my home."
    - question: "Based on my favorite cuisine, which boroughs have the best options?"
    - question: "I live at 30 Rockefeller Plaza. What restaurants near me have Grade A?"
    - question: "I'm a food safety inspector focusing on critical violations in Brooklyn."
    - question: "Remember that I prefer Italian food."
    - question: "What are the most common violations in restaurants near my office?"

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: nyc_inspection_analyst
      description: "Queries live DOHMH inspection data for counts, scores, rates, trends, rankings, and comparisons across restaurants, boroughs, cuisines, and time periods"
  - tool_spec:
      type: cortex_search
      name: nyc_health_docs_search
      description: "Searches NYC Health Code PDFs (Article 81, Chapter 23) for legal definitions, violation condition levels, enforcement rules, penalty schedules, and closure triggers"
  - tool_spec:
      type: data_to_chart
      name: data_to_chart
      description: "Generates visualizations from query results — bar, line, pie, scatter, heatmap charts"
  - tool_spec:
      type: "generic"
      name: "store_user_memory"
      description: "Stores a personal fact about the current user for future sessions. Call this when the user shares personal information such as their home address, work location, preferred borough, favorite cuisine, role, organization, or any preference. The fact is stored with a semantic embedding so it can be retrieved by meaning later. Each fact_key is unique per user — storing with an existing key updates the value."
      input_schema:
        type: "object"
        properties:
          fact_key:
            type: "string"
            description: "A snake_case label for the fact. Use descriptive keys: home_address, work_location, preferred_borough, favorite_cuisine, role, organization, neighborhood, violation_focus, grade_focus, inspection_type, score_threshold, time_window"
          fact_value:
            type: "string"
            description: "The exact value to store, as stated by the user. For addresses include street and area. For preferences use the user's own words."
          category:
            type: "string"
            description: "Category for the fact. One of: location (addresses, boroughs, neighborhoods), preference (cuisines, grades, time ranges), identity (role, organization), analytical (score thresholds, violation focus), general (anything else)"
        required:
          - fact_key
          - fact_value
          - category
  - tool_spec:
      type: "generic"
      name: "retrieve_user_memories"
      description: "Retrieves stored personal facts about the current user that are relevant to the given query. Uses semantic similarity to find the most relevant facts. Call this BEFORE answering any question that references personal context — locations ('near me', 'my area'), preferences ('my favorite', 'I usually'), or any prior stated facts. Returns a ranked list of facts with similarity scores."
      input_schema:
        type: "object"
        properties:
          query:
            type: "string"
            description: "A natural language description of what personal information is needed. Examples: 'user home address location', 'food cuisine preferences', 'user role and organization', 'all personal preferences and identity information'"
          max_results:
            type: "integer"
            description: "Maximum number of facts to return (1-10, default 5). Use higher values when the question might relate to multiple stored facts."
        required:
          - query

tool_resources:
  nyc_inspection_analyst:
    semantic_view: "RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS"
  nyc_health_docs_search:
    name: "RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH"
    max_results: 5
  store_user_memory:
    type: "procedure"
    identifier: "RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY"
    execution_environment:
      type: "warehouse"
      warehouse: "RESTAURANT_WH"
  retrieve_user_memories:
    type: "procedure"
    identifier: "RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES"
    execution_environment:
      type: "warehouse"
      warehouse: "RESTAURANT_WH"
$$;


-- =============================================================================
-- PART 3 — Verify the memory-enhanced agent spec
-- =============================================================================

-- Confirm original agent is still untouched
DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT;

-- Verify the new memory-enhanced agent
DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_MEMORY_AGENT;

-- Checklist:
--
--   ✅ Five tools present:
--      1. cortex_analyst_text_to_sql (nyc_inspection_analyst)
--      2. cortex_search (nyc_health_docs_search)
--      3. data_to_chart
--      4. generic (store_user_memory)
--      5. generic (retrieve_user_memories)
--
--   ✅ tool_resources for store_user_memory:
--      type: "procedure"
--      identifier: "RESTAURANT_INTELLIGENCE.RAW.STORE_USER_MEMORY"
--      execution_environment.warehouse: "RESTAURANT_WH"
--
--   ✅ tool_resources for retrieve_user_memories:
--      type: "procedure"
--      identifier: "RESTAURANT_INTELLIGENCE.RAW.RETRIEVE_USER_MEMORIES"
--      execution_environment.warehouse: "RESTAURANT_WH"
--
--   ✅ input_schema present on both generic tools
--      (Snowsight UI may strip this — always verify after any edit)
--
--   ✅ System instructions mention memory behavior
--   ✅ Orchestration instructions include MEMORY TOOLS section
--   ✅ Response instructions include MEMORY ACKNOWLEDGMENT section
--
--   ✅ Budget: seconds=45, tokens=20000 (increased from 30/16000 to
--      accommodate memory tool calls + analyst queries in a single turn)
--
--   ❌ If any tool_resources show lowercase identifiers or missing
--      input_schema → Snowsight UI corruption. Re-run this CREATE OR REPLACE.


-- =============================================================================
-- PART 4 — End-to-end test scenarios
-- =============================================================================
-- Open the MEMORY agent in Snowsight:
--   AI & ML → Agents → NYC Restaurant Intelligence (Memory)
-- The original agent "NYC Restaurant Intelligence" remains available for
-- side-by-side comparison on the same questions.
--
-- Scenario 1: Store + Retrieve in same session
--   User: "I live at 245 W 107th St in Manhattan"
--   Expected: Agent calls store_user_memory(home_address, "245 W 107th St, Manhattan", location)
--   Expected: Agent confirms "I'll remember that for next time."
--
--   User: "Show me Grade A restaurants near my home"
--   Expected: Agent calls retrieve_user_memories("user home address location")
--   Expected: Agent uses the address in a proximity query via nyc_inspection_analyst
--
-- Scenario 2: Cross-session persistence
--   (Close and reopen the agent chat — new thread)
--   User: "What restaurants are near me?"
--   Expected: Agent calls retrieve_user_memories → gets stored address → queries
--
-- Scenario 3: Preference storage
--   User: "I prefer Japanese food"
--   Expected: Agent calls store_user_memory(favorite_cuisine, "Japanese", preference)
--
--   User: "What are the best restaurants for my favorite cuisine in Manhattan?"
--   Expected: retrieve_user_memories → gets "Japanese" → queries Japanese restaurants
--
-- Scenario 4: No memory → ask the user
--   (New user or cleared memory)
--   User: "Show me restaurants near my office"
--   Expected: retrieve_user_memories → "No memories found" → Agent asks for office address
--   User: "30 E 33rd St, Midtown"
--   Expected: Agent stores it AND uses it to answer the original question
--
-- Scenario 5: Memory does NOT fire for factual questions
--   User: "What does violation code 04L mean?"
--   Expected: Agent routes to nyc_health_docs_search directly — NO memory call
--
--   User: "How many inspections were conducted in Queens last year?"
--   Expected: Agent routes to nyc_inspection_analyst directly — NO memory call
--
-- Scenario 6: Side-by-side comparison (for the article screenshot)
--   Ask both agents the SAME question:
--     "Show me restaurants near my home with the worst inspection scores"
--   Original agent: asks for address every time
--   Memory agent: remembers from Scenario 1 — no re-asking


-- =============================================================================
-- PART 5 — Recovery: re-apply spec after accidental Snowsight edit
-- =============================================================================
-- Same pattern as 04_cortex_agent_setup.sql.
-- If someone edits the memory agent through Snowsight and memory tools break:
--
-- Symptoms:
--   - Agent says "I don't have access to memory tools"
--   - Agent never calls store/retrieve even when personal info is shared
--   - Error about procedure not found
--   - input_schema stripped from custom tools
--
-- Fix: re-run Part 2 (CREATE OR REPLACE) from this file.
-- Then verify with Part 3.
--
-- The original NYC_RESTAURANT_AGENT is never affected by changes to
-- NYC_RESTAURANT_MEMORY_AGENT — they are independent objects.
