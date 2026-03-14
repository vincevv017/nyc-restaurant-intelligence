-- =============================================================================
-- 04_cortex_agent_setup.sql
-- NYC Restaurant Intelligence — Phase 3: Cortex Agent
-- =============================================================================
-- Wires Cortex Analyst (semantic view → structured data) and
-- Cortex Search (document chunks → unstructured context) into a single
-- Cortex Agent that can answer both types of questions in one session.
--
-- Prerequisites:
--   ✅ Semantic view deployed  (setup/02_deploy_semantic_view.sql)
--   ✅ Cortex Search created   (setup/03_cortex_search_setup.sql)
--   ✅ Cross-region inference  (ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION)
--
-- This file:
--   Part 1 — Validate both tools are discoverable
--   Part 2 — Validate tool routing manually
--   Part 3 — Grant privileges
--   Part 4 — Create the agent via SQL (authoritative spec)
--   Part 5 — Verify the agent spec
--   Part 6 — Recovery: re-apply spec after Snowsight UI edits
--
-- ⚠️  CRITICAL LESSON LEARNED:
--   NEVER edit the agent through the Snowsight UI wizard.
--   The UI silently rewrites tool_resources on every save:
--     1. Lowercases and double-quotes the search service name
--     2. Re-adds columns_and_descriptions with UPPERCASE names
--     3. May swap 'name' key to 'search_service'
--   All three changes break Cortex Search retrieval silently.
--   Always use ALTER AGENT ... SET SPECIFICATION from this SQL file.
-- =============================================================================

USE ROLE      RESTAURANT_LOADER;
USE WAREHOUSE RESTAURANT_WH;
USE DATABASE  RESTAURANT_INTELLIGENCE;


-- =============================================================================
-- PART 1 — Validate both tools are discoverable
-- =============================================================================

-- Confirm semantic view exists
DESCRIBE SEMANTIC VIEW RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS;

-- Confirm Cortex Search service exists and is ready
DESCRIBE CORTEX SEARCH SERVICE RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH;

-- Check search service status (ACTIVE = ready, check scoring_profile_count)
SHOW CORTEX SEARCH SERVICES IN SCHEMA RESTAURANT_INTELLIGENCE.RAW;


-- =============================================================================
-- PART 2 — Validate tool routing manually before the demo
-- =============================================================================
-- These queries replicate what the agent does internally when it decides
-- which tool to use. Run them to confirm both tools return useful results
-- for your demo questions.

-- ── Tool 1: Cortex Analyst path (structured data) ───────────────────────────
-- Agent will route here for: counts, scores, grades, trends, comparisons

SELECT *
FROM SEMANTIC_VIEW(
  RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS
  METRICS grade_a_pass_rate, total_inspections
  DIMENSIONS borough
)
ORDER BY grade_a_pass_rate DESC;

-- ── Tool 2: Cortex Search path (unstructured docs) ──────────────────────────
-- Agent will route here for: "what does X mean", "why", "how is X calculated"

SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH',
    '{
      "query":   "violation 04L mice evidence condition levels points",
      "columns": ["chunk_text", "doc_type", "doc_label", "page_number"],
      "limit":   3
    }'
  )
)['results'] AS results;


-- =============================================================================
-- PART 3 — Grant privileges (run as ACCOUNTADMIN once)
-- =============================================================================

-- ── Run as ACCOUNTADMIN ──────────────────────────────────────────────────────
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE RESTAURANT_LOADER;

-- Grant Cortex Search usage to the agent's role.
-- Required after CREATE OR REPLACE on the search service, which resets grants.
-- GRANT USAGE ON CORTEX SEARCH SERVICE
--   RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH
--   TO ROLE RESTAURANT_LOADER;
-- Grant usage on the MARTS schema to RESTAURANT_LOADER
-- GRANT USAGE ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS TO ROLE RESTAURANT_LOADER;

-- Grant CREATE AGENT if creating agents in MARTS
-- GRANT CREATE AGENT ON SCHEMA RESTAURANT_INTELLIGENCE.MARTS TO ROLE RESTAURANT_LOADER;
-- ────────────────────────────────────────────────────────────────────────────


-- =============================================================================
-- PART 4 — Create the Cortex Agent via SQL
-- =============================================================================
-- This is the AUTHORITATIVE agent specification. Keep it in Git.
--
-- Instruction architecture (three layers):
--
--   system:         WHO the agent is. Persona, boundaries, anti-hallucination.
--                   Persistent across all turns.
--
--   orchestration:  HOW the agent thinks. Tool selection rules, multi-tool
--                   sequencing, chart guidance, ambiguity resolution.
--                   Controls planning before the user sees anything.
--
--   response:       WHAT the user sees. Transparency requirement, formatting
--                   rules, score directionality, citation style.
--                   Controls the final output format.
--
-- tool_resources design decisions:
--
--   1. MINIMAL Cortex Search config: just 'name' and 'max_results'.
--      No columns_and_descriptions. The search service already knows
--      which columns are searchable (ON) and filterable (ATTRIBUTES).
--
--   2. UPPERCASE service name, no quotes. Snowflake stores unquoted
--      identifiers in uppercase. The Snowsight UI lowercases and
--      double-quotes on save → case-sensitive mismatch → 399502 error.
--
--   3. data_to_chart tool included for visualization requests.
-- =============================================================================

CREATE OR REPLACE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
  COMMENT = 'NYC public health analytics — structured inspection data + health code documents. Use SQL (ALTER AGENT) for all spec changes. Do NOT edit via Snowsight UI.'
  PROFILE = '{"display_name": "NYC Restaurant Intelligence"}'
  FROM SPECIFICATION
$$
models:
  orchestration: claude-sonnet-4-6

orchestration:
  budget:
    seconds: 30
    tokens: 16000

instructions:
  system: |
    You are a NYC public health analytics assistant built for DOHMH regulators,
    restaurant operators, and public health researchers. You have access to live
    inspection data (296k+ records) and official NYC Health Code documents.

    You provide accurate, citation-backed answers. You never fabricate violation
    codes, section numbers, or penalty amounts. If you cannot find the specific
    information requested, say so clearly rather than guessing.

  orchestration: |
    TOOL SELECTION:
    - NUMBERS, COUNTS, RATES, TRENDS, RANKINGS, COMPARISONS → nyc_inspection_analyst ONLY
    - DEFINITIONS, LEGAL TEXT, CONDITION LEVELS, ENFORCEMENT RULES, PENALTY SCHEDULES, CLOSURE TRIGGERS → nyc_health_docs_search ONLY
    - Questions combining data AND regulatory context → use BOTH tools
    - When unsure which tool → try nyc_inspection_analyst first, then supplement with nyc_health_docs_search if the answer needs regulatory context

    MULTI-TOOL SEQUENCING:
    - When using both tools, query the structured data FIRST to identify specific violation codes or patterns, THEN search documents for those specific codes
    - Example: "worst violations in Brooklyn" → get top codes from analyst → search health code for each code's meaning and penalties

    CHART GENERATION:
    - When the user requests a chart or when the result is clearly visual (trends, comparisons, distributions), use data_to_chart
    - Prefer bar charts for categorical comparisons, line charts for time series, pie charts only when showing parts-of-whole with 6 or fewer categories
    - Always include axis labels and a descriptive title

    AMBIGUITY HANDLING:
    - If a question could apply to inspections OR violations table, prefer the inspections table (higher grain, includes scores and grades)
    - "worst" or "best" restaurants → rank by average_score (lower = better)
    - "most violations" → count from violations table, not inspections
    - If a borough is not specified, include all boroughs but exclude borough = '0' (data quality artifact)

    TIME DEFAULTS:
    - If the user does not specify a date range or time period, default to the last 12 months
    - If the user says "this year", use YEAR(inspection_date) = YEAR(CURRENT_DATE)
    - If the user says "recent", default to the last 6 months

  response: |
    TRANSPARENCY REQUIREMENT:
    Whenever you apply a filter or condition that the user did NOT explicitly
    request in their question, you MUST state it at the top of your response
    in this format:
    🔍 Filters applied:
       - <condition> — <reason>
    Examples:
      - grade IN ('A','B','C') → "Final grades only — pre-permit and pending excluded"
      - inspection_date >= DATEADD(month, -12, CURRENT_DATE) → "Last 12 months (default when no date specified)"
      - YEAR(inspection_date) = YEAR(CURRENT_DATE) → "Current year only"
      - borough != '0' → "Excluded records with missing borough data"
    If no implicit filters were applied, omit this section entirely.

    FORMATTING:
    - Scores are penalty-based: LOWER scores = BETTER performance. Never describe high scores as "good."
    - Round percentages to 1 decimal place. Round scores to whole numbers.
    - When citing health code sections, include the section number (e.g., §81.09).
    - When showing violation codes, include the human-readable description.

  sample_questions:
    - question: "What is the Grade A pass rate for each borough? Show as a bar chart."
    - question: "Which 10 cuisine types have the highest average inspection score?"
    - question: "How many inspections resulted in each grade (A, B, C) across all boroughs? Show as a pie chart."
    - question: "Show the monthly trend of total inspections and average scores for the last 24 months as a line chart."
    - question: "Compare the closure rate by quarter for 2024 versus 2025. Use a bar chart grouped by quarter."
    - question: "Are inspections conducted on weekends? Show the inspection volume by day of week."
    - question: "Which restaurants within 0.5 mile of Times Square have the worst inspection scores this year?"
    - question: "What are the top 15 most frequently cited critical violations, and what percentage of all violations do they represent? Show as a horizontal bar chart."
    - question: "Which boroughs have the highest re-inspection rate, and how does it correlate with average score? Show as a scatter chart with borough labels."
    - question: "What does violation code 10F mean, and what are the penalty amounts for each condition level?"
    - question: "Under what circumstances must an inspector call the office to discuss closing a restaurant?"
    - question: "What is the difference between a Condition I and a Condition V violation? How are points assigned?"
    - question: "Which Manhattan restaurants received the most critical violations this year, and what does the health code say about the enforcement actions for those specific violation types?"
    - question: "What is the critical violation rate for Chinese restaurants in Brooklyn, and what do the health code regulations say about the most common violations found in food preparation areas?"
    - question: "Show the top 5 violation codes by total citation count as a bar chart, then explain what each violation means according to the health code and what penalties apply."

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: nyc_inspection_analyst
      description: "Queries live DOHMH inspection data for counts, scores, rates, trends, rankings"
  - tool_spec:
      type: cortex_search
      name: nyc_health_docs_search
      description: "Searches NYC Health Code PDFs (Article 81, Chapter 23) for legal definitions, condition levels, enforcement rules"
  - tool_spec:
      type: data_to_chart
      name: data_to_chart
      description: "Generates visualizations from query results — bar, line, pie, scatter, heatmap charts"

tool_resources:
  nyc_inspection_analyst:
    semantic_view: "RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_INSPECTIONS"
  nyc_health_docs_search:
    name: "RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH"
    max_results: 5
$$;


-- =============================================================================
-- PART 5 — Verify the agent spec
-- =============================================================================
-- After creation (or after any ALTER), always verify the spec is clean.

DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT;

-- Inspect the agent_spec column. Checklist:
--
--   ✅ "name": "RESTAURANT_INTELLIGENCE.RAW.NYC_HEALTH_DOCS_SEARCH"
--      (uppercase, no escaped quotes)
--
--   ✅ No "columns_and_descriptions" in nyc_health_docs_search resources
--
--   ✅ No "search_service" key (should be "name")
--
--   ✅ Three instruction sections present: system, orchestration, response
--
--   ✅ Budget: seconds=30, tokens=16000
--
--   ✅ Three tools: cortex_analyst_text_to_sql, cortex_search, data_to_chart
--
--   ❌ "search_service": "...\"nyc_health_docs_search\""  ← Snowsight broke it
--   ❌ "columns_and_descriptions" present ← Snowsight re-added them

-- Grant usage so other roles can query the agent
-- GRANT USAGE ON AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
--   TO ROLE <other_role>;


-- =============================================================================
-- PART 6 — Recovery: re-apply spec after Snowsight UI edits
-- =============================================================================
-- If someone edited the agent through Snowsight and search broke,
-- run this ALTER to restore the authoritative spec.
--
-- Symptoms that indicate Snowsight corrupted the spec:
--   - Error 399502 "Cortex Search Service does not exist"
--   - Agent returns "I couldn't find information" for document questions
--   - SEARCH_PREVIEW works but agent search returns empty
--
-- Diagnosis:
--   DESCRIBE AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT;
--   Look for escaped quotes, lowercase service name, or columns_and_descriptions
--
-- Fix: run Part 4 again (CREATE OR REPLACE), or use ALTER:
--
-- ALTER AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
-- MODIFY LIVE VERSION SET SPECIFICATION =
-- $$
--   <paste the exact specification from Part 4 here>
-- $$;
--
-- Then verify with Part 5.
-- Then re-grant if needed:
-- GRANT USAGE ON AGENT RESTAURANT_INTELLIGENCE.MARTS.NYC_RESTAURANT_AGENT
--   TO ROLE RESTAURANT_LOADER;