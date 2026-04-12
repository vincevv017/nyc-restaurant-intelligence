#!/usr/bin/env python3
"""
cortex_agent.py
────────────────────────────────────────────────────────────────────────────────
Cortex Agent demo for the NYC Restaurant Intelligence article series.

Authentication fix: The Cortex Agent /api/v2/cortex/agent:run endpoint requires
a JWT (key-pair authentication). Session tokens work for older Snowflake REST
APIs but not this one. One-time setup below.

ONE-TIME KEY PAIR SETUP
────────────────────────────────────────────────────
# 1. Generate private key
openssl genrsa -out ~/.ssh/snowflake_rsa_key.pem 2048

# 2. Extract public key
openssl rsa -in ~/.ssh/snowflake_rsa_key.pem -pubout \
  | grep -v "PUBLIC KEY" | tr -d '\n' > /tmp/sf_pub.key

# 3. Register with Snowflake (run in a worksheet as your user):
ALTER USER <YOUR_USERNAME> SET RSA_PUBLIC_KEY='<paste content of /tmp/sf_pub.key>';

# 4. Add to .env:
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.pem
────────────────────────────────────────────────────

Usage:
    python cortex_agent.py --question "What is the Grade A pass rate by borough?"
    python cortex_agent.py --demo        # 3-question progressive context demo
    python cortex_agent.py               # interactive multi-turn session
"""

from __future__ import annotations

import argparse
import hashlib
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterator

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # SiS runtime — python-dotenv is not installed; no .env file to load

# ── Config ────────────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"].upper()
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "RESTAURANT_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "RESTAURANT_INTELLIGENCE")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "RESTAURANT_LOADER")
PRIVATE_KEY_PATH    = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    str(Path.home() / ".ssh" / "snowflake_rsa_key.pem"),
)

# JWT needs uppercase account name without region suffix
# e.g.  abc12345.eu-west-3.aws  →  ABC12345
ACCOUNT_IDENTIFIER = SNOWFLAKE_ACCOUNT.split(".")[0].upper()
DEBUG = False  # set via --debug flag

AGENT_ENDPOINT = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    "/api/v2/cortex/agent:run"
)

# ── Tool definitions ──────────────────────────────────────────────────────────
# tools array — only tool_spec entries, no nested tool_resources
# tool_resources is a separate top-level field in the request payload (keyed by tool name)

CORTEX_TOOLS = [
    {
        "tool_spec": {
            "type": "cortex_analyst_text_to_sql",
            "name": "nyc_inspection_analyst",
        },
    },
    {
        "tool_spec": {
            "type": "cortex_search",
            "name": "nyc_health_docs_search",
        },
    },
]

# Separate top-level dict keyed by tool name — this is what Snowflake actually expects
CORTEX_TOOL_RESOURCES = {
    "nyc_inspection_analyst": {
        "semantic_view": "RESTAURANT_INTELLIGENCE.MARTS.nyc_restaurant_inspections",
        "execution_environment": {
            "type": "warehouse",
            "warehouse": SNOWFLAKE_WAREHOUSE,
        },
    },
    "nyc_health_docs_search": {
        "search_service": "RESTAURANT_INTELLIGENCE.RAW.nyc_health_docs_search",
    },
}

SYSTEM_PROMPT = """You are a NYC public health analytics assistant with access to two tools:

1. nyc_inspection_analyst — queries LIVE INSPECTION DATA (counts, scores, rates, trends, rankings)
2. nyc_health_docs_search — searches NYC HEALTH CODE PDFs (Article 81, Chapter 23, legal definitions, condition levels, enforcement rules, penalty schedules)

STRICT TOOL ROUTING RULES — follow these exactly:
- Questions about NUMBERS, COUNTS, RATES, TRENDS, RANKINGS → use nyc_inspection_analyst ONLY
- Questions about WHAT a violation IS, HOW severity is determined, CONDITION LEVELS, LEGAL DEFINITIONS, ENFORCEMENT THRESHOLDS, CLOSURE TRIGGERS, HEALTH CODE TEXT → use nyc_health_docs_search ONLY
- Questions combining data AND legal context → use BOTH tools

EXAMPLES:
  "How many 04L violations this year?" → nyc_inspection_analyst
  "What is violation 04L?" → nyc_health_docs_search (NOT the inspection table — the database only has a short description, the PDFs have the full legal definition, condition levels I/II/III, and point schedules)
  "Which cuisines have the most 04L violations AND what triggers closure?" → both tools

IMPORTANT: The inspection database has violation CODE and a brief DESCRIPTION column. That is NOT sufficient for questions about condition levels, point values, legal thresholds, or enforcement rules. Those answers only exist in the health code PDFs. Always use nyc_health_docs_search for any question about HOW violations work.

- Scores are penalty-based: LOWER = BETTER (Grade A = 0-13 pts, Grade C = 28+)
- Always cite your source: "According to Article 81..." or "Based on inspection data..."
- Keep responses concise and useful to a food safety regulator"""

DEMO_QUESTIONS = [
    # Q1: Cortex Analyst only (structured data)
    "What is the Grade A pass rate by borough for the last 12 months?",
    # Q2: Cortex Search only (document lookup)
    "What exactly is violation 04L and how are its condition levels determined?",
    # Q3: Both tools — the article's money shot
    (
        "Which cuisines have the highest rate of 04L violations this year, "
        "and what does the health code say about enforcement thresholds "
        "that trigger closure for this violation?"
    ),
]


# ── JWT authentication ────────────────────────────────────────────────────────

def _load_private_key(path: str):
    key_path = Path(path).expanduser()
    if not key_path.exists():
        raise FileNotFoundError(
            f"Private key not found: {key_path}\n\n"
            "One-time setup:\n"
            "  openssl genrsa -out ~/.ssh/snowflake_rsa_key.pem 2048\n"
            "  openssl rsa -in ~/.ssh/snowflake_rsa_key.pem -pubout \\\n"
            "    | grep -v 'PUBLIC KEY' | tr -d '\\n' > /tmp/sf_pub.key\n"
            "  # In Snowflake worksheet:\n"
            "  ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<content of /tmp/sf_pub.key>';\n"
            "  # In .env:\n"
            "  SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.pem"
        )
    with open(key_path, "rb") as f:
        return serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )


def _public_key_fingerprint(private_key) -> str:
    pub_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return "SHA256:" + base64.b64encode(hashlib.sha256(pub_bytes).digest()).decode()


def get_jwt_token() -> str:
    """Generates a signed JWT for the Cortex Agent REST API."""
    private_key  = _load_private_key(PRIVATE_KEY_PATH)
    fingerprint  = _public_key_fingerprint(private_key)
    now          = datetime.now(timezone.utc)

    payload = {
        "iss": f"{ACCOUNT_IDENTIFIER}.{SNOWFLAKE_USER}.{fingerprint}",
        "sub": f"{ACCOUNT_IDENTIFIER}.{SNOWFLAKE_USER}",
        "iat": now,
        "exp": now + timedelta(minutes=59),
    }

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return jwt.encode(payload, private_key_pem, algorithm="RS256")


# ── Snowflake SQL execution ───────────────────────────────────────────────────

def _execute_sql(sql: str) -> str:
    """
    Execute a SQL query via snowflake-connector and return results as
    a compact markdown table string to feed back to the agent.
    Uses key-pair auth — password auth triggers MFA on accounts where it is enforced.
    """
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
    )
    import snowflake.connector

    with open(Path(PRIVATE_KEY_PATH).expanduser(), "rb") as f:
        _pk = load_pem_private_key(f.read(), password=None, backend=default_backend())
    private_key_bytes = _pk.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )

    conn = snowflake.connector.connect(
        account     = SNOWFLAKE_ACCOUNT,
        user        = os.environ["SNOWFLAKE_USER"],
        private_key = private_key_bytes,
        warehouse   = SNOWFLAKE_WAREHOUSE,
        database    = SNOWFLAKE_DATABASE,
        role        = SNOWFLAKE_ROLE,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        if not rows:
            return "Query returned no rows."

        # Build a compact markdown table (max 50 rows to stay within context)
        header = " | ".join(cols)
        sep    = " | ".join(["---"] * len(cols))
        lines  = [header, sep]
        for row in rows[:50]:
            lines.append(" | ".join(
                str(round(v, 2) if isinstance(v, float) else v) for v in row
            ))
        if len(rows) > 50:
            lines.append(f"... ({len(rows)} rows total, showing first 50)")
        return "\n".join(lines)
    finally:
        conn.close()


def _parse_sse(resp) -> list[dict]:
    """Consume a streaming SSE response and return all parsed event dicts."""
    events = []
    event_type = None
    for line in resp.iter_lines():
        if not line:
            continue
        raw = line.decode("utf-8")
        if raw.startswith("event: "):
            event_type = raw[7:].strip()
            continue
        if not raw.startswith("data: "):
            continue
        data = raw[6:].strip()
        if data in ("[DONE]", ""):
            break
        try:
            event = json.loads(data)
            event["_sse_event"] = event_type
            events.append(event)
        except json.JSONDecodeError:
            continue
    return events


# ── Agent API call ────────────────────────────────────────────────────────────

def call_cortex_agent(
    question: str,
    token: str,
    conversation_history: list[dict] | None = None,
) -> Iterator[str]:
    """
    Agentic loop for Snowflake Cortex Agent.

    The cortex_analyst_text_to_sql tool is CLIENT-SIDE:
      1. We send the question → agent calls Cortex Analyst → returns SQL
      2. We execute the SQL against Snowflake
      3. We send the results back → agent synthesises the final answer

    This loop runs up to MAX_TURNS iterations to handle multi-step reasoning.
    """
    MAX_TURNS = 5
    messages = (conversation_history or []) + [
        {"role": "user", "content": [{"type": "text", "text": question}]}
    ]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json, text/event-stream",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    for turn in range(MAX_TURNS):
        payload = {
            "models":         {"orchestration": "claude-4-sonnet"},
            "tools":          CORTEX_TOOLS,
            "tool_resources": CORTEX_TOOL_RESOURCES,
            "messages":       messages,
            "instructions": {
                "system": SYSTEM_PROMPT,
                "orchestration": (
                    "Use nyc_inspection_analyst ONLY for quantitative questions: "
                    "counts, rates, scores, trends, rankings, comparisons across time or geography. "
                    "Use nyc_health_docs_search ONLY for qualitative questions: "
                    "violation definitions, condition levels, point schedules, legal thresholds, "
                    "enforcement rules, closure triggers, health code text, inspection procedures. "
                    "The inspection database has only a brief description per violation code — "
                    "it does NOT contain condition levels or enforcement rules. "
                    "For those, you MUST use nyc_health_docs_search. "
                    "Use both tools when the question asks for data AND legal/procedural context."
                ),
            },
        }

        with requests.post(
            AGENT_ENDPOINT, headers=headers, json=payload, stream=True, timeout=180
        ) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}:\n{resp.text[:500]}")

            events = _parse_sse(resp)

        if DEBUG:
            for ev in events:
                print(f"\n[DEBUG turn={turn} event={ev.get('_sse_event')}] "
                      f"{json.dumps({k:v for k,v in ev.items() if k!='_sse_event'})[:1000]}",
                      file=sys.stderr)

        # ── Process SSE events using the new per-event schema ────────────────
        #
        # New event format (post-Sept 2025):
        #   response.text.delta      → {"content_index": N, "text": "chunk"}
        #   response.tool_use        → {"name": "...", "tool_use_id": "...",
        #                               "input": {...}, "client_side_execute": bool}
        #   response.tool_result     → {"content": [{"json": {...}}], ...}
        #
        # cortex_analyst_text_to_sql is CLIENT-SIDE (client_side_execute=True):
        #   we receive the SQL in response.tool_use.input, execute it, and
        #   send results back as a tool_result message to continue the loop.
        #
        # cortex_search is SERVER-SIDE (client_side_execute=False):
        #   Snowflake executes it internally; we just wait for the text output.

        pending_tool_results = []   # (tool_use_id, sql, tool_name)
        text_buf = {}               # content_index → accumulated text
        analyst_tool_uses = {}      # tool_use_id → {"name": ..., "input": ...}

        for ev in events:
            sse_event = ev.get("_sse_event", "")

            # ── Streaming text chunks → collect per content_index ──────────
            if sse_event == "response.text.delta":
                idx = ev.get("content_index", 0)
                text_buf[idx] = text_buf.get(idx, "") + ev.get("text", "")

            # ── Tool invocation ────────────────────────────────────────────
            elif sse_event == "response.tool_use":
                if ev.get("client_side_execute", False):
                    # Cortex Analyst — we must execute the SQL ourselves
                    analyst_tool_uses[ev["tool_use_id"]] = {
                        "name":  ev.get("name", "analyst"),
                        "input": ev.get("input", {}),
                    }
                # else: server-side (Cortex Search) — nothing to do

            # ── Client-side tool result: extract SQL from Cortex Analyst ───
            elif sse_event == "response.tool_result":
                # Only present for client-side tools (Cortex Analyst)
                for item in ev.get("content", []):
                    if item.get("type") == "json":
                        payload_json = item.get("json", {})
                        sql   = payload_json.get("sql", "")
                        interp = payload_json.get("text", "")
                        # Match back to the tool_use to get tool_use_id & name
                        # The tool_result event contains tool_use_id directly
                        tuid = ev.get("tool_use_id", "")
                        tool_name = analyst_tool_uses.get(tuid, {}).get("name", "analyst")
                        if sql and tuid:
                            if interp:
                                yield f"\n🔧 [{tool_name}] {interp}\n\n"
                            else:
                                yield f"\n🔧 [{tool_name}] Executing query...\n\n"
                            pending_tool_results.append((tuid, sql, tool_name))

        # Yield accumulated text in content_index order
        for idx in sorted(text_buf.keys()):
            txt = text_buf[idx].strip()
            if txt:
                yield txt + "\n"

        # If there are SQL queries to execute, run them and continue the loop
        if pending_tool_results:
            # Execute each SQL and add results as user tool_result messages
            tool_result_content = []
            for tool_use_id, sql, tool_name in pending_tool_results:
                try:
                    result_table = _execute_sql(sql)
                    yield f"📊 Results:\n```\n{result_table}\n```\n\n"
                    result_text = f"SQL execution results:\n{result_table}"
                    status = "success"
                except Exception as exc:
                    result_text = f"SQL execution failed: {exc}"
                    status = "error"
                    yield f"⚠  SQL error: {exc}\n"

                tool_result_content.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": [{"type": "text", "text": result_text}],
                    "status": status,
                })

            messages.append({
                "role": "user",
                "content": tool_result_content,
            })
            # Loop continues → agent synthesises final answer

        else:
            # No pending SQL — agent is done
            break


# ── Run modes ─────────────────────────────────────────────────────────────────

def run_interactive(token: str) -> None:
    history: list[dict] = []
    print("\n" + "═" * 70)
    print("  NYC Restaurant Intelligence — Cortex Agent  (multi-turn)")
    print("  Type 'exit' to quit.")
    print("═" * 70 + "\n")

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break
        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            break

        print("\nAgent: ", end="", flush=True)
        full_response = ""
        try:
            for chunk in call_cortex_agent(question, token, history):
                print(chunk, end="", flush=True)
                full_response += chunk
        except RuntimeError as exc:
            print(f"\n⚠  {exc}")
            continue
        print("\n")

        history.extend([
            {"role": "user",      "content": [{"type": "text", "text": question}]},
            {"role": "assistant", "content": [{"type": "text", "text": full_response}]},
        ])


def run_demo(token: str) -> None:
    labels = [
        "Structured data only  → Cortex Analyst",
        "Document search only  → Cortex Search",
        "Both tools combined   → the money shot",
    ]
    print("\n" + "═" * 70)
    print("  Progressive Context Demo — Cortex Analyst + Cortex Search")
    print("═" * 70)

    for i, (question, label) in enumerate(zip(DEMO_QUESTIONS, labels), start=1):
        print(f"\n{'─' * 70}")
        print(f"  Q{i}: {label}")
        print(f"{'─' * 70}")
        print(f"You: {question}\n")
        print("Agent: ", end="", flush=True)
        try:
            for chunk in call_cortex_agent(question, token):
                print(chunk, end="", flush=True)
        except RuntimeError as exc:
            print(f"\n⚠  {exc}")
        print("\n")
        if i < len(DEMO_QUESTIONS):
            time.sleep(1)

    print("═" * 70 + "\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NYC Restaurant Intelligence — Cortex Agent demo"
    )
    mode = parser.add_mutually_exclusive_group()
    parser.add_argument("--debug", action="store_true", help="Print raw SSE events to stderr (for troubleshooting)")
    mode.add_argument("--demo",     action="store_true",
                      help="Run the 3-question progressive context demo")
    mode.add_argument("--question", "-q", type=str, default=None,
                      help="Ask a single question non-interactively")
    args = parser.parse_args()
    global DEBUG
    DEBUG = getattr(args, "debug", False)

    print("Generating JWT … ", end="", flush=True)
    try:
        token = get_jwt_token()
        print("✅")
    except FileNotFoundError as exc:
        print(f"\n\n{exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"\n⚠  {exc}")
        sys.exit(1)

    if args.demo:
        run_demo(token)
    elif args.question:
        print(f"\nYou: {args.question}\nAgent: ", end="", flush=True)
        try:
            for chunk in call_cortex_agent(args.question, token):
                print(chunk, end="", flush=True)
        except RuntimeError as exc:
            print(f"\n⚠  {exc}")
        print("\n")
    else:
        run_interactive(token)


if __name__ == "__main__":
    main()