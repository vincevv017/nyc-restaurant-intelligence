#!/usr/bin/env python3
"""
agent_with_memory.py
─────────────────────────────────────────────────────────────────────────────
CLI Cortex Agent with persistent user memory.

Extends cortex_agent.py (Phase 3) with:
  - MemoryManager  : loads user facts from Snowflake at session start,
                     injects them into the user question, and saves new facts
                     when the agent requests personal information
  - ResponseParser : extracts 📅 data freshness and 🔍 filter transparency
                     blocks from every agent response and displays them
                     distinctly in the terminal

Authentication: JWT key-pair (same setup as Phase 3 cortex_agent.py).

Usage:
    cd memory
    python agent_with_memory.py                        # interactive session
    python agent_with_memory.py --user analyst_01      # explicit user identity
    python agent_with_memory.py --show-memory          # print stored facts and exit
    python agent_with_memory.py --clear-memory         # wipe all facts and exit
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# ── Import from sibling cortex/ directory ────────────────────────────────────
# cortex_agent.py lives one level up at ../cortex/cortex_agent.py
_CORTEX_DIR = Path(__file__).parent.parent / "cortex"
if str(_CORTEX_DIR) not in sys.path:
    sys.path.insert(0, str(_CORTEX_DIR))

# Validated against cortex_agent.py:
#   get_jwt_token()  → str
#   call_cortex_agent(question: str, token: str,
#                     conversation_history: list[dict] | None = None) → Iterator[str]
#   SNOWFLAKE_ACCOUNT: str  (module-level constant, line 56)
from cortex_agent import get_jwt_token, call_cortex_agent, SNOWFLAKE_ACCOUNT  # noqa: E402

from memory_manager import MemoryManager       # noqa: E402
from response_parser import parse_response, ParsedResponse  # noqa: E402

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]


# ── Fact-answer guard ─────────────────────────────────────────────────────────

_DELETE_CMD = r"\b(remove|delete|forget|clear|erase)\b.{0,40}"
_DELETE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("home_address",      re.compile(_DELETE_CMD + r"\b(home|home address|address|where i live|my location)\b", re.I)),
    ("work_location",     re.compile(_DELETE_CMD + r"\b(work|office|workplace|work address|work location)\b", re.I)),
    ("neighborhood",      re.compile(_DELETE_CMD + r"\b(neighbourhood|neighborhood|area|district)\b", re.I)),
    ("preferred_borough", re.compile(_DELETE_CMD + r"\b(borough|preferred borough)\b", re.I)),
    ("favorite_cuisine",  re.compile(_DELETE_CMD + r"\b(cuisine|food|favorite cuisine|favourite cuisine)\b", re.I)),
    ("violation_focus",   re.compile(_DELETE_CMD + r"\b(violation|violation code|violation type|violation focus)\b", re.I)),
    ("grade_focus",       re.compile(_DELETE_CMD + r"\b(grade|grade focus|grade filter|grades)\b", re.I)),
    ("inspection_type",   re.compile(_DELETE_CMD + r"\b(inspection type|initial|re-?inspection)\b", re.I)),
    ("score_threshold",   re.compile(_DELETE_CMD + r"\b(score|threshold|score threshold|cutoff)\b", re.I)),
    ("time_window",       re.compile(_DELETE_CMD + r"\b(date range|time (range|window|period|frame)|time window)\b", re.I)),
    ("role",              re.compile(_DELETE_CMD + r"\b(role|job|title|position)\b", re.I)),
    ("organization",      re.compile(_DELETE_CMD + r"\b(org|organization|organisation|company|agency|department|team)\b", re.I)),
]


def _detect_memory_delete(text: str) -> str | None:
    """Return the fact_key to delete if the input is a delete/forget command, else None."""
    for fact_key, pattern in _DELETE_PATTERNS:
        if pattern.search(text):
            return fact_key
    return None


_QUESTION_STARTERS = re.compile(
    r"^(what|how|show|tell|find|which|same|can|could|would|is|are|do|does|did|"
    r"where|when|who|why|get|list|give|help|any|compare|top|best|worst)\b",
    re.I,
)


def _looks_like_fact_answer(text: str) -> bool:
    """
    Return True if the text looks like a direct answer to a personal-info
    question rather than a new question to the agent.

    Two fast heuristics (no LLM required):
      - Contains '?'                       → question, not an answer
      - Starts with a question/directive   → follow-up question, not an answer

    False negatives (e.g. "show me 245 W 107th St") are harmless — the agent
    will just receive a slightly odd question and respond normally.
    """
    if "?" in text:
        return False
    if _QUESTION_STARTERS.match(text.strip()):
        return False
    return True


# ── Terminal formatting helpers ───────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[36m"
YELLOW = "\033[33m"
DIM    = "\033[2m"
GREEN  = "\033[32m"


def _print_metadata(parsed: ParsedResponse) -> None:
    """Print freshness and transparency blocks above the response body."""
    if parsed.freshness_date:
        print(f"\n  {CYAN}📅 Data through: {BOLD}{parsed.freshness_date}{RESET}")

    if parsed.filters:
        print(f"  {YELLOW}🔍 Filters applied:{RESET}")
        for f in parsed.filters:
            print(f"  {DIM}   • {f}{RESET}")

    if parsed.freshness_date or parsed.filters:
        print()  # blank line before body


def _print_memory_state(memory: MemoryManager) -> None:
    if not memory.facts:
        print(f"  {DIM}[No stored memory for this user]{RESET}\n")
        return
    print(f"\n  {GREEN}🧠 Memory loaded ({len(memory.facts)} fact(s)):{RESET}")
    for k, v in memory.facts.items():
        print(f"  {DIM}   • {k.replace('_', ' ')}: {v}{RESET}")
    print()


# ── Core: build question with injected memory context ────────────────────────

def _augment_question(question: str, memory: MemoryManager) -> str:
    """
    Prepend user facts to the question so the agent can answer
    location/preference questions without asking again.

    call_cortex_agent does not accept a system_context parameter — the only
    way to inject memory without modifying cortex_agent.py is to prepend it
    to the question text itself.
    """
    context = memory.to_system_context()
    if context:
        return f"{context}\n\n---\n\nUser question: {question}"
    return question


# ── Interactive session ───────────────────────────────────────────────────────

def run_interactive(user_id: str) -> None:
    print("\n" + "═" * 70)
    print("  NYC Restaurant Intelligence — Cortex Agent + Memory  (multi-turn)")
    print(f"  User: {user_id}")
    print("  Type 'memory' to review stored facts, 'exit' to quit.")
    print("═" * 70 + "\n")

    # ── Auth ──────────────────────────────────────────────────────────────────
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

    # ── Memory ────────────────────────────────────────────────────────────────
    memory = MemoryManager(user_id)
    memory.load()
    _print_memory_state(memory)

    history: list[dict] = []
    pending_memory_key: str | None = None

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
        if question.lower() == "memory":
            _print_memory_state(memory)
            continue

        # ── Capture pending memory key from previous turn ─────────────────────
        if pending_memory_key:
            if _looks_like_fact_answer(question):
                memory.save(
                    key         = pending_memory_key,
                    value       = question,
                    source_turn = question,
                )
                print(f"  {GREEN}💾 Saved: {pending_memory_key} = '{question}'{RESET}\n")
            else:
                # Looks like a follow-up question — skip save, proceed normally
                print(f"  {DIM}(Skipped memory save — looks like a question, not an answer){RESET}\n")
            pending_memory_key = None

        # ── Local memory delete commands (handled without calling the agent) ────
        delete_key = _detect_memory_delete(question)
        if delete_key:
            if delete_key in memory.facts:
                memory.delete(delete_key)
                print(f"\n  {GREEN}🗑  Deleted: {delete_key.replace('_', ' ')}{RESET}\n")
            else:
                print(f"\n  {DIM}(No stored value for '{delete_key.replace('_', ' ')}' — nothing to delete){RESET}\n")
            pending_memory_key = None
            continue

        # ── Call agent — inject memory as question prefix ─────────────────────
        augmented_question = _augment_question(question, memory)

        print(f"\n{BOLD}Agent:{RESET} ", end="", flush=True)

        full_response = ""
        try:
            # Signature: call_cortex_agent(question, token, conversation_history=None)
            # No system_context parameter exists — memory is injected via question prefix
            for chunk in call_cortex_agent(
                question             = augmented_question,
                token                = token,
                conversation_history = history,
            ):
                full_response += chunk
        except RuntimeError as exc:
            print(f"\n⚠  {exc}")
            continue

        # ── Parse and display metadata above body ─────────────────────────────
        parsed = parse_response(full_response)

        print("\r" + " " * 10 + "\r", end="")  # clear "Agent: " cursor line
        _print_metadata(parsed)
        print(f"{BOLD}Agent:{RESET} {parsed.body}\n")

        # ── Update history with the original (un-augmented) question ──────────
        # Store the original question so history reads naturally in future turns
        history.extend([
            {"role": "user",      "content": [{"type": "text", "text": question}]},
            {"role": "assistant", "content": [{"type": "text", "text": parsed.body}]},
        ])

        # ── Check whether the agent requested personal info ───────────────────
        extraction = memory.check_for_memory_request(parsed.body)
        if extraction:
            pending_memory_key = extraction["key"]
            print(f"  {DIM}💾 I'll remember your answer for future sessions.{RESET}\n")


# ── Utility modes ─────────────────────────────────────────────────────────────

def show_memory(user_id: str) -> None:
    memory = MemoryManager(user_id)
    memory.load()
    if not memory.facts:
        print(f"No stored facts for user '{user_id}'.")
        return
    print(f"\nStored facts for '{user_id}':")
    for k, v in memory.facts.items():
        print(f"  {k:<30} {v}")


def clear_memory(user_id: str) -> None:
    memory  = MemoryManager(user_id)
    deleted = memory.clear()
    print(f"Memory cleared for user '{user_id}'. {deleted} fact(s) deleted.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NYC Restaurant Intelligence — Cortex Agent with persistent memory"
    )
    parser.add_argument(
        "--user", "-u",
        default=os.getenv("AGENT_USER_ID", SNOWFLAKE_USER),
        help="User identity for memory scoping (default: SNOWFLAKE_USER from .env)"
    )
    parser.add_argument(
        "--show-memory",
        action="store_true",
        help="Print stored facts for this user and exit"
    )
    parser.add_argument(
        "--clear-memory",
        action="store_true",
        help="Delete all stored facts for this user and exit"
    )
    args = parser.parse_args()

    if args.show_memory:
        show_memory(args.user)
    elif args.clear_memory:
        clear_memory(args.user)
    else:
        run_interactive(args.user)


if __name__ == "__main__":
    main()
