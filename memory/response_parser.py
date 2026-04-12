"""
response_parser.py
─────────────────────────────────────────────────────────────────────────────
Extracts structured metadata blocks that the Cortex Agent injects into every
response (per the agent system prompt spec):

  📅 Data through: <date>          — data freshness, always first if present
  🔍 Filters applied:              — implicit filter transparency block
     - <condition> — <reason>

Returns a ParsedResponse with:
  freshness_date : str | None    e.g. "2025-11-14"
  filters        : list[str]     each filter line stripped of leading " - "
  body           : str           remaining response text with metadata removed

These anchors are guaranteed by the agent system prompt — do not change them
here without updating SYSTEM_PROMPT in cortex/cortex_agent.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class ParsedResponse:
    freshness_date: str | None = None
    filters: list[str] = field(default_factory=list)
    body: str = ""


# Regexes anchored on the emojis the system prompt mandates
_FRESHNESS_RE      = re.compile(r"📅\s*Data through:\s*(.+?)(?:\n|$)", re.IGNORECASE)
_FILTERS_HEADER_RE = re.compile(r"🔍\s*Filters applied:", re.IGNORECASE)
_FILTER_ITEM_RE    = re.compile(r"^\s*[-–]\s+(.+)$")  # leading whitespace optional


def parse_response(text: str) -> ParsedResponse:
    """
    Parse structured metadata from a Cortex Agent response string.

    Strips 📅 freshness and 🔍 filter blocks from the text and returns
    them as structured fields alongside the clean body for display.
    """
    result = ParsedResponse()
    lines  = text.split("\n")
    body_lines: list[str] = []
    in_filters_block = False

    for line in lines:
        # ── Data freshness ────────────────────────────────────────────────────
        m = _FRESHNESS_RE.match(line)
        if m:
            result.freshness_date = m.group(1).strip()
            in_filters_block = False
            continue  # strip from body

        # ── Filters header ────────────────────────────────────────────────────
        if _FILTERS_HEADER_RE.search(line):
            in_filters_block = True
            continue  # strip from body

        # ── Filter bullet items ───────────────────────────────────────────────
        if in_filters_block:
            item = _FILTER_ITEM_RE.match(line)
            if item:
                result.filters.append(item.group(1).strip())
                continue  # strip from body
            if line.strip() == "":
                in_filters_block = False
                continue
            # Non-matching line after header — end of block, keep in body
            in_filters_block = False

        body_lines.append(line)

    result.body = "\n".join(body_lines).strip()
    return result
