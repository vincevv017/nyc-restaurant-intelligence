"""
table_aware_extraction.py
────────────────────────────────────────────────────────────────────────────────
Drop-in replacement for the extract_pdf_pages() function in load_documents.py.

WHY THIS EXISTS
────────────────────────────────────────────────────────────────────────────────
Cortex Search uses a two-stage retrieval pipeline: embedding retrieval followed
by a cross-encoder reranker. Linearized PDF tables (flattened rows like
"165°F for 15 seconds ... 7 8 9 10 28") score well on embedding retrieval but
poorly on the reranker, because cross-encoders are trained on natural language
passages, not flattened cell grids.


THE FIX
────────────────────────────────────────────────────────────────────────────────
Instead of treating every page identically with extract_text(), this module:
  1. Detects table pages using pdfplumber's find_tables()
  2. For prose pages: extracts text normally (unchanged behavior)
  3. For table pages: extracts structured table data with extract_tables(),
     then converts each row into narrative prose that rerankers can score well

INTEGRATION
────────────────────────────────────────────────────────────────────────────────
In load_documents.py, replace:

    from table_aware_extraction import extract_pdf_pages_table_aware

And in the main loop:

    for page_num, page_text in extract_pdf_pages_table_aware(pdf_bytes):
        page_chunks = chunk_text(page_text, source_page=page_num)
        ...

Everything else (chunking, loading, OpenLineage) stays the same.
────────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import io
import re
from typing import Iterator

import pdfplumber

# ── Condition level labels ───────────────────────────────────────────────────
CONDITION_LABELS = ["I", "II", "III", "IV", "V"]


# ── Appendix detection ───────────────────────────────────────────────────────

class AppendixTracker:
    """
    Tracks which appendix we're in as we scan pages sequentially.
    Once we see an appendix header, all subsequent table pages belong to it
    until we see a different appendix header.
    """

    # Patterns that identify each appendix
    # re.DOTALL required because extract_text() puts headers on separate lines
    APPENDIX_PATTERNS = [
        ("23-C-unscored", re.compile(
            r"APPENDIX\s+23-C.*UNSCORED\s+VIOLATIONS", re.IGNORECASE | re.DOTALL
        )),
        ("23-C-scored", re.compile(
            r"APPENDIX\s+23-C.*PENALTY\s+SCHEDULE", re.IGNORECASE | re.DOTALL
        )),
        ("23-B", re.compile(
            r"APPENDIX\s+23-B.*GUIDE\s+TO\s+CONDITIONS", re.IGNORECASE | re.DOTALL
        )),
        ("23-A", re.compile(
            r"APPENDIX\s+23-A.*INSPECTION\s+WORKSHEET", re.IGNORECASE | re.DOTALL
        )),
    ]

    # Section headers within appendices (for context in narration)
    SECTION_HEADERS = [
        "Time and Temperature Control for Safety",
        "Food Source",
        "Food Protection",
        "Facility Design and Construction",
        "Food Worker Hygiene and Other Food Protection",
        "Other Criticals",
        "Garbage, Waste Disposal and Pest Management",
        "General Violations",
    ]

    def __init__(self):
        self.current_appendix: str | None = None
        self.current_section: str | None = None

    def update(self, page_text: str) -> None:
        """Update appendix/section state from page content."""
        # Check for appendix headers (check all, take the first match)
        for appendix_id, pattern in self.APPENDIX_PATTERNS:
            if pattern.search(page_text):
                self.current_appendix = appendix_id
                self.current_section = None  # reset section on new appendix
                break

        # Check for section headers within the current appendix
        for header in self.SECTION_HEADERS:
            if header in page_text:
                self.current_section = header
                break

    def get_context_prefix(self) -> str:
        """Build a narrative prefix for the current appendix/section."""
        parts = []

        if self.current_appendix == "23-A":
            parts.append(
                "Appendix 23-A of Chapter 23: Food Service Establishment "
                "Inspection Worksheet — violation codes, condition levels "
                "(I through V), and point scores."
            )
        elif self.current_appendix == "23-B":
            parts.append(
                "Appendix 23-B of Chapter 23: Inspection Scoring Parameters — "
                "a guide to condition levels with examples for each violation."
            )
        elif self.current_appendix == "23-C-scored":
            parts.append(
                "Appendix 23-C of Chapter 23: Penalty Schedule for scored "
                "violations — fine amounts by violation code, Health Code "
                "citation, and condition level."
            )
        elif self.current_appendix == "23-C-unscored":
            parts.append(
                "Appendix 23-C of Chapter 23: Penalty Schedule for unscored "
                "violations — fines for tobacco, signage, permits, and other "
                "non-food-safety violations."
            )

        if self.current_section:
            parts.append(f"Section: {self.current_section}.")

        return " ".join(parts)


# ── Score parsing ────────────────────────────────────────────────────────────

def _parse_scores(score_str: str) -> dict[str, int]:
    """Parse a score string like '7 8 9 10 28' into {condition: points}."""
    if not score_str:
        return {}
    tokens = score_str.replace("–", "-").replace("—", "-").split()
    result = {}
    for i, token in enumerate(tokens):
        if i < len(CONDITION_LABELS):
            clean = token.strip()
            if clean not in ("-", "–", "—", "") and clean.isdigit():
                result[CONDITION_LABELS[i]] = int(clean)
    return result


def _extract_violation_code(text: str) -> tuple[str, str] | None:
    """Extract violation code and description from a cell.
    Returns (code, description) or None if no code found."""
    match = re.match(r"^(\d{1,2}[A-Z]\*?\+?)\s+(.+)", text.strip(), re.DOTALL)
    if match:
        return match.group(1), match.group(2).replace("\n", " ").strip()
    return None


# ── Row narration per appendix type ──────────────────────────────────────────

def _narrate_23a_row(row: list) -> str | None:
    """Narrate a row from Appendix 23-A (scoring worksheet).
    Row structure: [code+description, scores, ...]"""
    cell0 = (row[0] or "").replace("\n", " ").strip()
    parsed = _extract_violation_code(cell0)
    if not parsed:
        return None

    code, desc = parsed
    is_critical = "*" in code
    is_preop = "+" in code

    # Find scores — they could be in any column
    scores = {}
    for cell in row[1:]:
        cell_text = (cell or "").strip()
        candidate = _parse_scores(cell_text)
        if candidate:
            scores = candidate
            break

    parts = [f"Violation {code}"]
    if is_critical:
        parts.append("(Critical/Public Health Hazard)")
    elif is_preop:
        parts.append("(Pre-permit Serious)")
    parts.append(f": {desc}")

    if scores:
        score_items = [f"Condition {k} = {v} points" for k, v in scores.items()]
        parts.append(f" Point scores: {', '.join(score_items)}.")
    else:
        # Some violations only score at condition IV/V
        parts.append(" (Scored only at highest condition levels.)")

    return " ".join(parts)


def _narrate_23b_row(row: list) -> str | None:
    """Narrate a row from Appendix 23-B (condition level guide).
    Row structure: [code, description, cond_I, cond_II, cond_III, cond_IV, cond_V]"""
    if not row or len(row) < 3:
        return None

    code = (row[0] or "").strip()
    if not re.match(r"^\d{1,2}[A-Z]", code):
        return None

    desc = (row[1] or "").replace("\n", " ").strip()

    parts = [f"Violation {code}: {desc}"]

    for i, label in enumerate(CONDITION_LABELS):
        col_idx = i + 2
        if col_idx < len(row) and row[col_idx]:
            cond_text = (row[col_idx] or "").replace("\n", " ").strip()
            if cond_text and cond_text not in ("—", "–", "-"):
                parts.append(f"Condition {label}: {cond_text}")

    result = " ".join(parts)
    return result if len(result) > 50 else None  # skip near-empty rows


def _narrate_23c_row(row: list) -> str | None:
    """Narrate a row from Appendix 23-C (penalty schedule).
    Row structure varies: [code, citation, category, description, ...]"""
    if not row or len(row) < 4:
        return None

    code = (row[0] or "").strip()
    if not re.match(r"^\d{2}[A-Z]", code):
        return None

    citation = (row[1] or "").replace("\n", " ").strip()
    category = (row[2] or "").replace("\n", " ").strip()
    desc = (row[3] or "").replace("\n", " ").strip()

    # Remaining columns are penalty amounts
    penalties = []
    for cell in row[4:]:
        val = (cell or "").strip()
        if val and val not in ("—", "–", "-"):
            penalties.append(val)

    parts = [f"Violation {code} ({citation}): {desc}."]
    if category:
        parts.append(f"Category: {category}.")
    if penalties:
        parts.append(f"Penalties: {', '.join(penalties)}.")

    result = " ".join(parts)
    return result if len(result) > 50 else None


# ── Main extraction function ─────────────────────────────────────────────────

def extract_pdf_pages_table_aware(
    pdf_bytes: bytes,
) -> Iterator[tuple[int, str]]:
    """
    Drop-in replacement for extract_pdf_pages() that handles tables properly.

    For prose pages: returns extract_text() as before.
    For table pages: converts structured table data into narrative prose
    that cross-encoder rerankers can score well.

    Yields (page_number, page_text) — same interface as the original.
    """
    tracker = AppendixTracker()

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            text = text.strip()

            if not text:
                continue

            # Update appendix/section tracking
            tracker.update(text)

            # Check for tables
            found_tables = page.find_tables()

            if not found_tables:
                # ── Prose page: yield as-is (unchanged behavior) ─────────
                yield i, text
                continue

            # ── Table page: extract structured data and narrate ──────────
            extracted_tables = page.extract_tables()
            if not extracted_tables:
                # find_tables found something but extract_tables couldn't parse
                yield i, text
                continue

            # Build narrated content for this page
            narrated_parts = []

            # Start with contextual prefix
            prefix = tracker.get_context_prefix()
            if prefix:
                narrated_parts.append(prefix)

            # Also capture any non-table text on the page (headers, notes)
            # We'll extract it from the raw text and include section headers
            violation_codes_on_page = re.findall(
                r"\b(\d{1,2}[A-Z]\*?\+?)\b", text
            )
            unique_codes = list(dict.fromkeys(violation_codes_on_page))
            if unique_codes:
                narrated_parts.append(
                    f"Violation codes on this page: {', '.join(unique_codes[:15])}."
                )

            # Narrate each table row based on current appendix
            for table in extracted_tables:
                for row in table:
                    narrated_row = None

                    if tracker.current_appendix == "23-A":
                        narrated_row = _narrate_23a_row(row)
                    elif tracker.current_appendix == "23-B":
                        narrated_row = _narrate_23b_row(row)
                    elif tracker.current_appendix in ("23-C-scored", "23-C-unscored"):
                        narrated_row = _narrate_23c_row(row)

                    if narrated_row:
                        narrated_parts.append(narrated_row)

            # If narration produced content, yield it
            if len(narrated_parts) > 1:  # more than just the prefix
                narrated_text = "\n\n".join(narrated_parts)
                yield i, narrated_text
            else:
                # Fallback: yield raw text (for tables we couldn't parse)
                yield i, text


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Run standalone to test narration against a PDF:
        python table_aware_extraction.py path/to/chapter23.pdf
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: python table_aware_extraction.py <pdf_path>")
        print("  Tests table-aware extraction and shows output per page")
        sys.exit(1)

    pdf_path = sys.argv[1]
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    total_prose = 0
    total_table = 0
    total_words = 0

    for page_num, page_text in extract_pdf_pages_table_aware(pdf_bytes):
        words = len(page_text.split())
        total_words += words

        # Detect if this was narrated (starts with "Appendix")
        is_table = page_text.startswith("Appendix")
        if is_table:
            total_table += 1
        else:
            total_prose += 1

        print(f"\n{'='*70}")
        print(f"PAGE {page_num} ({'TABLE → narrated' if is_table else 'PROSE'}) "
              f"— {words} words")
        print(f"{'='*70}")
        # Show first 400 chars
        print(page_text[:400])
        if len(page_text) > 400:
            print(f"... ({len(page_text) - 400} more chars)")

    print(f"\n{'═'*70}")
    print(f"  Prose pages:    {total_prose}")
    print(f"  Table pages:    {total_table}")
    print(f"  Total words:    {total_words:,}")
    print(f"{'═'*70}")
