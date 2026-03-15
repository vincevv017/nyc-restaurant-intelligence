#!/usr/bin/env python3
"""
load_documents.py
────────────────────────────────────────────────────────────────────────────────
Downloads NYC DOHMH reference documents (PDFs and web pages) directly from
their public URLs, extracts and chunks the text content, and loads it into
Snowflake for indexing by Cortex Search.

This is Phase 3 of the NYC Restaurant Intelligence project — adding an
unstructured document layer on top of the structured star schema. Once loaded,
run setup/03_cortex_search_setup.sql to create the Cortex Search service.

The resulting pipeline:
  NYC DOHMH / Socrata API  ──► RAW.INSPECTIONS_RAW  ──► STAGING  ──► MARTS
  NYC DOHMH PDFs (HTTP)    ──► RAW.DOCUMENT_CHUNKS  ──────────────────────►
                                                              ▼
                                                   Cortex Search Service
                                                              ▼
                                                       Cortex Agent
                                        (Cortex Analyst + Cortex Search)

Chunking strategy:
  - Target chunk size: 800 words (configurable via CHUNK_SIZE)
  - Overlap: 80 words between adjacent chunks (avoids mid-concept splits)
  - Minimum chunk size: 50 words (skips near-empty pages / headers)
  - Each chunk stored with doc_type, source_url, page_number for filtering

OpenLineage:
  After a successful load, emits an OpenLineage COMPLETE event declaring the
  HTTP sources as inputs and RAW.DOCUMENT_CHUNKS as the output.

Usage:
    cd ingestion
    pip install -r requirements.txt
    python load_documents.py

    # Load a single doc type:
    python load_documents.py --doc-type health_code

    # Dry run (download + parse only, no Snowflake writes):
    python load_documents.py --dry-run

    # Skip OpenLineage:
    python load_documents.py --no-lineage
────────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import io
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

import pdfplumber
import requests
import snowflake.connector
from tqdm import tqdm
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "setup"))
import config
from table_aware_extraction import extract_pdf_pages_table_aware

# ── Document catalogue ────────────────────────────────────────────────────────
# Each entry is a public URL. The script downloads it at runtime — no local
# files needed. Add or remove entries freely; the loader handles both PDFs
# and plain-text web pages automatically.

@dataclass
class DocumentSource:
    url:         str
    doc_type:    str    # used as a filter attribute in Cortex Search
    label:       str    # human-readable name for logging
    is_pdf:      bool = True


DOCUMENT_SOURCES: list[DocumentSource] = [
    DocumentSource(
        url="https://www.nyc.gov/assets/doh/downloads/pdf/rii/article81-book.pdf",
        doc_type="health_code",
        label="Article 81 — NYC Food Safety Regulations",
    ),
    DocumentSource(
        url="https://www.nyc.gov/assets/doh/downloads/pdf/about/healthcode/health-code-chapter23.pdf",
        doc_type="inspection_procedures",
        label="Chapter 23 — Inspection Scoring and Procedures",
    ),
    DocumentSource(
        url="http://www.chinatownpartnership.org/pdf/restaurantinspection.pdf",
        doc_type="operator_guide",
        label="What to Expect When You're Inspected (Operator Guide)",
    ),
    DocumentSource(
        url="https://www.nyc.gov/assets/doh/downloads/pdf/rii/inspection-cycle-overview.pdf",
        doc_type="inspection_procedures",
        label="Inspection Cycle Overview — Condition Levels and Enforcement",
    ),
    DocumentSource(
        url="https://www.nyc.gov/assets/doh/downloads/pdf/permit/identify-a-health-inspector.pdf",
        doc_type="enforcement",
        label="How to Identify a Health Inspector",
    ),
    DocumentSource(
        url="https://www.nyc.gov/assets/doh/downloads/pdf/permit/consultative-inspections.pdf",
        doc_type="enforcement",
        label="Consultative Inspections — Pre-Opening Advice",
    ),
]

# ── Chunking configuration ────────────────────────────────────────────────────

CHUNK_SIZE    = 800     # target words per chunk
CHUNK_OVERLAP = 80      # word overlap between adjacent chunks
MIN_CHUNK_WORDS = 50    # discard chunks shorter than this (headers, page numbers)

# ── Snowflake target ──────────────────────────────────────────────────────────

TARGET_TABLE  = "DOCUMENT_CHUNKS"
TARGET_SCHEMA = "RAW"

# ── OpenLineage constants ─────────────────────────────────────────────────────

OL_JOB_NAMESPACE = "nyc_restaurant_intelligence"
OL_JOB_NAME      = "load_documents"


# ── PDF download and text extraction ─────────────────────────────────────────

def download_bytes(url: str, label: str) -> bytes:
    """
    Streams a file from a URL into memory.
    Uses a browser-like User-Agent to avoid 403 responses from some servers.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; NYC-Restaurant-Intelligence/1.0; "
            "+https://github.com/vincevv017/nyc-restaurant-intelligence)"
        )
    }
    print(f"  ⬇  Downloading: {label}")
    print(f"     URL: {url}")

    resp = requests.get(url, headers=headers, timeout=60, stream=True)
    resp.raise_for_status()

    chunks = []
    total  = int(resp.headers.get("content-length", 0))

    with tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        desc="     ",
        leave=False,
        dynamic_ncols=True,
    ) as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            chunks.append(chunk)
            pbar.update(len(chunk))

    return b"".join(chunks)


def extract_pdf_pages(pdf_bytes: bytes) -> Iterator[tuple[int, str]]:
    """
    Yields (page_number, page_text) for each page in a PDF.
    Uses pdfplumber which handles both text-layer PDFs and scanned documents.
    Page numbers are 1-indexed to match human-readable PDF page references.
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            text = text.strip()
            if text:
                yield i, text


def chunk_text(text: str, source_page: int) -> list[dict]:
    """
    Splits a text block into overlapping word-window chunks.

    Returns a list of dicts with:
      - chunk_text  : the text content
      - page_number : source page (for citation)
      - chunk_index : position within the page (0-based)
    """
    words = text.split()
    if len(words) < MIN_CHUNK_WORDS:
        return []

    chunks = []
    i      = 0
    idx    = 0

    while i < len(words):
        chunk_words = words[i : i + CHUNK_SIZE]
        chunk_text  = " ".join(chunk_words)

        if len(chunk_words) >= MIN_CHUNK_WORDS:
            chunks.append({
                "chunk_text":  chunk_text,
                "page_number": source_page,
                "chunk_index": idx,
                "word_count":  len(chunk_words),
            })
            idx += 1

        i += CHUNK_SIZE - CHUNK_OVERLAP

    return chunks


# ── Snowflake setup ───────────────────────────────────────────────────────────

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS
  RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE} (
    doc_id        VARCHAR        NOT NULL,
    source_url    VARCHAR        NOT NULL,
    doc_type      VARCHAR        NOT NULL,
    doc_label     VARCHAR,
    page_number   INTEGER,
    chunk_index   INTEGER,
    word_count    INTEGER,
    chunk_text    VARCHAR        NOT NULL,
    loaded_at     TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
"""

def ensure_table(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Creates the document chunks table if it does not exist."""
    conn.cursor().execute(CREATE_TABLE_SQL)
    print(f"   ✅ Table RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE} ready")


# ── Snowflake load ────────────────────────────────────────────────────────────

def load_chunks_to_snowflake(
    chunks: list[dict],
    source: DocumentSource,
    conn: snowflake.connector.SnowflakeConnection,
) -> int:
    """
    Inserts document chunks into RAW.DOCUMENT_CHUNKS.
    Deletes existing rows for the same source_url before inserting so the
    function is idempotent — re-running for the same document replaces it.

    Returns the number of rows inserted.
    """
    cur = conn.cursor()

    # Idempotent: remove previous version of this document
    cur.execute(
        f"DELETE FROM RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE} "
        "WHERE source_url = %s",
        (source.url,),
    )

    rows = [
        (
            str(uuid.uuid4()),    # doc_id
            source.url,           # source_url
            source.doc_type,      # doc_type
            source.label,         # doc_label
            c["page_number"],     # page_number
            c["chunk_index"],     # chunk_index
            c["word_count"],      # word_count
            c["chunk_text"],      # chunk_text
        )
        for c in chunks
    ]

    cur.executemany(
        f"INSERT INTO RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE} "
        "(doc_id, source_url, doc_type, doc_label, page_number, "
        " chunk_index, word_count, chunk_text) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        rows,
    )

    return len(rows)


# ── OpenLineage ───────────────────────────────────────────────────────────────

def emit_openlineage_event(
    conn: snowflake.connector.SnowflakeConnection,
    sources_loaded: list[DocumentSource],
    total_chunks: int,
    run_id: str,
    event_time: str,
) -> None:
    """
    Posts an OpenLineage COMPLETE event declaring all HTTP sources as inputs
    and RAW.DOCUMENT_CHUNKS as the output.

    Authentication reuses the open connector session token — no additional
    credentials required. Failures are non-blocking warnings.
    """
    try:
        if conn.rest is None:
            raise RuntimeError("Snowflake session has no REST client — cannot retrieve token for OpenLineage.")
        token = conn.rest.token
    except Exception:
        print("   ⚠  Could not retrieve session token — skipping lineage emission.")
        return

    output_namespace = (
        f"snowflake://{config.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    )
    output_name = (
        f"RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE}"
    )

    inputs = [
        {
            "namespace": src.url.split("/")[0] + "//" + src.url.split("/")[2],
            "name":      "/".join(src.url.split("/")[3:]),
            "facets": {
                "dataSource": {
                    "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                    "name": src.label,
                    "uri":  src.url,
                },
                "documentation": {
                    "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json",
                    "description": f"NYC DOHMH reference document — {src.doc_type}",
                },
            },
        }
        for src in sources_loaded
    ]

    payload = {
        "eventType":  "COMPLETE",
        "eventTime":  event_time,
        "schemaURL":  "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "run": {
            "runId": run_id,
            "facets": {
                "processing_engine": {
                    "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json",
                    "name":    "snowflake-connector-python",
                    "version": snowflake.connector.__version__,
                }
            },
        },
        "job": {
            "namespace": OL_JOB_NAMESPACE,
            "name":      OL_JOB_NAME,
            "facets": {
                "documentation": {
                    "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
                    "description": (
                        "Downloads NYC DOHMH reference PDFs from public URLs, "
                        "extracts and chunks text, loads into Snowflake for Cortex Search indexing."
                    ),
                },
                "sourceCode": {
                    "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
                    "type": "git",
                    "url":  "https://github.com/vincevv017/nyc-restaurant-intelligence",
                },
            },
        },
        "inputs": inputs,
        "outputs": [
            {
                "namespace": output_namespace,
                "name":      output_name,
                "facets": {
                    "schema": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json",
                        "fields": [
                            {"name": "DOC_ID",      "type": "VARCHAR"},
                            {"name": "SOURCE_URL",  "type": "VARCHAR"},
                            {"name": "DOC_TYPE",    "type": "VARCHAR"},
                            {"name": "DOC_LABEL",   "type": "VARCHAR"},
                            {"name": "PAGE_NUMBER", "type": "INTEGER"},
                            {"name": "CHUNK_INDEX", "type": "INTEGER"},
                            {"name": "WORD_COUNT",  "type": "INTEGER"},
                            {"name": "CHUNK_TEXT",  "type": "VARCHAR"},
                            {"name": "LOADED_AT",   "type": "TIMESTAMP_TZ"},
                        ],
                    },
                    "outputStatistics": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
                        "rowCount": total_chunks,
                    },
                },
            }
        ],
    }

    endpoint = (
        f"https://{config.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
        "/api/v2/lineage/openlineage/v1/lineage"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-Snowflake-Authorization-Token-Type": "OAUTH",
    }

    print("\n📡 Emitting OpenLineage event …")
    print(f"   Run ID  : {run_id}")
    print(f"   Inputs  : {len(inputs)} document sources")
    print(f"   Output  : {output_name}")

    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=15)
        if resp.status_code in (200, 201, 202):
            print(f"   ✅ OpenLineage event accepted (HTTP {resp.status_code})")
        else:
            print(f"   ⚠  HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        print(f"   ⚠  OpenLineage emission failed: {exc}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Load NYC DOHMH reference documents to Snowflake for Cortex Search"
    )
    parser.add_argument(
        "--doc-type",
        default=None,
        help="Load only sources matching this doc_type "
             "(e.g. health_code, inspection_procedures, operator_guide, enforcement)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and parse only — do NOT write to Snowflake",
    )
    parser.add_argument(
        "--no-lineage",
        action="store_true",
        help="Skip OpenLineage event emission after load",
    )
    args = parser.parse_args()

    run_id     = str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).isoformat()
    start      = datetime.now(timezone.utc)

    # Filter sources if --doc-type was specified
    sources = DOCUMENT_SOURCES
    if args.doc_type:
        sources = [s for s in DOCUMENT_SOURCES if s.doc_type == args.doc_type]
        if not sources:
            print(f"⚠  No sources found with doc_type='{args.doc_type}'.")
            print(f"   Available types: {sorted({s.doc_type for s in DOCUMENT_SOURCES})}")
            return

    print("=" * 70)
    print("  NYC Restaurant Intelligence — Document Loader")
    print(f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Run ID  : {run_id}")
    print(f"  Sources : {len(sources)} document(s) to process")
    if args.dry_run:
        print("  Mode    : DRY RUN (no Snowflake writes)")
    print("=" * 70 + "\n")

    # Connect once, reuse for all inserts
    conn: snowflake.connector.SnowflakeConnection | None = None
    if not args.dry_run:
        print("Connecting to Snowflake …")
        conn = snowflake.connector.connect(
            account   = config.SNOWFLAKE_ACCOUNT,
            user      = config.SNOWFLAKE_USER,
            password  = config.SNOWFLAKE_PASSWORD,
            warehouse = config.SNOWFLAKE_WAREHOUSE,
            database  = config.SNOWFLAKE_DATABASE,
            schema    = TARGET_SCHEMA,
            role      = config.SNOWFLAKE_ROLE,
            session_parameters={"QUERY_TAG": "nyc_document_loader"},
        )
        ensure_table(conn)

    sources_loaded: list[DocumentSource] = []
    total_chunks    = 0
    total_pages     = 0

    for source in sources:
        print(f"\n{'─' * 60}")
        print(f"📄 {source.label}")
        print(f"   Type: {source.doc_type}")

        # Download
        try:
            pdf_bytes = download_bytes(source.url, source.label)
        except requests.exceptions.RequestException as exc:
            print(f"   ✗ Download failed: {exc}")
            print("   Skipping this document.")
            continue

        print(f"   Downloaded: {len(pdf_bytes) / 1024:.1f} KB")

        # Extract and chunk
        all_chunks: list[dict] = []
        page_count = 0

        for page_num, page_text in extract_pdf_pages_table_aware(pdf_bytes):
            page_chunks = chunk_text(page_text, source_page=page_num)
            all_chunks.extend(page_chunks)
            page_count += 1

        total_pages += page_count
        print(f"   Pages    : {page_count}")
        print(f"   Chunks   : {len(all_chunks)}")

        if not all_chunks:
            print("   ⚠  No text extracted — PDF may be scanned or empty.")
            print("      Consider adding OCR support (pytesseract) for this document.")
            continue

        if args.dry_run:
            # Show a sample chunk
            sample = all_chunks[0]
            preview = sample["chunk_text"][:300].replace("\n", " ")
            print(f"   Sample chunk (page {sample['page_number']}, "
                  f"{sample['word_count']} words):")
            print(f"   {preview}…")
            continue

        # Load
        assert conn is not None
        inserted = load_chunks_to_snowflake(all_chunks, source, conn)
        total_chunks   += inserted
        sources_loaded.append(source)
        print(f"   ✅ Inserted {inserted:,} chunks → "
              f"RESTAURANT_INTELLIGENCE.{TARGET_SCHEMA}.{TARGET_TABLE}")

        time.sleep(0.5)    # polite pause between documents

    # Summary
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n{'═' * 60}")
    print(f"  Documents processed : {len(sources_loaded)}")
    print(f"  Total pages         : {total_pages}")
    print(f"  Total chunks loaded : {total_chunks:,}")
    print(f"  Elapsed             : {elapsed:.1f}s")
    print(f"{'═' * 60}")

    if not args.dry_run and conn:
        if not args.no_lineage and sources_loaded:
            emit_openlineage_event(conn, sources_loaded, total_chunks, run_id, event_time)

        conn.close()

        print("\n📋 Next step: create the Cortex Search service")
        print("   Run: cortex/03_cortex_search_setup.sql in a Snowflake worksheet")
        print("   Then verify: SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(...))")


if __name__ == "__main__":
    main()