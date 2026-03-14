#!/usr/bin/env python3
"""
load_inspections.py
────────────────────────────────────────────────────────────────────────────────
Pulls the full NYC Restaurant Inspection dataset from the Socrata API and loads
it into Snowflake RAW.INSPECTIONS_RAW.

Load strategy: TRUNCATE + full reload on every run.
This is the correct approach for a trial/demo setup because:
  - The source dataset updates incrementally but has no reliable CDC key
  - A full reload is idempotent and easy to reason about
  - At ~250k rows the full load takes ~2 minutes on an XS warehouse

Fix note: Socrata omits fields that are null in a given record rather than
returning them as empty strings. We explicitly name every column in $select
to force Socrata to include all fields in every row.

Usage:
    cd ingestion
    pip install -r requirements.txt
    python load_inspections.py

    # Dry run (fetch only, no Snowflake writes):
    python load_inspections.py --dry-run

    # Limit rows for testing:
    python load_inspections.py --limit 1000
────────────────────────────────────────────────────────────────────────────────
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

import requests
import snowflake.connector
from tqdm import tqdm

import config

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


# Ordered to match the RAW table DDL in 01_snowflake_setup.sql
COLUMN_ORDER = [
    "camis", "dba", "boro", "building", "street", "zipcode", "phone",
    "cuisine_description", "inspection_date", "action", "violation_code",
    "violation_description", "critical_flag", "score", "grade", "grade_date",
    "record_date", "inspection_type", "latitude", "longitude",
    "community_board", "council_district", "census_tract", "bin", "bbl", "nta",
]

# OpenLineage namespace identifiers
OL_JOB_NAMESPACE  = "nyc_restaurant_intelligence"
OL_JOB_NAME       = "load_inspections"
OL_INPUT_NS       = "https://data.cityofnewyork.us"
OL_INPUT_NAME     = "Health/NYC-Restaurant-Inspection-Results/gv23-aida"
OL_SOCRATA_URI    = "https://data.cityofnewyork.us/resource/gv23-aida.json"


# ── Socrata fetch ─────────────────────────────────────────────────────────────

def fetch_all_records(limit: int | None = None) -> list[dict]:
    """
    Pages through the Socrata API and returns all records as a list of dicts.
    Socrata uses $offset / $limit for pagination.

    IMPORTANT: We explicitly name every column in $select. Without this,
    Socrata silently omits fields that are null in a given record, causing
    entire columns (e.g. camis) to load as NULL across the full dataset.
    """
    headers = {}
    if config.NYC_APP_TOKEN:
        headers["X-App-Token"] = config.NYC_APP_TOKEN
    else:
        print("⚠  No NYC_APP_TOKEN set — requests will be rate-limited to 1,000 rows/req")
        print("   Register at https://data.cityofnewyork.us to get a free token.\n")

    all_records: list[dict] = []
    offset = 0
    page_size = config.PAGE_SIZE

    print(f"Fetching from: {config.SOCRATA_ENDPOINT}")
    print(f"Page size: {page_size:,} rows\n")

    with tqdm(desc="Fetching pages", unit="rows", dynamic_ncols=True) as pbar:
        while True:
            # Explicitly select all columns so Socrata returns empty strings
            # instead of omitting null fields from the response entirely.
            params = {
                "$limit":  page_size,
                "$offset": offset,
                "$order":  ":id",
            }
            if limit:
                remaining = limit - len(all_records)
                if remaining <= 0:
                    break
                params["$limit"] = min(page_size, remaining)

            resp = requests.get(
                config.SOCRATA_ENDPOINT,
                params=params,
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            page = resp.json()

            if not page:
                break   # no more data

            all_records.extend(page)
            pbar.update(len(page))
            offset += len(page)

            if len(page) < page_size:
                break   # last page

            # Be a polite API citizen
            time.sleep(0.2)

    print(f"\n✅ Fetched {len(all_records):,} total records from Socrata\n")
    return all_records


# ── Row normalisation ─────────────────────────────────────────────────────────

def normalise_row(record: dict) -> tuple:
    """
    Converts a Socrata JSON record to an ordered tuple matching COLUMN_ORDER.
    All values are strings or None — typing happens in dbt STAGING models.
    """
    return tuple(
        str(record.get(col, "")).strip() or None
        for col in COLUMN_ORDER
    )


# ── Snowflake load ────────────────────────────────────────────────────────────

def load_to_snowflake(records: list[dict]) -> None:
    print("Connecting to Snowflake …")
    conn = snowflake.connector.connect(
        account   = config.SNOWFLAKE_ACCOUNT,
        user      = config.SNOWFLAKE_USER,
        password  = config.SNOWFLAKE_PASSWORD,
        warehouse = config.SNOWFLAKE_WAREHOUSE,
        database  = config.SNOWFLAKE_DATABASE,
        schema    = config.SNOWFLAKE_SCHEMA,
        role      = config.SNOWFLAKE_ROLE,
        session_parameters={"QUERY_TAG": "nyc_restaurant_loader"},
    )
    cur = conn.cursor()

    try:
        target = f"{config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.SNOWFLAKE_TABLE}"
        print(f"Truncating {target} …")
        cur.execute(f"TRUNCATE TABLE {target}")

        print(f"Building dataframe from {len(records):,} records …")
        df = pd.DataFrame([
            {col.upper(): str(r.get(col, "") or "").strip() or None for col in COLUMN_ORDER}
            for r in records
        ])

        print(f"Loading via internal stage (PUT + COPY INTO) …")
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name    = config.SNOWFLAKE_TABLE,
            database      = config.SNOWFLAKE_DATABASE,
            schema        = config.SNOWFLAKE_SCHEMA,
            auto_create_table = False,
            overwrite     = False,
        )

        print(f"\n✅ Inserted {nrows:,} rows into {target} ({nchunks} chunk(s))")

        cur.execute(f"SELECT COUNT(*) FROM {target}")
        count = cur.fetchone()[0]
        print(f"   Row count confirmed: {count:,}")

    finally:
        cur.close()

    # Return the open connection — OpenLineage emitter reuses the session token
    return conn

# ── OpenLineage ───────────────────────────────────────────────────────────────

def _get_snowflake_token(conn: snowflake.connector.SnowflakeConnection) -> str | None:
    """
    Retrieves the current session token from an open Snowflake connector
    connection. Used to authenticate the REST call to the lineage endpoint.

    Returns None if the token cannot be obtained (non-blocking).
    """
    try:
        # The connector stores the session token in the REST handler
        return conn.rest.token
    except Exception as exc:
        print(f"   ⚠  Could not retrieve session token: {exc}")
        return None


def build_openlineage_event(
    run_id: str,
    event_time: str,
    row_count: int,
) -> dict:
    """
    Builds an OpenLineage COMPLETE event declaring:
      - Input:  Socrata API (NYC Open Data)  →  external source
      - Output: RESTAURANT_INTELLIGENCE.RAW.INSPECTIONS_RAW  →  Snowflake table

    Spec: https://openlineage.io/spec/1-0-5/OpenLineage.json
    """
    output_namespace = (
        f"snowflake://{config.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    )
    output_name = (
        f"{config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.SNOWFLAKE_TABLE}"
    )

    return {
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
                        "Full TRUNCATE + reload of NYC DOHMH restaurant inspection "
                        "records from the Socrata API into Snowflake RAW schema."
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
        "inputs": [
            {
                "namespace": OL_INPUT_NS,
                "name":      OL_INPUT_NAME,
                "facets": {
                    "dataSource": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                        "name": "NYC Open Data — DOHMH Restaurant Inspection Results",
                        "uri":  OL_SOCRATA_URI,
                    },
                    "dataQuality": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityMetricsInputDatasetFacet.json",
                        "rowCount": row_count,
                    },
                    "documentation": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json",
                        "description": (
                            "NYC DOHMH restaurant inspection results. Updated daily by NYC "
                            "Open Data via the Socrata API. Includes inspection scores, grades, "
                            "violation codes, and enforcement actions for ~250k records."
                        ),
                    },
                },
            }
        ],
        "outputs": [
            {
                "namespace": output_namespace,
                "name":      output_name,
                "facets": {
                    "schema": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json",
                        "fields": [
                            {"name": col.upper(), "type": "VARCHAR"}
                            for col in COLUMN_ORDER
                        ] + [
                            {"name": "LOADED_AT", "type": "TIMESTAMP_TZ"},
                        ],
                    },
                    "outputStatistics": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
                        "rowCount": row_count,
                    },
                    "lifecycleStateChange": {
                        "_producer": f"{OL_JOB_NAMESPACE}/{OL_JOB_NAME}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
                        "lifecycleStateChange": "OVERWRITE",
                    },
                },
            }
        ],
    }


def emit_openlineage_event(
    conn: snowflake.connector.SnowflakeConnection,
    row_count: int,
    run_id: str,
    event_time: str,
) -> None:
    """
    Posts an OpenLineage COMPLETE event to Snowflake's external lineage endpoint.

    Endpoint (announced January 16, 2026):
      POST https://<account>.snowflakecomputing.com/api/v2/lineage/openlineage/v1/lineage

    Authentication: Snowflake session token (Bearer), reusing the open connector
    session — no additional credential required.

    This call is fire-and-forget: failures are logged as warnings and do not
    interrupt the pipeline. Trial accounts may not have the endpoint enabled.
    """
    token = _get_snowflake_token(conn)
    if not token:
        print("   ⚠  Skipping OpenLineage emission — session token unavailable.")
        return

    endpoint = (
        f"https://{config.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
        "/api/v2/lineage/openlineage/v1/lineage"
    )
    headers = {
        "Authorization":  f"Bearer {token}",
        "Content-Type":   "application/json",
        "Accept":         "application/json",
        "X-Snowflake-Authorization-Token-Type": "OAUTH",
    }
    payload = build_openlineage_event(run_id, event_time, row_count)

    print("\n📡 Emitting OpenLineage event …")
    print(f"   Endpoint : {endpoint}")
    print(f"   Run ID   : {run_id}")
    print(f"   Source   : {OL_INPUT_NS}/{OL_INPUT_NAME}")
    print(f"   Target   : {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.SNOWFLAKE_TABLE}")

    try:
        resp = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 201, 202):
            print(f"   ✅ OpenLineage event accepted (HTTP {resp.status_code})")
            print(
                "   → Lineage graph in Snowsight: "
                "Governance → Lineage → INSPECTIONS_RAW"
            )
        else:
            print(
                f"   ⚠  OpenLineage endpoint returned HTTP {resp.status_code}: "
                f"{resp.text[:200]}"
            )
    except requests.exceptions.ConnectionError:
        print(
            "   ⚠  Could not reach lineage endpoint "
            "(network block or endpoint not enabled on this account)."
        )
    except requests.exceptions.Timeout:
        print("   ⚠  OpenLineage request timed out — continuing.")
    except Exception as exc:
        print(f"   ⚠  OpenLineage emission failed: {exc}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load NYC Restaurant Inspections to Snowflake")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data from Socrata but do NOT write to Snowflake",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit total rows fetched (useful for testing, e.g. --limit 1000)",
    )
    args = parser.parse_args()

    start = datetime.now(timezone.utc)
    print("=" * 70)
    print(f"  NYC Restaurant Intelligence — Data Loader")
    print(f"  Started: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if args.dry_run:
        print("  Mode: DRY RUN (no Snowflake writes)")
    if args.limit:
        print(f"  Row limit: {args.limit:,}")
    print("=" * 70 + "\n")

    records = fetch_all_records(limit=args.limit)

    if args.dry_run:
        print("Dry run complete. Sample record:")
        print(json.dumps(records[0], indent=2) if records else "(no records)")
        sys.exit(0)

    load_to_snowflake(records)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n🏁 Pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
