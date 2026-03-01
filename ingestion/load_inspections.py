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

Usage:
    cd ingestion
    pip install -r requirements.txt
    python load_inspections.py

    # Dry run (fetch only, no Snowflake writes):
    python load_inspections.py --dry-run

    # Limit rows for testing:
    python load_inspections.py --limit 5000
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

# Ordered to match the RAW table DDL in 01_snowflake_setup.sql
COLUMN_ORDER = [
    "camis", "dba", "boro", "building", "street", "zipcode", "phone",
    "cuisine_description", "inspection_date", "action", "violation_code",
    "violation_description", "critical_flag", "score", "grade", "grade_date",
    "record_date", "inspection_type", "latitude", "longitude",
    "community_board", "council_district", "census_tract", "bin", "bbl", "nta",
]


# ── Socrata fetch ─────────────────────────────────────────────────────────────

def fetch_all_records(limit: int | None = None) -> list[dict]:
    """
    Pages through the Socrata API and returns all records as a list of dicts.
    Socrata uses $offset / $limit for pagination.
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
            params = {
                "$limit":  page_size,
                "$offset": offset,
                "$order":  ":id",   # stable pagination order
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
    """
    Truncates RAW.INSPECTIONS_RAW and bulk-inserts all records using
    executemany (batched internally by the Snowflake connector).
    """
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
        # 1. Truncate (wipe previous load)
        target = f"{config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.SNOWFLAKE_TABLE}"
        print(f"Truncating {target} …")
        cur.execute(f"TRUNCATE TABLE {target}")

        # 2. Prepare INSERT — 26 data columns + _LOADED_AT uses DEFAULT
        placeholders = ", ".join(["%s"] * len(COLUMN_ORDER))
        insert_sql = f"""
            INSERT INTO {target}
                ({", ".join(c.upper() for c in COLUMN_ORDER)})
            VALUES ({placeholders})
        """

        # 3. Normalise rows
        print(f"Normalising {len(records):,} records …")
        rows = [normalise_row(r) for r in records]

        # 4. Bulk insert in chunks of 10,000
        chunk_size = 10_000
        total_inserted = 0
        with tqdm(total=len(rows), desc="Inserting rows", unit="rows", dynamic_ncols=True) as pbar:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                cur.executemany(insert_sql, chunk)
                total_inserted += len(chunk)
                pbar.update(len(chunk))

        conn.commit()
        print(f"\n✅ Inserted {total_inserted:,} rows into {target}")

        # 5. Quick row-count sanity check
        cur.execute(f"SELECT COUNT(*) FROM {target}")
        count = cur.fetchone()[0]
        print(f"   Row count confirmed: {count:,}")

    finally:
        cur.close()
        conn.close()


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
        help="Limit total rows fetched (useful for testing, e.g. --limit 5000)",
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
