#!/usr/bin/env python3
"""
upload_to_stage.py
──────────────────────────────────────────────────────────────────────────────
Uploads map/ app files to the Snowflake SiS stage using snowflake-connector-
python (already installed in the cortex venv). Replaces the snowsql PUT
commands in the README — snowsql is a separate installer not available by
default on macOS.

Usage:
    cd <repo-root>
    source cortex/.venv/bin/activate
    python map/upload_to_stage.py

Reads credentials from .env (same file cortex_agent.py uses).
"""

from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
)
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

STAGE = "@RESTAURANT_INTELLIGENCE.RAW.STREAMLIT_MAP_STAGE"

# Files to upload — paths relative to repo root
REPO_ROOT = Path(__file__).parent.parent
FILES = [
    REPO_ROOT / "map" / "streamlit_map_app.py",
    REPO_ROOT / "map" / "map_engine.py",
    REPO_ROOT / "map" / "environment.yml",   # conda deps — SiS reads this, not requirements.txt
]


def _private_key_bytes() -> bytes:
    key_path = Path(
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH",
                       str(Path.home() / ".ssh" / "snowflake_rsa_key.pem"))
    ).expanduser()
    with open(key_path, "rb") as f:
        pk = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return pk.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def main() -> None:
    conn = snowflake.connector.connect(
        account     = os.environ["SNOWFLAKE_ACCOUNT"],
        user        = os.environ["SNOWFLAKE_USER"],
        private_key = _private_key_bytes(),
        warehouse   = os.environ.get("SNOWFLAKE_WAREHOUSE", "RESTAURANT_WH"),
        database    = "RESTAURANT_INTELLIGENCE",
        schema      = "RAW",
        role        = os.environ.get("SNOWFLAKE_ROLE", "RESTAURANT_LOADER"),
    )

    try:
        cur = conn.cursor()

        for local_path in FILES:
            if not local_path.exists():
                print(f"  SKIP  {local_path.name} — file not found")
                continue

            put_sql = (
                f"PUT 'file://{local_path}' {STAGE} "
                f"OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
            )
            print(f"  PUT   {local_path.name} … ", end="", flush=True)
            cur.execute(put_sql)
            row = cur.fetchone()
            status = row[6] if row else "unknown"   # column 6 = STATUS
            print(status)

        print()
        cur.execute(f"LIST {STAGE}")
        rows = cur.fetchall()
        print(f"Stage contents ({len(rows)} file(s)):")
        for r in rows:
            print(f"  {r[0]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
