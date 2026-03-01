"""
config.py
Reads all configuration from environment variables (.env file).
No credentials are ever hardcoded here.
"""
import os
from dotenv import load_dotenv

# Load .env from the project root (one level up from ingestion/)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


def _require(key: str) -> str:
    """Get a required env var or raise a clear error."""
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"Missing required environment variable: {key}\n"
            f"Copy .env.example to .env and fill in your values."
        )
    return val


# ── Snowflake ─────────────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = _require("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = _require("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = _require("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "RESTAURANT_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "RESTAURANT_INTELLIGENCE")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE",      "RESTAURANT_LOADER")
SNOWFLAKE_SCHEMA    = "RAW"
SNOWFLAKE_TABLE     = "INSPECTIONS_RAW"

# ── Socrata / NYC Open Data ───────────────────────────────────────────────────
# Dataset: NYC Restaurant Inspection Results
# Docs: https://dev.socrata.com/foundry/data.cityofnewyork.us/gv23-aida
SOCRATA_ENDPOINT = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
NYC_APP_TOKEN    = os.environ.get("NYC_APP_TOKEN", "")   # Optional but recommended

# Pagination: Socrata max is 50,000 rows per request
# We use 10,000 to stay well within limits and show progress granularly
PAGE_SIZE = 10_000
