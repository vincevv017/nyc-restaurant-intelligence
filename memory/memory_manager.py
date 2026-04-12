"""
memory_manager.py
─────────────────────────────────────────────────────────────────────────────
Persistent user-fact memory for the NYC Restaurant Intelligence Cortex Agent.

Table: RESTAURANT_INTELLIGENCE.RAW.AGENT_USER_MEMORY
  user_id     – Snowflake username or any stable caller identity
  fact_key    – snake_case label: "home_address", "preferred_borough", "role"
  fact_value  – stored string value
  source_turn – raw user message that provided the value (audit trail)
  updated_at  – set on every MERGE

Usage:
    from memory_manager import MemoryManager

    mgr = MemoryManager(user_id="analyst_01")
    mgr.load()                           # fetch facts from Snowflake
    ctx = mgr.to_system_context()        # prepend to user question
    mgr.save("home_address", "245 W 107th St", source_turn="<user msg>")
    mgr.delete("home_address")
    mgr.clear()                          # wipe all facts for this user
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # SiS runtime — python-dotenv not installed; no .env file to load

# ── Connection config (mirrors cortex_agent.py) ───────────────────────────────
# Use .get() so memory_manager.py can be imported in SiS without a .env file.
# _ACCOUNT, _USER, and _PRIVATE_KEY_PATH are only used by _connect(), which is
# the CLI path. In SiS, streamlit_app.py uses session.sql() directly and never
# calls _connect().
_ACCOUNT          = os.environ.get("SNOWFLAKE_ACCOUNT", "")
_USER             = os.environ.get("SNOWFLAKE_USER", "")
_WAREHOUSE        = os.environ.get("SNOWFLAKE_WAREHOUSE", "RESTAURANT_WH")
_DATABASE         = os.environ.get("SNOWFLAKE_DATABASE",  "RESTAURANT_INTELLIGENCE")
_ROLE             = os.environ.get("SNOWFLAKE_ROLE",       "RESTAURANT_LOADER")
_PRIVATE_KEY_PATH = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    str(Path.home() / ".ssh" / "snowflake_rsa_key.pem"),
)
_FULL_TABLE = f"{_DATABASE}.RAW.AGENT_USER_MEMORY"

# ── Rule-based memory-request patterns ───────────────────────────────────────
#
# Each tuple is (fact_key, compiled_regex).
# The regex is matched against the AGENT'S response text to detect when the
# agent asked the user for personal or preference information.
#
# Design rule: patterns must require possessive or interrogative context so
# that incidental keyword mentions ("The address information you provided…")
# do not false-fire. Every pattern needs the agent to be clearly *asking* for
# something, not just discussing a topic.
#
# Supported fact keys — see memory_README.md for the full capability table.
_MEMORY_REQUEST_PATTERNS: list[tuple[str, re.Pattern]] = [

    # ── Location ──────────────────────────────────────────────────────────────
    ("home_address", re.compile(
        r"\b(where (do |did )?you (live|stay|reside|are located|are based)|"
        r"your (home |current )?(address|location|neighbourhood|neighborhood)|"
        r"(share|tell me|provide|what is) your (home |current )?(address|location))\b",
        re.I,
    )),
    ("work_location", re.compile(
        r"\b(your (work|office|workplace) (address|location)|"
        r"where (do you work|is your office|is your workplace)|"
        r"(share|tell me|provide) your (work|office) (address|location))\b",
        re.I,
    )),
    ("neighborhood", re.compile(
        r"\b(which (neighbourhood|neighborhood|area|district)|"
        r"your (neighbourhood|neighborhood|area|district)|"
        r"what (neighbourhood|neighborhood|area) (are you|do you)|"
        r"(share|tell me) your (neighbourhood|neighborhood|area))\b",
        re.I,
    )),
    ("preferred_borough", re.compile(
        r"\b(preferred borough|which borough|borough you (prefer|live|work)|"
        r"your (preferred |home )?borough)\b",
        re.I,
    )),

    # ── Cuisine & food ────────────────────────────────────────────────────────
    ("favorite_cuisine", re.compile(
        r"\b(your (favorite|favourite|preferred) cuisine|"
        r"which cuisine (is your (favorite|favourite)|do you prefer|are you interested in)|"
        r"what (cuisine|food) (is your (favorite|favourite)|do you prefer|do you like|"
        r"are you (interested|focused) in))\b",
        re.I,
    )),

    # ── Inspection focus ──────────────────────────────────────────────────────
    ("violation_focus", re.compile(
        r"\b(which violation (code|type|category|number)|"
        r"what violation (code|type|category|number)|"
        r"violation (code|type|category) (are you|do you) (focus|track|monitor|investigat|interest)|"
        r"(focusing|working|concentrating) on (which|what) violation|"
        r"your (focus|target|current) violation)\b",
        re.I,
    )),
    ("grade_focus", re.compile(
        r"\b(which grade(s)?( are you| do you)? (focus|track|monitor|interest|look)|"
        r"what grade(s)? (are you|do you) (focus|track|monitor|interest|look)|"
        r"your (preferred|focus|target) grade(s)?|"
        r"grade(s)? (you are|you're) (focused|interested|monitoring|tracking))\b",
        re.I,
    )),
    ("inspection_type", re.compile(
        r"\b(which (type of )?inspection(s)?( are you| do you)? (focus|track|prefer|interest)|"
        r"initial or re.?inspection|"
        r"your (preferred|focus) inspection type|"
        r"(initial|re-?inspection)(s)? (only|preference|focus))\b",
        re.I,
    )),
    ("score_threshold", re.compile(
        r"\b(your (score|inspection score) threshold|"
        r"what score (threshold|cutoff|limit|range)|"
        r"which score(s)? (are you|do you) (focus|track|monitor|interest)|"
        r"score (above|below|over|under|greater|less) (which|what) (value|number|threshold))\b",
        re.I,
    )),

    # ── Time preferences ──────────────────────────────────────────────────────
    ("time_window", re.compile(
        r"\b(your (preferred |default )?(date range|time (range|window|period|frame))|"
        r"how (far back|many months) (do you|would you|should (i|we))|"
        r"what (date range|time (range|period|frame|window)) (do you|would you) prefer)\b",
        re.I,
    )),

    # ── Identity ──────────────────────────────────────────────────────────────
    ("role", re.compile(
        r"\b(your (role|job|title|position)|"
        r"what (do you do|is your role|is your job|is your title))\b",
        re.I,
    )),
    ("organization", re.compile(
        r"\b(your (organization|organisation|company|agency|department|team)|"
        r"which (organization|organisation|company|agency|department|team) (do you|are you))\b",
        re.I,
    )),
]


class MemoryManager:
    """
    Loads, stores, and injects per-user facts from AGENT_USER_MEMORY.

    Parameters
    ----------
    user_id : str
        Stable identity for the user (e.g. SNOWFLAKE_USER value).
    """

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.facts: dict[str, str] = {}

    # ── Snowflake connection ──────────────────────────────────────────────────

    def _connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Key-pair auth — mirrors cortex_agent.py's approach.
        Reads SNOWFLAKE_PRIVATE_KEY_PATH from .env (default: ~/.ssh/snowflake_rsa_key.pem).
        Password auth is not used: it triggers MFA on accounts where MFA is enforced.
        """
        key_path = Path(_PRIVATE_KEY_PATH).expanduser()
        with open(key_path, "rb") as f:
            private_key = load_pem_private_key(f.read(), password=None, backend=default_backend())
        private_key_bytes = private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        return snowflake.connector.connect(
            account     = _ACCOUNT,
            user        = _USER,
            private_key = private_key_bytes,
            warehouse   = _WAREHOUSE,
            database    = _DATABASE,
            schema      = "RAW",
            role        = _ROLE,
        )

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def load(self) -> None:
        """
        Fetch all stored facts for this user from Snowflake.
        Populates self.facts in-place.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT fact_key, fact_value "
                f"FROM   {_FULL_TABLE} "
                f"WHERE  user_id = %s "
                f"ORDER BY fact_key",
                (self.user_id,),
            )
            self.facts = {row[0]: row[1] for row in cur.fetchall()}

    def save(self, key: str, value: str, source_turn: str = "") -> None:
        """
        Upsert a single fact for this user.
        Uses MERGE so the same key is updated in-place rather than duplicated.
        Also updates self.facts in-place so callers see the change immediately.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                MERGE INTO {_FULL_TABLE} AS tgt
                USING (SELECT %s AS user_id, %s AS fact_key) AS src
                    ON tgt.user_id = src.user_id AND tgt.fact_key = src.fact_key
                WHEN MATCHED THEN
                    UPDATE SET fact_value  = %s,
                               source_turn = %s,
                               updated_at  = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (user_id, fact_key, fact_value, source_turn)
                    VALUES (%s, %s, %s, %s)
                """,
                (
                    self.user_id, key,            # USING source
                    value, source_turn,            # UPDATE
                    self.user_id, key, value, source_turn,  # INSERT
                ),
            )
        self.facts[key] = value

    def delete(self, key: str) -> None:
        """Remove one fact for this user."""
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_FULL_TABLE} WHERE user_id = %s AND fact_key = %s",
                (self.user_id, key),
            )
        self.facts.pop(key, None)

    def clear(self) -> int:
        """Delete ALL facts for this user. Returns the number of rows deleted."""
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_FULL_TABLE} WHERE user_id = %s",
                (self.user_id,),
            )
            deleted = cur.rowcount
        self.facts.clear()
        return deleted

    # ── Context injection ─────────────────────────────────────────────────────

    def to_system_context(self) -> str:
        """
        Serialise stored facts into a directive string that can be prepended
        to the user's question before passing to call_cortex_agent.

        Returns an empty string when no facts are stored.
        """
        if not self.facts:
            return ""
        lines = "\n".join(
            f"  - {k.replace('_', ' ')}: {v}" for k, v in self.facts.items()
        )
        return (
            "The following facts about this user are known and should be used "
            "to answer location-specific and preference-specific questions directly "
            "without asking for them again:\n"
            + lines
        )

    # ── Memory-request detection ──────────────────────────────────────────────

    def check_for_memory_request(self, response_text: str) -> dict | None:
        """
        Scan the agent's response for patterns that indicate it asked for
        personal information. Returns {"key": fact_key} when a match is found,
        or None when the response contains no personal-info request.

        Uses a fast rule-based approach (zero latency, no extra API calls).
        """
        for fact_key, pattern in _MEMORY_REQUEST_PATTERNS:
            if pattern.search(response_text):
                return {"key": fact_key}
        return None
