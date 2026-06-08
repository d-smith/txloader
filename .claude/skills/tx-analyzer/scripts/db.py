"""
db.py — SQLite connection helper for tx-analyzer tools.

Locates transactions.db by checking (in order):
  1. TX_DB environment variable
  2. transactions.db relative to CWD (works when run from project root)
  3. Walk up from this file's location until transactions.db is found
"""
import os
import sqlite3
from pathlib import Path

_db_path = None  # type: Path


def _resolve() -> Path:
    if env := os.environ.get("TX_DB"):
        return Path(env)
    candidate = Path("transactions.db")
    if candidate.exists():
        return candidate
    here = Path(__file__).resolve().parent
    for parent in [here, *here.parents]:
        candidate = parent / "transactions.db"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Cannot find transactions.db — set TX_DB or run from the project root"
    )


def init():
    global _db_path
    _db_path = _resolve()


def get_conn(readonly: bool = False) -> sqlite3.Connection:
    global _db_path
    if _db_path is None:
        _db_path = _resolve()
    uri = f"file:{_db_path}{'?mode=ro' if readonly else ''}"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn
