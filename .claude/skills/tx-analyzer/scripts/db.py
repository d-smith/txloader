"""
db.py — PostgreSQL connection helper for tx-analyzer tools.

Locates the database using (in order):
  1. TX_DB environment variable (libpq DSN, e.g. postgresql://localhost/txloader)
  2. Default: postgresql://localhost/txloader
"""
import os

import psycopg
import psycopg.rows

_dsn = None  # type: str


def _resolve() -> str:
    if env := os.environ.get("TX_DB"):
        return env
    return "postgresql://localhost/txloader"


def init():
    global _dsn
    _dsn = _resolve()


def get_conn(readonly: bool = False) -> psycopg.Connection:
    global _dsn
    if _dsn is None:
        _dsn = _resolve()
    conn = psycopg.connect(_dsn, row_factory=psycopg.rows.dict_row)
    if readonly:
        conn.autocommit = True
    return conn
