"""
tools.py — Analysis functions for Claude Code to call during a session.

Claude Code will discover these via CLAUDE.md. All functions are read-only.
Run directly to test: python tools.py
"""
from __future__ import annotations

import json
from datetime import datetime, date

import db


# ── Helpers ──────────────────────────────────────────────────────────────────

def _rows_to_dicts(rows) -> list[dict]:
    return [dict(r) for r in rows]


def _json(data) -> str:
    """Pretty-print for Claude to read."""
    return json.dumps(data, indent=2, default=str)


# ── Core tools ───────────────────────────────────────────────────────────────

def query(sql: str, params: tuple = ()) -> str:
    """
    Run any read-only SQL against the transactions database.
    Returns results as JSON. Use for ad-hoc analysis.

    Example:
        query("SELECT * FROM transactions WHERE merchant LIKE %s LIMIT 5", ("%amazon%",))
    """
    conn = db.get_conn(readonly=True)
    try:
        rows = conn.execute(sql, params).fetchall()
        return _json(_rows_to_dicts(rows))
    except Exception as e:
        return json.dumps({"error": str(e)})


def get_monthly_summary(year: int = None) -> str:
    """
    Total spend, credits, and transaction count by month.
    Defaults to the current year if year is not provided.
    """
    year = year or datetime.now().year
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        "SELECT * FROM monthly_summary WHERE month LIKE %s",
        (f"{year}%",)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def get_spending_by_category(month: str = None) -> str:
    """
    Spend per category for a given month (YYYY-MM).
    Defaults to the current month.
    """
    month = month or datetime.now().strftime("%Y-%m")
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        "SELECT * FROM category_summary WHERE month = %s ORDER BY total DESC",
        (month,)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def get_top_merchants(n: int = 10, since: str = None) -> str:
    """
    Top N merchants by total spend since a given date (ISO: YYYY-MM-DD).
    Defaults to last 90 days.
    """
    if since is None:
        from datetime import timedelta
        since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        """SELECT merchant, ROUND(ABS(SUM(amount)), 2) AS total_spend, COUNT(*) AS tx_count
           FROM transactions
           WHERE date >= %s AND amount < 0
           GROUP BY merchant
           ORDER BY total_spend DESC
           LIMIT %s""",
        (since, n)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def find_transactions(keyword: str, limit: int = 20) -> str:
    """
    Search transactions by merchant name or notes (case-insensitive substring match).
    """
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        """SELECT t.date, t.merchant, t.amount, t.category, a.name AS account
           FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id
           WHERE t.merchant LIKE %s OR t.raw_desc LIKE %s
           ORDER BY t.date DESC
           LIMIT %s""",
        (f"%{keyword}%", f"%{keyword}%", limit)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def get_recurring_charges(min_occurrences: int = 2) -> str:
    """
    Detect likely recurring/subscription charges: same merchant, similar amount,
    appearing in multiple distinct months.
    """
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        """SELECT
               merchant,
               ROUND(ABS(AVG(amount)), 2)                    AS avg_amount,
               COUNT(*)                                       AS total_occurrences,
               COUNT(DISTINCT TO_CHAR(date, 'YYYY-MM'))       AS months_seen,
               MIN(date)                                      AS first_seen,
               MAX(date)                                      AS last_seen
           FROM transactions
           WHERE amount < 0
           GROUP BY merchant
           HAVING COUNT(DISTINCT TO_CHAR(date, 'YYYY-MM')) >= %s
           ORDER BY months_seen DESC, avg_amount DESC""",
        (min_occurrences,)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def compare_months(month_a: str, month_b: str) -> str:
    """
    Side-by-side category spend comparison between two months (YYYY-MM).
    Returns a list of categories with spend in each month and the delta.
    """
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        """WITH
               a AS (SELECT category, ABS(SUM(amount)) AS total FROM transactions
                     WHERE TO_CHAR(date, 'YYYY-MM') = %s AND amount < 0 GROUP BY category),
               b AS (SELECT category, ABS(SUM(amount)) AS total FROM transactions
                     WHERE TO_CHAR(date, 'YYYY-MM') = %s AND amount < 0 GROUP BY category)
           SELECT category, month_a, month_b, delta FROM (
               SELECT
                   COALESCE(a.category, b.category)              AS category,
                   ROUND(COALESCE(a.total, 0), 2)                AS month_a,
                   ROUND(COALESCE(b.total, 0), 2)                AS month_b,
                   ROUND(COALESCE(b.total, 0) - COALESCE(a.total, 0), 2) AS delta
               FROM a LEFT JOIN b ON a.category = b.category
               UNION ALL
               SELECT b.category, 0.0, ROUND(b.total, 2), ROUND(b.total, 2)
               FROM b LEFT JOIN a ON a.category = b.category
               WHERE a.category IS NULL
           ) sub
           ORDER BY ABS(delta) DESC""",
        (month_a, month_b)
    ).fetchall()
    return _json({
        "month_a": month_a,
        "month_b": month_b,
        "by_category": _rows_to_dicts(rows)
    })


def get_large_transactions(threshold: float = 100.0, since: str = None) -> str:
    """
    List all transactions above a dollar threshold since a given date.
    Useful for spotting unexpected charges.
    """
    if since is None:
        from datetime import timedelta
        since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    conn = db.get_conn(readonly=True)
    rows = conn.execute(
        """SELECT t.date, t.merchant, ABS(t.amount) AS amount, t.category, a.name AS account
           FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id
           WHERE t.amount < 0 AND ABS(t.amount) >= %s AND t.date >= %s
           ORDER BY t.amount ASC""",
        (threshold, since)
    ).fetchall()
    return _json(_rows_to_dicts(rows))


def list_accounts() -> str:
    """List all accounts in the database."""
    conn = db.get_conn(readonly=True)
    rows = conn.execute("SELECT * FROM accounts").fetchall()
    return _json(_rows_to_dicts(rows))


# ── Quick self-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    db.init()
    print("=== Accounts ===")
    print(list_accounts())
    print("\n=== Monthly Summary (current year) ===")
    print(get_monthly_summary())
    print("\n=== Top 5 Merchants (last 90 days) ===")
    print(get_top_merchants(5))
    print("\n=== Recurring Charges ===")
    print(get_recurring_charges())
