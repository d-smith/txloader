---
name: tx-analyzer
description: Perform personal finance analysis on transaction data.
---

# Transaction Analyzer
This skill lets you analyze personal bank and credit card transactions stored in a local SQLite database.

## Database Schema

### Tables

**`accounts`** — one row per bank/card account
| column | type    | notes                                  |
|--------|---------|----------------------------------------|
| id     | INTEGER | PK                                     |
| name   | TEXT    | e.g. "Chase Checking", "Amex Gold"    |
| type   | TEXT    | 'checking', 'savings', or 'credit'     |

**`transactions`** — one row per transaction
| column     | type    | notes                                                  |
|------------|---------|--------------------------------------------------------|
| id         | INTEGER | PK                                                     |
| date       | DATE    | ISO format YYYY-MM-DD                                  |
| merchant   | TEXT    | cleaned merchant name                                  |
| amount     | REAL    | **negative = money out (spend), positive = refund/credit** |
| category   | TEXT    | auto-assigned by flink app    |
| account_id | INTEGER | FK → accounts.id                                      |
| raw_desc   | TEXT    | original CSV description before normalization          |

### Useful Views

- **`monthly_summary`** — total spend, credits, and tx count per month
- **`category_summary`** — spend by category per month

## Available tools

The tools are in the python file scripts/tools.py.


Call these functions to analyze data. All are read-only.

| function                                    | what it does                                           |
|---------------------------------------------|--------------------------------------------------------|
| `query(sql, params)`                        | run ad-hoc SQL, returns JSON                          |
| `get_monthly_summary(year)`                 | spend/credits by month for a year                     |
| `get_spending_by_category(month)`           | category breakdown for a month (YYYY-MM)              |
| `get_top_merchants(n, since)`               | top N merchants by spend since a date                 |
| `find_transactions(keyword, limit)`         | search by merchant name                               |
| `get_recurring_charges(min_occurrences)`    | detect subscriptions/recurring charges                |
| `compare_months(month_a, month_b)`          | side-by-side category diff between two months        |
| `get_large_transactions(threshold, since)`  | flag charges over a dollar threshold                  |
| `list_accounts()`                           | show all accounts                                     |


