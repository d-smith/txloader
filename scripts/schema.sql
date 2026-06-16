CREATE TABLE IF NOT EXISTS accounts (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id         SERIAL PRIMARY KEY,
    date       DATE NOT NULL,
    merchant   TEXT NOT NULL,
    amount     NUMERIC(15,2) NOT NULL,
    category   TEXT,
    account_id INTEGER REFERENCES accounts(id),
    raw_desc   TEXT
);

CREATE OR REPLACE VIEW monthly_summary AS
    SELECT
        TO_CHAR(date, 'YYYY-MM')  AS month,
        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS total_spend,
        SUM(CASE WHEN amount > 0 THEN ABS(amount) ELSE 0 END) AS total_credits,
        COUNT(*)                  AS tx_count
    FROM transactions
    GROUP BY 1
    ORDER BY 1 DESC;


CREATE OR REPLACE VIEW category_summary AS
    SELECT
        category,
        TO_CHAR(date, 'YYYY-MM') AS month,
        SUM(amount)              AS total,
        COUNT(*)                 AS tx_count,
        AVG(amount)              AS avg_tx
    FROM transactions
    WHERE amount < 0
    GROUP BY 1, 2
    ORDER BY 2 ASC, 3 ASC;
