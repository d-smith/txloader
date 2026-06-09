# TX Loader - Load Transactions into SQLite

This project loads transactions into a database using a stream architecture, which is overkill for a local tool
but may be appropriate for a larger entity that wants to process data in real time via a stream. We'll simulate
a realtime feed via utilities that can read transactions from files and write them to the stream.

## Architecture

The flow for this application is:

Transactions => Raw Txn Stream => Transaction Classifier => Classified Txn Stream => Transaction Database

Transactions are written in real time to the stream, transformed and classified and written to an output stream, with the output stream items written to a database.

In terms of technology components, we'll use NATs.io for the stream, host the classifier and transaction db writer as a Flink stream processing app, and write the transactions to a SQLite database.

## Database Schema

### Tables

**`accounts`** — one row per bank/card account. 
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
| category   | TEXT    | assigned by classifier    |
| account_id | INTEGER | FK → accounts.id                                      |
| raw_desc   | TEXT    | original CSV description before normalization          |

## Stream Objects

Raw transactions written to the stream look like:

```json
{
    "date":"4/1/2026",
    "account":"amx",
    "description":"STARBUCKS 8007827282   800-782-7282  WA",
    "amount":"-50.00"
}
```

The output of the Flink transformation produces output that looks like:

```json
{
    "date":"2026-04-01",
    "merchant":"STARBUCKS",
    "amount":"-50.00",
    "category":"Food & Drink",
    "account_id":1,
    "raw_desc":"STARBUCKS 8007827282   800-782-7282  WA"
}
```

## Nuts and Bolts

```bash
# From the repo root — builds all three modules
mvn package

# Run the producer against a CSV (--account is required)
java -jar txloader-csv-producer/target/txloader-csv-producer-*.jar --account "Chase Checking" transactions.csv

# Run the Flink processor (embedded mini-cluster)
java -jar txloader-flink-processor/target/txloader-flink-processor-*.jar
```

See the [Command Reference](#command-reference) below for all flags and their defaults.

## Command Reference

### CSV Producer

```
java -jar txloader-csv-producer-*.jar --account <name> <file> [<file> ...] [options]
```

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--account` | String | — | **Yes** | Account name applied to every transaction row |
| Positional args | String(s) | — | **Yes** | One or more CSV file paths to process |
| `--nats-url` | String | `nats://localhost:4222` | No | NATS server URL |
| `--subject` | String | `txns.raw` | No | NATS subject to publish raw transactions to |
| `--stream` | String | `TRANSACTIONS` | No | JetStream stream name |

### Flink Processor

```
java -jar txloader-flink-processor-*.jar [options]
```

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--db` | String | `transactions.db` | No | Path to the SQLite database file |
| `--nats-url` | String | `nats://localhost:4222` | No | NATS server URL |
| `--subject` | String | `txns.raw` | No | NATS subject to consume raw transactions from |
| `--stream` | String | `TRANSACTIONS` | No | JetStream stream name |
| `--consumer` | String | `flink-processor` | No | JetStream consumer/durable name |
| `--web-port` | Integer | `8081` | No | Port for the embedded Flink web UI |
| `--classifier` | String | `keyword` | No | Merchant categorization algorithm (`keyword` is the only supported value) |
| `--rules` | String | built-in CSV | No | Path to a custom categorization rules CSV (keyword classifier only) |

## Local Setup

### Prerequisites

| Requirement | Notes |
|-------------|-------|
| Java 17+ | Required to run the JARs |
| Maven 3.8+ | Required to build from source |
| NATS Server 2.x | The message broker — must be running before either app starts |
| SQLite CLI (optional) | Only needed to inspect the database directly; the app bundles its own JDBC driver |
| NATS CLI (optional) | Only needed to inspect stream state; install separately from the server |

Flink runs as an **embedded mini-cluster** inside the processor JAR — no standalone Flink installation is needed.  
SQLite is a file — no server process is needed.

### Install NATS Server

**macOS (Homebrew)**
```bash
brew install nats-server
```

**Linux**
```bash
# Download the latest release binary from https://nats.io/download/
curl -fsSL https://binaries.nats.dev/nats-io/nats-server/v2@latest | sh
sudo mv nats-server /usr/local/bin/
```

**Verify**
```bash
nats-server --version
```

### Install NATS CLI (optional, for observing streams)

```bash
# macOS
brew install nats-io/nats-tools/nats

# Linux — download from https://github.com/nats-io/natscli/releases
```

### Install SQLite CLI (optional, for inspecting the database)

```bash
# macOS
brew install sqlite

# Ubuntu / Debian
sudo apt install sqlite3
```

### Configure and Start NATS

JetStream (NATS's persistent streaming layer) must be enabled. The simplest way is the `-js` flag:

```bash
nats-server -js
```

The TRANSACTIONS stream and its consumer are created automatically by the producer on first run — no manual stream configuration is needed.

To persist messages across server restarts, run with a data directory:

```bash
nats-server -js -sd /tmp/nats-data
```

### Build

```bash
mvn package
```

This produces two fat JARs:
- `txloader-csv-producer/target/txloader-csv-producer-1.0-SNAPSHOT.jar`
- `txloader-flink-processor/target/txloader-flink-processor-1.0-SNAPSHOT.jar`

### Initialize the Database

Before running the processor for the first time, create the schema:

```bash
sqlite3 transactions.db < scripts/schema.sql
```

This is safe to re-run — both tables are created with `IF NOT EXISTS`.

### Run

Start the components in this order:

**1. NATS server** (if not already running)
```bash
nats-server -js
```

**2. Flink processor** — start this before the producer so no messages are missed
```bash
java -jar txloader-flink-processor/target/txloader-flink-processor-1.0-SNAPSHOT.jar \
  --db transactions.db
```

Once started, the Flink web UI is available at **http://localhost:8081**. Use `--web-port <port>` to change the port if 8081 is already in use.

**3. CSV producer** — `--account` is required and identifies which account the CSV belongs to
```bash
java -jar txloader-csv-producer/target/txloader-csv-producer-1.0-SNAPSHOT.jar \
  --account "Chase Checking" \
  transactions.csv
```

Multiple CSV files can be passed in one invocation:
```bash
java -jar txloader-csv-producer/target/txloader-csv-producer-1.0-SNAPSHOT.jar \
  --account "Amex Gold" \
  jan.csv feb.csv mar.csv
```

### Observe

**Flink web UI**

Open **http://localhost:8081** in a browser while the processor is running. The UI shows:
- **Running jobs** — live job graph with each operator's throughput and backpressure
- **Task managers** — JVM memory and CPU usage
- **Checkpoints** — history and timing (if checkpointing is enabled)
- **Exceptions** — full stack traces for any task failures

**Watch the processor logs**

The processor logs lifecycle events at INFO and per-record detail at DEBUG. To enable DEBUG output, set the log level before running:

```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
  -jar txloader-flink-processor/target/txloader-flink-processor-1.0-SNAPSHOT.jar
```

**Inspect the NATS stream** (requires NATS CLI)

```bash
# Stream summary — message count, bytes, consumer lag
nats stream info TRANSACTIONS

# Watch raw messages arrive in real time
nats sub txns.raw
```

**Query the SQLite database**

```bash
# Recent transactions
sqlite3 transactions.db "SELECT date, merchant, amount, category FROM transactions ORDER BY id DESC LIMIT 20;"

# Transactions by category
sqlite3 transactions.db "SELECT category, COUNT(*) AS n, ROUND(SUM(amount),2) AS total FROM transactions GROUP BY category ORDER BY n DESC;"

# Accounts that were auto-created by the processor
sqlite3 transactions.db "SELECT * FROM accounts WHERE type = 'Unknown';"
```

### Clean Up

```bash
# removes all rows but keeps the table structure:
sqlite3 transactions.db "DELETE FROM transactions;"


# To wipe both tables and start completely fresh:
sqlite3 transactions.db "DELETE FROM transactions; DELETE FROM accounts;"
```


## SQLite commands

SELECT name FROM sqlite_master WHERE type='table';

SELECT
        strftime('%Y-%m', date)  AS month,
        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS total_spend,
        SUM(CASE WHEN amount > 0 THEN ABS(amount) ELSE 0 END) AS total_credits,
        COUNT(*)                 AS tx_count
    FROM transactions
    GROUP BY 1
    ORDER BY 1 DESC;

SELECT
        category,
        strftime('%Y-%m', date) AS month,
        SUM(amount)             AS total,
        COUNT(*)                AS tx_count,
        AVG(amount)             AS avg_tx
    FROM transactions
    WHERE amount < 0
    GROUP BY 1, 2
    ORDER BY 2 ASC, 3 ASC;