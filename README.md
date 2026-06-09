# TX Loader - Load Transactions into SQLite

Loads bank/card transactions into a local SQLite database using a stream architecture. Transactions are published to a NATS JetStream topic by a CSV producer, classified and normalized by an embedded Flink processor, and written to SQLite. The stream layer is overkill for a local tool but mirrors a real-time production pipeline.

**Architecture:**  
`Transactions => Raw Txn Stream => Transaction Classifier => Classified Txn Stream => Transaction Database`

**Stack:** NATS JetStream (broker), Apache Flink embedded mini-cluster (processor), SQLite (storage).

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Java 17+ | Required to run the JARs |
| Maven 3.8+ | Required to build from source |
| NATS Server 2.x | Must be running before either app starts |
| SQLite CLI (optional) | Only needed to inspect the database directly |
| NATS CLI (optional) | Only needed to inspect stream state |

Flink runs as an **embedded mini-cluster** inside the processor JAR — no standalone Flink installation needed.

---

## Build

```bash
mvn package
```

Produces two fat JARs:
- `txloader-csv-producer/target/txloader-csv-producer-1.0-SNAPSHOT.jar`
- `txloader-flink-processor/target/txloader-flink-processor-1.0-SNAPSHOT.jar`

---

## Infrastructure Setup

### NATS Server

**macOS**
```bash
brew install nats-server
```

**Linux**
```bash
curl -fsSL https://binaries.nats.dev/nats-io/nats-server/v2@latest | sh
sudo mv nats-server /usr/local/bin/
```

Start with JetStream enabled:
```bash
nats-server -js
```

To persist messages across restarts:
```bash
nats-server -js -sd /tmp/nats-data
```

The TRANSACTIONS stream and consumer are created automatically on first run — no manual stream configuration needed.

### NATS CLI (optional)

```bash
# macOS
brew install nats-io/nats-tools/nats

# Linux — download from https://github.com/nats-io/natscli/releases
```

### SQLite CLI (optional)

```bash
# macOS
brew install sqlite

# Ubuntu / Debian
sudo apt install sqlite3
```

### Database Schema

Initialize the schema before running the processor for the first time:

```bash
sqlite3 transactions.db < scripts/schema.sql
```

This is safe to re-run — both tables use `IF NOT EXISTS`.

**`accounts`** — one row per bank/card account

| column | type    | notes                               |
|--------|---------|-------------------------------------|
| id     | INTEGER | PK                                  |
| name   | TEXT    | e.g. "Chase Checking", "Amex Gold" |
| type   | TEXT    | 'checking', 'savings', or 'credit'  |

**`transactions`** — one row per transaction

| column     | type    | notes                                                      |
|------------|---------|------------------------------------------------------------|
| id         | INTEGER | PK                                                         |
| date       | DATE    | ISO format YYYY-MM-DD                                      |
| merchant   | TEXT    | cleaned merchant name                                      |
| amount     | REAL    | negative = money out (spend), positive = refund/credit     |
| category   | TEXT    | assigned by classifier                                     |
| account_id | INTEGER | FK → accounts.id                                          |
| raw_desc   | TEXT    | original CSV description before normalization              |

---

## Running

Start components in this order:

**1. NATS server**
```bash
nats-server -js
```

**2. Flink processor** — start before the producer so no messages are missed
```bash
java -jar txloader-flink-processor/target/txloader-flink-processor-1.0-SNAPSHOT.jar \
  --db transactions.db
```

The Flink web UI is available at **http://localhost:8081** once the processor starts.

**3. CSV producer** — `--account` is required
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

---

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
