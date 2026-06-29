# TX Loader - Load Transactions into PostgreSQL

Loads bank/card transactions into a local PostgreSQL database using a stream architecture. Transactions are published to a NATS JetStream topic by a CSV producer, classified and normalized by an embedded Flink processor, and written to PostgreSQL. The stream layer is overkill for a local tool but mirrors a real-time production pipeline.

**Architecture:**  
`Transactions => Raw Txn Stream => Transaction Classifier => Classified Txn Stream => Transaction Database`

**Stack:** NATS JetStream (broker), Apache Flink embedded mini-cluster (processor), PostgreSQL (storage).

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Java 17+ | Required to build and run locally |
| Maven 3.8+ | Required to build from source |
| Docker + Docker Compose | Required for the containerized startup path |
| psql (optional) | Only needed to inspect the database directly |
| NATS CLI (optional) | Only needed to inspect stream state |

Flink runs as an **embedded mini-cluster** inside the processor JAR — no standalone Flink installation needed.

---

## Build

```bash
mvn package
```

Produces two fat JARs:
- `txloader-csv-producer/target/txloader-csv-producer.jar`
- `txloader-flink-processor/target/txloader-flink-processor.jar`

---

## Infrastructure Setup

Two Docker Compose files manage the infrastructure:

| File | Services |
|------|----------|
| `docker-compose.yaml` | PostgreSQL |
| `docker-compose-processor.yaml` | NATS, Flink processor |

### Credentials

Copy your credentials into a `.env` file in the project root before starting anything. Docker Compose loads this file automatically.

```bash
cat .env
```
```dotenv
DB_USER=myuser
DB_PASSWORD=mypassword
```

### Start PostgreSQL

```bash
docker compose up -d
```

Starts a PostgreSQL 16 container on `localhost:5432`, database `txloader`. Also creates the shared `txloader-net` Docker network used by both compose files.

### Apply the Database Schema

```bash
mvn liquibase:update -N
```

This is safe to re-run — Liquibase tracks applied changesets and skips them on subsequent runs.

To add schema changes in the future, create a new numbered SQL file under `db/changelog/changes/` and add a corresponding `<include>` entry in `db/changelog/db.changelog-master.xml`.

**`accounts`** — one row per bank/card account

| column | type   | notes                               |
|--------|--------|-------------------------------------|
| id     | SERIAL | PK                                  |
| name   | TEXT   | e.g. "Chase Checking", "Amex Gold" |
| type   | TEXT   | 'checking', 'savings', or 'credit'  |

**`transactions`** — one row per transaction

| column      | type          | notes                                                      |
|-------------|---------------|------------------------------------------------------------|
| id          | SERIAL        | PK                                                         |
| date        | DATE          | ISO format YYYY-MM-DD                                      |
| merchant    | TEXT          | cleaned merchant name                                      |
| amount      | NUMERIC(15,2) | negative = money out (spend), positive = refund/credit     |
| category    | TEXT          | assigned by classifier                                     |
| subcategory | TEXT          | assigned by classifier                                     |
| account_id  | INTEGER       | FK → accounts.id                                          |
| raw_desc    | TEXT          | original CSV description before normalization              |

---

## Running

### Option 1: Docker (recommended)

**1. Build the fat JAR** (required before the first build and after any code change):
```bash
mvn -pl txloader-flink-processor -am package -DskipTests
```

**2. Start PostgreSQL** (if not already running):
```bash
docker compose up -d
```

**3. Start NATS and the Flink processor:**
```bash
docker compose -f docker-compose-processor.yaml up -d --build
```

The `--build` flag rebuilds the processor image from the current JAR, so steps 1 and 3 are the only two commands needed after a code change.

The Flink web UI is available at **http://localhost:8081** once the processor starts.

**4. Run the CSV producer** — `--account` is required:
```bash
java -jar txloader-csv-producer/target/txloader-csv-producer.jar \
  --account "Chase Checking" \
  transactions.csv
```

**Teardown** (reverse order):
```bash
docker compose -f docker-compose-processor.yaml down
docker compose down
```

---

### Option 2: Run locally

**1. Start NATS with JetStream enabled:**

```bash
# macOS
brew install nats-server && nats-server -js

# Linux
curl -fsSL https://binaries.nats.dev/nats-io/nats-server/v2@latest | sh
sudo mv nats-server /usr/local/bin/
nats-server -js
```

To persist messages across restarts:
```bash
nats-server -js -sd /tmp/nats-data
```

**2. Start the Flink processor** — start before the producer so no messages are missed:
```bash
export DB_USER=myuser
export DB_PASSWORD=mypassword
java -jar txloader-flink-processor/target/txloader-flink-processor.jar \
  --db-url jdbc:postgresql://localhost:5432/txloader
```

**3. Run the CSV producer:**
```bash
java -jar txloader-csv-producer/target/txloader-csv-producer.jar \
  --account "Chase Checking" \
  transactions.csv
```

Multiple CSV files can be passed in one invocation:
```bash
java -jar txloader-csv-producer/target/txloader-csv-producer.jar \
  --account "Amex Gold" \
  jan.csv feb.csv mar.csv
```

---

## Command Reference

### CSV Producer

```
java -jar txloader-csv-producer.jar --account <name> <file> [<file> ...] [options]
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
java -jar txloader-flink-processor.jar [options]
```

| Flag | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `--db-url` | String | `jdbc:postgresql://localhost:5432/txloader` | No | JDBC URL for the PostgreSQL database |
| `--nats-url` | String | `nats://localhost:4222` | No | NATS server URL |
| `--subject` | String | `txns.raw` | No | NATS subject to consume raw transactions from |
| `--stream` | String | `TRANSACTIONS` | No | JetStream stream name |
| `--consumer` | String | `flink-processor` | No | JetStream consumer/durable name |
| `--web-port` | Integer | `8081` | No | Port for the embedded Flink web UI |
| `--classifier` | String | `keyword` | No | Merchant categorization algorithm (`keyword` is the only supported value) |
| `--rules` | String | built-in CSV | No | Path to a custom categorization rules CSV (keyword classifier only) |
