# TX Loader - Load Transactions into SQLite

This project loads transactions into SQL using a stream architecture, which is overkill for a local tool
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

## Nuts and Bolts

To build and run:
# From the repo root — builds all three modules
mvn package

# Run the producer against a CSV
java -jar txloader-csv-producer/target/txloader-csv-producer-*.jar transactions.csv

# Run the Flink processor (embedded mini-cluster)
java -jar txloader-flink-processor/target/txloader-flink-processor-*.jar

Both apps accept --nats-url, --subject, --stream, and --db flags to override defaults.