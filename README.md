# Citi Bikes Streaming Pipeline — Kafka → S3 → Glue → Athena

A real-time data pipeline that ingests live NYC Citi Bike station data from the
public GBFS feed, streams it through Apache Kafka, lands it in S3 as
date-partitioned JSON Lines, catalogs the schema with AWS Glue, and makes it
SQL-queryable through Amazon Athena.

Built end-to-end to practice the canonical *event-streaming → data-lake →
serverless-analytics* pattern.

📺 **Demo walkthrough:** [LinkedIn post](https://www.linkedin.com/posts/<your-post-url>)

---

## Architecture

```
 ┌─────────────┐   HTTP    ┌──────────┐   produce   ┌─────────┐
 │ Citi Bikes  │ ────────► │ Producer │ ──────────► │  Kafka  │
 │  GBFS API   │           │ (Python) │             │ (Docker)│
 └─────────────┘           └──────────┘             └────┬────┘
                                                         │ consume
                                                         ▼
                                                  ┌──────────────┐
                                                  │   Consumer   │
                                                  │ (batched +   │
                                                  │  manual      │
                                                  │  commits)    │
                                                  └──────┬───────┘
                                                         │ JSON Lines
                                                         ▼
                          ┌──────────────────────────────────────────┐
                          │  s3://bucket/<topic>/year=/month=/day=/   │
                          │       hour=/batch-<uuid>.jsonl            │
                          └──────────────────┬───────────────────────┘
                                             │ schema discovery
                                             ▼
                                       ┌──────────┐
                                       │ AWS Glue │  (crawler + Data Catalog)
                                       └─────┬────┘
                                             │
                                             ▼
                                       ┌──────────┐
                                       │  Athena  │  (SQL queries)
                                       └──────────┘
```

---

## Tech stack

| Layer            | Tool                                                   |
| ---------------- | ------------------------------------------------------ |
| Streaming        | Apache Kafka (single-node KRaft, via Docker)           |
| Producer/Consumer| Python 3.12, `kafka-python`, `requests`                |
| Storage          | Amazon S3 (date-partitioned JSON Lines)                |
| Schema catalog   | AWS Glue Crawler + Data Catalog                        |
| Querying         | Amazon Athena (Trino-based, serverless SQL)            |
| Cloud SDK        | `boto3`                                                |
| Orchestration    | Docker Compose                                         |

---

## Key engineering decisions

- **Manual offset commits, post-upload.** Kafka offsets are committed only
  after a successful S3 `PutObject`, so a crash mid-batch causes reprocessing
  rather than data loss. `enable_auto_commit=False` to enforce this.
- **Per-topic batching.** Buffered up to 500 messages or 60 s per topic
  before flushing — keeps S3 object count low (~10× cheaper Athena scans)
  while keeping recovery RPO bounded.
- **Hive-style partitioning** (`year=/month=/day=/hour=`) so the Glue crawler
  auto-detects partitions and Athena can prune scans to single hours.
- **Rebalance listener** commits offsets cleanly on partition revocation,
  preventing duplicate processing when consumer groups re-balance.
- **Two topics, two prefixes, two tables** — `station_information` (mostly
  static) is kept separate from `station_status` (high-volume, mutable) so
  schema evolution and partition pruning work independently.

---

## Repo layout

```
.
├── docker-compose.yaml      # single-node Kafka in KRaft mode
├── requirements.txt
├── produce.py               # entrypoint: fetch GBFS → publish to Kafka
├── consume.py               # entrypoint: Kafka → S3 sink
├── bikes/                   # GBFS fetching + per-station publish
├── services/                # HTTP client + S3 client
├── kafka_producer/          # Kafka producer wrapper
├── kafka_consumer/          # Kafka consumer wrapper
├── helpers/                 # ConsumerRebalanceListener
├── constants/               # topics, routes
└── S3_GLUE_ATHENA_GUIDE.md  # AWS setup walkthrough
```

---

## Run it yourself

### Prerequisites
- Python 3.12+, Docker Desktop
- AWS account with an S3 bucket and a Glue role
- `aws configure` set up locally

### 1. Start Kafka
```bash
docker compose up -d
```

### 2. Set up Python env
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp .env.example .env
# edit .env and set S3_BUCKET
export $(grep -v '^#' .env | xargs)
```

### 4. Run the producer (one terminal)
```bash
python produce.py
```

### 5. Run the consumer (another terminal)
```bash
python consume.py
```
You'll see `flushed N -> s3://...` lines as batches upload.

### 6. AWS Glue + Athena setup
See [`S3_GLUE_ATHENA_GUIDE.md`](S3_GLUE_ATHENA_GUIDE.md)
for IAM, Glue crawler, and Athena query setup. Sample query:

```sql
SELECT i.name, s.num_bikes_available, s.num_docks_available
FROM station_status s
JOIN station_information i USING (station_id)
WHERE s.year='2026' AND s.month='05' AND s.day='12'
ORDER BY s.num_bikes_available DESC
LIMIT 10;
```

---

## What I'd do differently for production

- **Parquet instead of JSON Lines** — ~10× smaller scans, columnar pruning.
- **Replace polling crawler with EventBridge → Lambda → Glue partition
  registration** for sub-minute freshness.
- **Schema Registry + Avro** for the Kafka topics to catch breaking changes
  before they hit S3.
- **Multi-broker Kafka with replication factor ≥ 3** instead of single-node.
- **Dead-letter topic** for messages that fail JSON parsing or S3 upload.
- **Iceberg or Hudi** for ACID upserts on `station_status` (currently
  append-only with downstream dedupe).
