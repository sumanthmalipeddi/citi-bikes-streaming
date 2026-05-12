# Kafka → S3 → Glue → Athena: step-by-step guide

This guide takes the data your `consume.py` is already pulling from Kafka topics
(`bikes_station_information`, `bikes_station_status`) and lands it in S3 in a
way that Glue can crawl and Athena can query.

---

## Step 1 — AWS setup (one-time)

### 1a. Create the S3 bucket
- Console → S3 → Create bucket.
- Name: `citi-bikes-raw-<your-suffix>` (bucket names must be globally unique).
- Region: `ap-south-1` (Mumbai) is the India-first default. Pick whichever is cheapest/closest for learning.
- Leave defaults (block all public access ON, versioning OFF for now).

You'll also want a second bucket for Athena query results:
- `citi-bikes-athena-results-<your-suffix>`.

### 1b. IAM user with programmatic access
- Console → IAM → Users → Create user.
- Attach a custom policy that allows:
  - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on both buckets above.
  - `glue:*` on the Glue database you'll create.
  - `athena:*` (or scoped to a workgroup) for Athena.
- Generate an access key + secret. Save them.

### 1c. Configure boto3 locally
```bash
pip install boto3
aws configure
# AWS Access Key ID:       <paste>
# AWS Secret Access Key:   <paste>
# Default region name:     ap-south-1
# Default output format:   json
```
This writes creds to `~/.aws/credentials` so `boto3` finds them automatically.
**Never commit these to git.**

---

## Step 2 — S3 storage layout

Two Kafka topics → two S3 prefixes. Partition by date so Athena queries only
scan what they need.

```
s3://citi-bikes-raw-<suffix>/
├── station_information/
│   └── year=2026/month=05/day=12/hour=14/
│       └── batch-<uuid>.jsonl
└── station_status/
    └── year=2026/month=05/day=12/hour=14/
        └── batch-<uuid>.jsonl
```

**File format:** Start with **JSON Lines (`.jsonl`)** — one JSON object per
line, easy to debug. Later switch to **Parquet** (columnar, compressed) to cut
Athena scan costs by ~10x.

**Why partition by `year/month/day/hour`?**
Athena charges per byte scanned. If you query "last hour's data" against an
unpartitioned table, it scans *everything*. With partitions, it only opens
files in the matching `hour=` folder.

---

## Step 3 — Modify the consumer to write to S3

Key changes to your existing consumer:

### Batch, don't write-per-message
Each Kafka message is small (a few hundred bytes). Writing one S3 object per
message = thousands of tiny files = expensive and slow to query. Buffer in
memory, then flush as one object.

**Flush trigger:** whichever comes first:
- 500 messages buffered, OR
- 60 seconds since last flush, OR
- batch size > 5 MB.

### One buffer per topic
Keep `station_information` and `station_status` separate — they have different
schemas and go to different S3 prefixes.

### Commit offsets only after S3 upload succeeds
Currently your `add_offset` is called per-message. Move it to *after* the S3
`put_object` returns 200. Otherwise: if the process crashes between offset
commit and S3 write, you lose data permanently.

### Pseudocode for the write side
```python
import boto3, json, uuid
from datetime import datetime, timezone

s3 = boto3.client("s3")
BUCKET = "citi-bikes-raw-<suffix>"

def flush(topic_prefix, messages):
    if not messages:
        return
    now = datetime.now(timezone.utc)
    key = (
        f"{topic_prefix}/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"batch-{uuid.uuid4()}.jsonl"
    )
    body = "\n".join(json.dumps(m) for m in messages).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    # only AFTER this succeeds: commit Kafka offsets for these messages
```

You'll call `flush()` from the consume loop when batch size/time threshold
is hit, then call your existing `rebalance_listener.add_offset(...)` for the
messages you just flushed, then `consumer.commit(...)`.

---

## Step 4 — Glue crawler (discovers the schema)

A crawler walks your S3 prefix, infers JSON schema and partition keys, and
creates an Athena-queryable table in the Glue Data Catalog.

### 4a. Create the Glue database
- Console → AWS Glue → Databases → Add database.
- Name: `citi_bikes`.

### 4b. IAM role for the crawler
- Console → IAM → Roles → Create role.
- Trusted entity: AWS service → Glue.
- Attach: `AWSGlueServiceRole` (managed) + a custom policy granting
  `s3:GetObject`, `s3:ListBucket` on your raw bucket.
- Name it `AWSGlueServiceRole-citi-bikes`.

### 4c. Create the crawler
- Glue → Crawlers → Create crawler.
- Name: `citi-bikes-raw-crawler`.
- Data source: S3, path
  `s3://citi-bikes-raw-<suffix>/` (subfolders become two tables automatically).
- IAM role: the one from 4b.
- Target database: `citi_bikes`.
- Schedule: On demand for now (or hourly later).
- Run it once.

After it finishes, Glue → Tables shows:
- `citi_bikes.station_information`
- `citi_bikes.station_status`

Each with partition columns `year`, `month`, `day`, `hour` and inferred JSON
schema. **Re-run the crawler whenever new partition values appear** (e.g.
once an hour) — otherwise Athena won't see the new partitions.

---

## Step 5 — Query in Athena

### 5a. Set query result location
- Athena → Settings → Query result location:
  `s3://citi-bikes-athena-results-<suffix>/`.

### 5b. Run queries
- Database: `citi_bikes`.
- Examples:

```sql
-- Latest snapshot of stations
SELECT name, capacity, lat, lon
FROM station_information
WHERE year='2026' AND month='05' AND day='12'
LIMIT 20;

-- Bikes available right now per station
SELECT
  s.station_id,
  i.name,
  s.num_bikes_available,
  s.num_ebikes_available,
  s.num_docks_available
FROM station_status s
JOIN station_information i USING (station_id)
WHERE s.year='2026' AND s.month='05' AND s.day='12' AND s.hour='14'
  AND i.year='2026' AND i.month='05' AND i.day='12'
ORDER BY s.num_bikes_available DESC
LIMIT 20;

-- How many bikes available across the system over time
SELECT hour, SUM(num_bikes_available) AS total_bikes
FROM station_status
WHERE year='2026' AND month='05' AND day='12'
GROUP BY hour
ORDER BY hour;
```

**Always filter on partition columns** (`year`, `month`, `day`, `hour`).
That's what makes Athena cheap.

---

## Gotchas before you start

1. **Athena charges per byte scanned** (~$5 per TB). JSON Lines is fine for
   learning but partition + Parquet for any real workload.
2. **`station_status` duplicates a lot.** Every poll re-emits the same stations.
   For analytics, dedupe downstream on `(station_id, last_reported)`.
3. **Crawlers cost ~$0.44/DPU-hour while running.** They finish in seconds for
   small data, but don't leave one scheduled every 5 minutes by accident.
4. **Glue + Athena are not real-time.** Crawler must re-run before Athena sees
   new partitions. For sub-minute freshness you'd need a different stack
   (Kinesis Firehose → S3, or Iceberg/Hudi with auto-commit).
5. **Don't commit AWS credentials.** Keep `~/.aws/credentials` outside the repo;
   never put keys in code or `.env` files that get committed.
6. **Region consistency.** Bucket, Glue database, Athena workgroup must all be
   in the same region or Athena can't query across them.

---

## Suggested order of work

1. Create both S3 buckets and IAM user/keys. Test with `aws s3 ls`.
2. Add boto3 + batched S3 sink to the consumer. Verify objects appear in S3.
3. Move offset commit to post-upload.
4. Set up Glue database, IAM role, and run the crawler.
5. Run a couple of Athena queries.
6. (Later) Switch JSON Lines → Parquet for cost savings.
