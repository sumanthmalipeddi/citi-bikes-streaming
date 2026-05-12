import json
import uuid
from datetime import datetime, timezone

import boto3


class S3Service:
    def __init__(self, bucket, region="ap-south-1"):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)

    def upload_jsonl(self, prefix, records):
        if not records:
            return None

        now = datetime.now(timezone.utc)
        key = (
            f"{prefix}/"
            f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
            f"batch-{uuid.uuid4()}.jsonl"
        )
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)
        return key
