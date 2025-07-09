import boto3
import csv
import json
import time
import logging
import argparse
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, ClientError

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

# --- AWS Setup ---
kinesis = boto3.client("kinesis", region_name="us-east-1")
s3 = boto3.client("s3")
S3_BUCKET = os.environ.get("S3_BUCKET", "nsp-bolt-trip-analytics")

DEFAULT_SLEEP = 0.2  # seconds

# --- Send to Kinesis ---
def send_record(stream_name, record):
    try:
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=record["trip_id"]
        )
        return True
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to send record {record['trip_id']} to {stream_name}: {e}")
        return False
    
# --- S3 Fallback Writer ---
def write_failures_to_s3(failed_records, event_type):
    if not failed_records:
        return

    date = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    key = f"failures/trip-{event_type}/{date}/failures-{timestamp}.json"

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(failed_records, indent=2),
            ContentType="application/json"
        )
        logging.warning(f"{len(failed_records)} failed records written to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        logging.exception(f"Could not write failure log to S3: {e}")
