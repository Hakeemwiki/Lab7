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
