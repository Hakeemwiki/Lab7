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

# --- Trip Start Processor ---
def process_trip_start(csv_path, stream_name, sleep_seconds):
    logging.info(f"Processing trip start events from {csv_path}")
    success, failed = 0, 0
    failed_records = []

    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = {
                    "trip_id": row["trip_id"],
                    "pickup_datetime": row["pickup_datetime"],
                    "pickup_location_id": row["pickup_location_id"],
                    "dropoff_location_id": row["dropoff_location_id"],
                    "vendor_id": row["vendor_id"],
                    "estimated_dropoff_datetime": row["estimated_dropoff_datetime"],
                    "estimated_fare_amount": row["estimated_fare_amount"],
                    "event_type": "start"
                }
                if send_record(stream_name, record):
                    success += 1
                else:
                    failed += 1
                    failed_records.append(record)
                time.sleep(sleep_seconds)
    except FileNotFoundError:
        logging.error(f"Trip start file not found: {csv_path}")
    except Exception as e:
        logging.exception(f"Unexpected error while processing trip start events: {e}")

    write_failures_to_s3(failed_records, "start")
    logging.info(f"Trip Start Results – Success: {success}, Failed: {failed}")

# --- Trip End Processor ---
def process_trip_end(csv_path, stream_name, sleep_seconds):
    logging.info(f"Processing trip end events from {csv_path}")
    success, failed = 0, 0
    failed_records = []

    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = {
                    "trip_id": row["trip_id"],
                    "dropoff_datetime": row["dropoff_datetime"],
                    "fare_amount": row["fare_amount"],
                    "tip_amount": row["tip_amount"],
                    "trip_distance": row["trip_distance"],
                    "rate_code": row["rate_code"],
                    "payment_type": row["payment_type"],
                    "trip_type": row["trip_type"],
                    "passenger_count": row["passenger_count"],
                    "event_type": "end"
                }
                if send_record(stream_name, record):
                    success += 1
                else:
                    failed += 1
                    failed_records.append(record)
                time.sleep(sleep_seconds)
    except FileNotFoundError:
        logging.error(f"Trip end file not found: {csv_path}")
    except Exception as e:
        logging.exception(f"Unexpected error while processing trip end events: {e}")

    write_failures_to_s3(failed_records, "end")
    logging.info(f"Trip End Results – Success: {success}, Failed: {failed}")