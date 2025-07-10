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
kinesis = boto3.client("kinesis", region_name="eu-north-1")
s3 = boto3.client("s3")
S3_BUCKET = os.environ.get("S3_BUCKET", "nsp-bolt-trip-analytics")
DEFAULT_SLEEP = 0.2  # seconds

# --- Send to Kinesis ---
def send_record(stream_name, record):
    try:
        if "trip_id" not in record or not record["trip_id"]:
            logging.warning("Skipping record with missing trip_id")
            return False
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=record["trip_id"]
        )
        return True
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to send record {record.get('trip_id', 'unknown')} to {stream_name}: {e}")
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
            rows = list(reader)
            if not rows:
                logging.warning(f"No rows found in {csv_path}")
                return

            for i, row in enumerate(rows, 1):
                record = {
                    "trip_id": row.get("trip_id"),
                    "pickup_datetime": row.get("pickup_datetime"),
                    "pickup_location_id": row.get("pickup_location_id"),
                    "dropoff_location_id": row.get("dropoff_location_id"),
                    "vendor_id": row.get("vendor_id"),
                    "estimated_dropoff_datetime": row.get("estimated_dropoff_datetime"),
                    "estimated_fare_amount": row.get("estimated_fare_amount"),
                    "event_type": "start"
                }
                logging.debug(f"[{i}] Sending trip start: {record['trip_id']}")
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
            rows = list(reader)
            if not rows:
                logging.warning(f"No rows found in {csv_path}")
                return

            for i, row in enumerate(rows, 1):
                record = {
                    "trip_id": row.get("trip_id"),
                    "dropoff_datetime": row.get("dropoff_datetime"),
                    "fare_amount": row.get("fare_amount"),
                    "tip_amount": row.get("tip_amount"),
                    "trip_distance": row.get("trip_distance"),
                    "rate_code": row.get("rate_code"),
                    "payment_type": row.get("payment_type"),
                    "trip_type": row.get("trip_type"),
                    "passenger_count": row.get("passenger_count"),
                    "event_type": "end"
                }
                logging.debug(f"[{i}] Sending trip end: {record['trip_id']}")
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

# --- Argument Parser ---
def parse_args():
    parser = argparse.ArgumentParser(description="Send trip events to Kinesis")
    parser.add_argument('--start', help="Path to trip_start.csv")
    parser.add_argument('--end', help="Path to trip_end.csv")
    parser.add_argument('--sleep', type=float, default=DEFAULT_SLEEP, help="Delay between sends (seconds)")
    return parser.parse_args()

# --- Main ---
if __name__ == "__main__":
    args = parse_args()
    if not args.start and not args.end:
        logging.error("Please provide at least one of --start or --end")
        exit(1)
    if args.start:
        process_trip_start(args.start, "trip_start_stream", args.sleep)
    if args.end:
        process_trip_end(args.end, "trip_end_stream", args.sleep)
    logging.info("Event dispatch complete.")
