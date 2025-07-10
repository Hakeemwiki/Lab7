import boto3
import csv
import json
import time
import random
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
DEFAULT_SLEEP = 0.2
BATCH_SIZE = 500  # Max for PutRecords

# --- Send Bulk Records ---
def send_bulk_records(stream_name, records):
    try:
        response = kinesis.put_records(
            StreamName=stream_name,
            Records=[{
                'Data': json.dumps(rec),
                'PartitionKey': rec["trip_id"]
            } for rec in records]
        )

        failed = response['FailedRecordCount']
        if failed > 0:
            failed_records = [records[i] for i, res in enumerate(response['Records']) if 'ErrorCode' in res]
            logging.warning(f"{failed} records failed in bulk send.")
            return failed_records
        return []
    except (BotoCoreError, ClientError) as e:
        logging.exception("Bulk put_records failed")
        return records  # All failed

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

# --- Validation Utilities ---
def is_valid_datetime(value):
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return True
    except (ValueError, TypeError):
        return False

def is_valid_float(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

# --- Trip Record Loader ---
def load_trip_csv(path, event_type):
    logging.info(f"Loading {event_type} records from {path}")
    try:
        with open(path, newline='') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        return []
    except Exception as e:
        logging.exception(f"Error loading {event_type} records: {e}")
        return []

# --- Mixed Processor ---
def process_mixed_events(start_path, end_path):
    logging.info("Processing mixed trip events for interleaved simulation")
    start_rows = load_trip_csv(start_path, "start")
    end_rows = load_trip_csv(end_path, "end")

    all_records = []

    for row in start_rows:
        trip_id = row.get("trip_id")
        pickup_dt = row.get("pickup_datetime")
        dropoff_est_dt = row.get("estimated_dropoff_datetime")
        fare = row.get("estimated_fare_amount")

        if trip_id and is_valid_datetime(pickup_dt) and is_valid_datetime(dropoff_est_dt) and is_valid_float(fare):
            all_records.append({
                "trip_id": trip_id,
                "pickup_datetime": pickup_dt,
                "pickup_location_id": row["pickup_location_id"],
                "dropoff_location_id": row["dropoff_location_id"],
                "vendor_id": row["vendor_id"],
                "estimated_dropoff_datetime": dropoff_est_dt,
                "estimated_fare_amount": fare,
                "event_type": "start",
                "stream": "trip_start_stream"
            })

    for row in end_rows:
        trip_id = row.get("trip_id")
        dropoff_dt = row.get("dropoff_datetime")
        fare = row.get("fare_amount")

        if trip_id and is_valid_datetime(dropoff_dt) and is_valid_float(fare):
            all_records.append({
                "trip_id": trip_id,
                "dropoff_datetime": dropoff_dt,
                "fare_amount": fare,
                "tip_amount": row["tip_amount"],
                "trip_distance": row["trip_distance"],
                "rate_code": row["rate_code"],
                "payment_type": row["payment_type"],
                "trip_type": row["trip_type"],
                "passenger_count": row["passenger_count"],
                "event_type": "end",
                "stream": "trip_end_stream"
            })

    random.shuffle(all_records)
    logging.info(f"Sending {len(all_records)} shuffled trip events...")

    stream_groups = {"trip_start_stream": [], "trip_end_stream": []}
    for rec in all_records:
        stream_groups[rec["stream"]].append(rec)

    for stream_name, records in stream_groups.items():
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            failed = send_bulk_records(stream_name, batch)
            event_type = "start" if stream_name == "trip_start_stream" else "end"
            write_failures_to_s3(failed, event_type)
            time.sleep(DEFAULT_SLEEP)

# --- Argument Parser ---
def parse_args():
    parser = argparse.ArgumentParser(description="Send trip events to Kinesis")
    parser.add_argument('--start', help="Path to trip_start.csv")
    parser.add_argument('--end', help="Path to trip_end.csv")
    parser.add_argument('--sleep', type=float, default=DEFAULT_SLEEP, help="Delay between sends (seconds)")
    parser.add_argument('--mode', choices=["sequential", "mixed"], default="sequential", help="Sending mode")
    return parser.parse_args()

# --- Main ---
if __name__ == "__main__":
    args = parse_args()
    DEFAULT_SLEEP = args.sleep

    if not args.start or not args.end:
        logging.error("Both --start and --end paths are required for mixed simulation")
        exit(1)

    if args.mode == "mixed":
        process_mixed_events(args.start, args.end)
    else:
        logging.error("Sequential mode not implemented. Use --mode mixed")

    logging.info("Event dispatch complete.")
