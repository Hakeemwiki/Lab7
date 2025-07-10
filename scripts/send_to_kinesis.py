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

# --- Send in Batches ---
def send_records_in_batches(all_records, stream_name):
    failed_all = []
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i+BATCH_SIZE]
        failed = send_bulk_records(stream_name, batch)
        failed_all.extend(failed)
        time.sleep(DEFAULT_SLEEP)
    return failed_all

# --- Trip Start Processor ---
def process_trip_start(csv_path, stream_name):
    logging.info(f"Processing trip start events from {csv_path}")
    valid_records = []

    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("trip_id") or not row.get("pickup_datetime"):
                    continue  # Simple deduplication/validation

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
                valid_records.append(record)

    except FileNotFoundError:
        logging.error(f"Trip start file not found: {csv_path}")
        return
    except Exception as e:
        logging.exception(f"Unexpected error while reading trip start events: {e}")
        return

    failed_records = send_records_in_batches(valid_records, stream_name)
    write_failures_to_s3(failed_records, "start")
    logging.info(f"Trip Start Results – Success: {len(valid_records) - len(failed_records)}, Failed: {len(failed_records)}")

# --- Trip End Processor ---
def process_trip_end(csv_path, stream_name):
    logging.info(f"Processing trip end events from {csv_path}")
    valid_records = []

    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("trip_id") or not row.get("dropoff_datetime"):
                    continue

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
                valid_records.append(record)

    except FileNotFoundError:
        logging.error(f"Trip end file not found: {csv_path}")
        return
    except Exception as e:
        logging.exception(f"Unexpected error while reading trip end events: {e}")
        return

    failed_records = send_records_in_batches(valid_records, stream_name)
    write_failures_to_s3(failed_records, "end")
    logging.info(f"Trip End Results – Success: {len(valid_records) - len(failed_records)}, Failed: {len(failed_records)}")

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
        process_trip_start(args.start, "trip_start_stream")

    if args.end:
        process_trip_end(args.end, "trip_end_stream")

    logging.info("Event dispatch complete.")
