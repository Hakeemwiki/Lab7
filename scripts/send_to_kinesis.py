import boto3
import csv
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

kinesis = boto3.client('kinesis', region_name='eu-north-1')
S3_BUCKET = 'trip-failure-logs'
BATCH_SIZE = 500

def is_valid_record(record, event_type):
    if event_type == 'start':
        return all(k in record for k in ['trip_id', 'pickup_datetime', 'estimated_fare_amount']) and \
               datetime.strptime(record['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
    return all(k in record for k in ['trip_id', 'dropoff_datetime', 'fare_amount']) and \
           datetime.strptime(record['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')

def send_records(stream_name, records):
    try:
        response = kinesis.put_records(
            StreamName=stream_name,
            Records=[{'Data': json.dumps({'event_type': event_type, **rec}), 'PartitionKey': rec['trip_id']} for rec in records]
        )
        failed = [records[i] for i, r in enumerate(response['Records']) if 'ErrorCode' in r]
        if failed:
            logging.warning(f"{len(failed)} records failed")
        return failed
    except Exception as e:
        logging.error(f"Send failed: {e}")
        return records

def store_failed_records(failed_records, event_type):
    if failed_records:
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        key = f"failed/{event_type}/{timestamp}.json"
        boto3.client('s3').put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(failed_records, indent=2))
        logging.warning(f"Saved {len(failed_records)} failed records to S3")

def process_file(file_path, event_type, stream_name):
    records = []
    with open(file_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if is_valid_record(row, event_type):
                records.append(row)
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        failed = send_records(stream_name, batch)
        store_failed_records(failed, event_type)
        time.sleep(0.2)

if __name__ == "__main__":
    process_file('data/trip_start.csv', 'start', 'TripEventsStream')
    process_file('data/trip_end.csv', 'end', 'TripEventsStream')
    logging.info("Ingestion completed")