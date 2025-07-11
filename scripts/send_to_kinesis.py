import boto3
import csv
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

kinesis = boto3.client('kinesis', region_name='eu-north-1')
S3_BUCKET = 'nsp-bolt-trip-analytics'
BATCH_SIZE = 20

def is_valid_record(record, event_type):
    """Validate record structure based on event type"""
    try:
        if event_type == 'start':
            required_fields = ['trip_id', 'pickup_datetime', 'estimated_fare_amount']
            if not all(k in record for k in required_fields):
                logger.warning(f"Missing required fields for start event: {record}")
                return False
            # Validate datetime format
            datetime.strptime(record['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
            # Validate fare amount
            float(record['estimated_fare_amount'])
            return True
        else:  # end event
            required_fields = ['trip_id', 'dropoff_datetime', 'fare_amount']
            if not all(k in record for k in required_fields):
                logger.warning(f"Missing required fields for end event: {record}")
                return False
            # Validate datetime format
            datetime.strptime(record['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
            # Validate fare amount
            float(record['fare_amount'])
            return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid data format in record: {record}, Error: {e}")
        return False

def send_records(stream_name, records, event_type):
    """Send records to Kinesis stream with retry logic"""
    if not records:
        return []
    
    try:
        # Prepare records for Kinesis
        kinesis_records = []
        for rec in records:
            # Add event_type to each record
            payload = {'event_type': event_type, **rec}
            kinesis_records.append({
                'Data': json.dumps(payload),
                'PartitionKey': rec['trip_id']
            })
        
        logger.info(f"Sending batch of {len(kinesis_records)} {event_type} records to {stream_name}")
        
        response = kinesis.put_records(
            StreamName=stream_name,
            Records=kinesis_records
        )
        
        # Check for failed records
        failed_records = []
        for i, record_result in enumerate(response['Records']):
            if 'ErrorCode' in record_result:
                failed_records.append(records[i])
                logger.error(f"Failed to send record {i}: {record_result['ErrorCode']} - {record_result.get('ErrorMessage', '')}")
        
        if failed_records:
            logger.warning(f"{len(failed_records)} out of {len(records)} records failed to send")
        else:
            logger.info(f"Successfully sent all {len(records)} records")
        
        return failed_records
        
    except Exception as e:
        logger.error(f"Error sending records to Kinesis: {e}")
        return records  # Return all records as failed

def store_failed_records(failed_records, event_type):
    """Store failed records to S3 for later retry"""
    if not failed_records:
        return
    
    try:
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
        key = f"failed/{event_type}/{timestamp}.json"
        
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(failed_records, indent=2),
            ContentType='application/json'
        )
        
        logger.warning(f"Saved {len(failed_records)} failed {event_type} records to s3://{S3_BUCKET}/{key}")
        
    except Exception as e:
        logger.error(f"Failed to store failed records to S3: {e}")

def process_file(file_path, event_type, stream_name):
    """Process CSV file and send records to Kinesis"""
    logger.info(f"Processing file: {file_path} for {event_type} events")
    
    try:
        records = []
        total_records = 0
        valid_records = 0
        
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                total_records += 1
                
                # Clean up the row data
                cleaned_row = {k.strip(): v.strip() for k, v in row.items() if k and v}
                
                if is_valid_record(cleaned_row, event_type):
                    records.append(cleaned_row)
                    valid_records += 1
                else:
                    logger.warning(f"Invalid record on row {row_num}: {cleaned_row}")
        
        logger.info(f"File processed: {total_records} total records, {valid_records} valid records")
        
        if not records:
            logger.warning(f"No valid records found in {file_path}")
            return
        
        # Send records in batches
        total_failed = 0
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            logger.info(f"Processing batch {batch_num} ({len(batch)} records)")
            
            failed = send_records(stream_name, batch, event_type)
            
            if failed:
                total_failed += len(failed)
                store_failed_records(failed, event_type)
            
            # Add delay between batches to avoid throttling
            if i + BATCH_SIZE < len(records):
                time.sleep(0.2)
        
        logger.info(f"Completed processing {file_path}: {len(records)} processed, {total_failed} failed")
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")

def main():
    """Main function to process both trip start and end files"""
    logger.info("Starting trip data ingestion process")
    
    # Process files
    files_to_process = [
        ('data/trip_start.csv', 'start', 'TripEventsStream'),
        ('data/trip_end.csv', 'end', 'TripEventsStream')
    ]
    
    for file_path, event_type, stream_name in files_to_process:
        try:
            process_file(file_path, event_type, stream_name)
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
    
    logger.info("Trip data ingestion process completed")

if __name__ == "__main__":
    main()