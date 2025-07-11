
import boto3
import json
import logging
from decimal import Decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key
import botocore.exceptions

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
region = boto3.session.Session().region_name
dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table('TripData')

# Custom JSON encoder to handle Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float for JSON serialization
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    logger.info("Lambda triggered with event: %s", json.dumps(event, indent=2, cls=DecimalEncoder))
    completed_count = 0
    skipped_count = 0
    error_count = 0

    # Process each record from DynamoDB Stream
    for record in event['Records']:
        logger.info(f"Processing record: {json.dumps(record, indent=2, cls=DecimalEncoder)}")
        if record['eventName'] not in ['INSERT', 'MODIFY']:
            logger.info(f"Skipping record with eventName: {record['eventName']}")
            skipped_count += 1
            continue

        new_image = record['dynamodb'].get('NewImage', {})
        if not new_image:
            logger.warning("No NewImage in record. Skipping.")
            skipped_count += 1
            continue

        # Convert DynamoDB stream types to native Python
        item = {k: deserialize_dynamo_value(v) for k, v in new_image.items()}
        try:
            logger.info(f"Parsed item: {json.dumps(item, indent=2, cls=DecimalEncoder)}")
        except Exception as e:
            logger.error(f"Failed to log parsed item: {str(e)}")
            # Continue processing despite logging error

        trip_id = item.get('trip_id')
        sort_key = item.get('sort_key', '')
        event_type = item.get('event_type', '').lower()

        if not trip_id or not sort_key or not sort_key.startswith('RAW#'):
            logger.warning(f"⚠️ Invalid item: trip_id={trip_id}, sort_key={sort_key}, event_type={event_type}. Skipping.")
            skipped_count += 1
            continue

        # Skip if already marked as completed
        if item.get('trip_status') == 'completed':
            logger.info(f"Trip_id={trip_id}, sort_key={sort_key} already completed (trip_status=completed). Skipping.")
            skipped_count += 1
            continue

        # Check for existing COMPLETED record
        try:
            existing = table.query(
                KeyConditionExpression=Key('trip_id').eq(trip_id) & Key('sort_key').begins_with('COMPLETED#')
            )
            if existing.get('Items'):
                logger.warning(f"Trip_id={trip_id} already has COMPLETED record: {existing['Items'][0]['sort_key']}. Cleaning up RAW records.")
                # Clean up lingering RAW records
                try:
                    delete_raw_record_with_retry(trip_id, sort_key)
                    # Find and delete counterpart
                    counterpart_prefix = 'RAW#START#' if 'end' in event_type else 'RAW#END#'
                    response = table.query(
                        KeyConditionExpression=Key('trip_id').eq(trip_id) & Key('sort_key').begins_with(counterpart_prefix)
                    )
                    for counterpart in response.get('Items', []):
                        delete_raw_record_with_retry(trip_id, counterpart['sort_key'])
                except Exception as e:
                    logger.error(f"Failed to clean up RAW records for trip_id={trip_id}: {str(e)}")
                skipped_count += 1
                continue
        except Exception as e:
            logger.error(f"Failed to check COMPLETED record for trip_id={trip_id}: {str(e)}")
            error_count += 1
            continue

        # Determine counterpart prefix
        if 'start' in event_type:
            counterpart_prefix = 'RAW#END#'
        elif 'end' in event_type:
            counterpart_prefix = 'RAW#START#'
        else:
            logger.warning(f"⚠️ Unknown event_type: {event_type}. Skipping.")
            skipped_count += 1
            continue

        # Query for counterpart RAW item
        try:
            response = table.query(
                KeyConditionExpression=Key('trip_id').eq(trip_id) & Key('sort_key').begins_with(counterpart_prefix)
            )
            counterpart_items = response.get('Items', [])
            logger.info(f"Found {len(counterpart_items)} counterpart items for trip_id={trip_id}, prefix={counterpart_prefix}")
        except Exception as e:
            logger.error(f"Query failed for trip_id={trip_id}, prefix={counterpart_prefix}: {str(e)}")
            error_count += 1
            continue

        if not counterpart_items:
            logger.info(f"No counterpart found for trip_id={trip_id}, event_type={event_type}. Skipping.")
            skipped_count += 1
            continue

        counterpart_item = counterpart_items[0]
        start_item, end_item = (item, counterpart_item) if 'start' in event_type else (counterpart_item, item)
        logger.info(f"Matched pair for trip_id={trip_id}: start_sort_key={start_item['sort_key']}, end_sort_key={end_item['sort_key']}")

        # Validate required fields
        if not start_item.get('pickup_datetime') or not end_item.get('dropoff_datetime'):
            logger.error(f"Missing datetime fields for trip_id={trip_id}: pickup={start_item.get('pickup_datetime')}, dropoff={end_item.get('dropoff_datetime')}")
            error_count += 1
            continue

        # Merge start and end into a completed item
        completed_item = merge_raw_items(trip_id, start_item, end_item)
        if not completed_item:
            logger.error(f"Failed to merge items for trip_id={trip_id}")
            error_count += 1
            continue

        # Write COMPLETED record and update/delete RAW records
        try:
            table.put_item(Item=completed_item)
            logger.info(f"Inserted COMPLETED record: {completed_item['sort_key']} for trip_id={trip_id}")

            # Update RAW records to completed
            mark_record_completed(trip_id, start_item['sort_key'])
            mark_record_completed(trip_id, end_item['sort_key'])

            # Delete RAW records with retry
            delete_raw_record_with_retry(trip_id, start_item['sort_key'])
            delete_raw_record_with_retry(trip_id, end_item['sort_key'])

            completed_count += 1
        except Exception as e:
            logger.error(f"Failed to process COMPLETED record for trip_id={trip_id}: {str(e)}")
            error_count += 1

    logger.info(f"🎉 Summary: {completed_count} completed, {skipped_count} skipped, {error_count} errors")
    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {completed_count} completed, {skipped_count} skipped, {error_count} errors")
    }

def deserialize_dynamo_value(value):
    """Convert DynamoDB Stream value types to native Python types."""
    try:
        if 'S' in value:
            return value['S']
        elif 'N' in value:
            return Decimal(value['N'])
        elif 'BOOL' in value:
            return value['BOOL']
        elif 'NULL' in value:
            return None
        elif 'M' in value:
            return {k: deserialize_dynamo_value(v) for k, v in value['M'].items()}
        elif 'L' in value:
            return [deserialize_dynamo_value(v) for v in value['L']]
        else:
            logger.warning(f"⚠️ Unknown DynamoDB type for value: {value}")
            return value
    except Exception as e:
        logger.error(f"Failed to deserialize value {value}: {str(e)}")
        return None

def merge_raw_items(trip_id, start_item, end_item):
    """Merge START and END events into a single COMPLETED item."""
    try:
        completed_item = {
            'trip_id': trip_id,
            'sort_key': f"COMPLETED#{datetime.utcnow().isoformat()}",
            'pickup_datetime': start_item.get('pickup_datetime'),
            'dropoff_datetime': end_item.get('dropoff_datetime'),
            'fare_amount': end_item.get('fare_amount'),
            'estimated_fare_amount': start_item.get('estimated_fare_amount'),
            'trip_status': 'completed',
            'aggregation_flag': True,
            'day_partition': start_item.get('day_partition') or end_item.get('day_partition'),
            'created_at': datetime.utcnow().isoformat()
        }
        logger.info(f"Merged COMPLETED item: {json.dumps(completed_item, indent=2, cls=DecimalEncoder)}")
        return completed_item
    except Exception as e:
        logger.error(f"Failed to merge records for trip_id={trip_id}: {str(e)}")
        return None

def mark_record_completed(trip_id, sort_key):
    """Mark a RAW# record as completed after aggregation."""
    try:
        table.update_item(
            Key={'trip_id': trip_id, 'sort_key': sort_key},
            UpdateExpression="SET trip_status = :status, aggregation_flag = :flag",
            ExpressionAttributeValues={
                ':status': 'completed',
                ':flag': True
            }
        )
        logger.info(f"Marked RAW record {sort_key} as completed for trip_id={trip_id}")
    except Exception as e:
        logger.error(f"Failed to update RAW record status for trip_id={trip_id}, sort_key={sort_key}: {str(e)}")

def delete_raw_record_with_retry(trip_id, sort_key, max_attempts=3):
    """Delete a RAW record with retry logic."""
    attempt = 1
    while attempt <= max_attempts:
        try:
            table.delete_item(Key={'trip_id': trip_id, 'sort_key': sort_key})
            logger.info(f"Deleted RAW record {sort_key} for trip_id={trip_id}")
            return
        except botocore.exceptions.ClientError as e:
            logger.error(f"Attempt {attempt} failed to delete RAW record {sort_key} for trip_id={trip_id}: {str(e)}")
            if attempt == max_attempts:
                logger.error(f"Max attempts reached for deleting RAW record {sort_key} for trip_id={trip_id}")
                raise
            attempt += 1
