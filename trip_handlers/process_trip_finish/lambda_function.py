import boto3
import json
import base64
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

def validate_data(payload):
    required = ['trip_id', 'dropoff_datetime', 'fare_amount']
    if not all(k in payload for k in required):
        logger.warning(f"Missing fields: {payload}")
        return False
    try:
        datetime.strptime(payload['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
        Decimal(str(payload['fare_amount']))
        return True
    except (ValueError, InvalidOperation):
        logger.warning(f"Invalid data format: {payload}")
        return False

def prepare_record(payload):
    trip_id = payload['trip_id']
    day_key = payload['dropoff_datetime'].split(' ')[0]
    return {
        'trip_id': trip_id,
        'sort_key': f"END#{payload['dropoff_datetime']}",
        'event_type': 'end',
        'day_partition': day_key,
        'dropoff_datetime': payload['dropoff_datetime'],
        'fare_amount': Decimal(str(payload['fare_amount'])),
        'status': 'pending'
    }

def lambda_handler(event, context):
    items = []
    for record in event['Records']:
        try:
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            if validate_data(payload):
                items.append(prepare_record(payload))
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    unique_items = {}
    for item in items:
        key = f"{item['trip_id']}#{item['sort_key']}"
        if key not in unique_items:
            unique_items[key] = item

    with table.batch_writer() as writer:
        for item in unique_items.values():
            writer.put_item(Item=item)
    logger.info(f"Processed {len(unique_items)} end events")
    return {'statusCode': 200, 'body': 'End events ingested'}