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
    required = ['trip_id', 'pickup_datetime', 'estimated_fare_amount']
    if not all(k in payload for k in required):
        logger.warning(f"Missing fields: {payload}")
        return False
    try:
        datetime.strptime(payload['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        Decimal(str(payload['estimated_fare_amount']))
        return True
    except (ValueError, InvalidOperation):
        logger.warning(f"Invalid data format: {payload}")
        return False

def prepare_record(payload):
    trip_id = payload['trip_id']
    pickup_datetime = payload['pickup_datetime']
    day_key = pickup_datetime.split(' ')[0]
    return {
        'trip_id': trip_id,
        'sort_key': f"RAW#START#{pickup_datetime}",
        'event_type': 'start',
        'day_partition': day_key,
        'pickup_datetime': pickup_datetime,
        'estimated_fare': Decimal(str(payload['estimated_fare_amount'])),
        'status': 'pending',
        'created_at': datetime.utcnow().isoformat()
    }

def lambda_handler(event, context):
    if 'Records' not in event:
        logger.error(f"Invalid event structure: {event}")
        return {'statusCode': 400, 'body': 'Missing Records in event'}

    items = []
    for record in event['Records']:
        try:
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            if validate_data(payload):
                items.append(prepare_record(payload))
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    with table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)
    logger.info(f"Processed {len(items)} start events")
    return {'statusCode': 200, 'body': 'Start events ingested'}
