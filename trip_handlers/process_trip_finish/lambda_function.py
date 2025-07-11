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
    missing = [k for k in required if k not in payload]
    if missing:
        logger.warning(f"Missing fields: {missing} in payload: {payload}")
        return False
    try:
        datetime.strptime(payload['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
        Decimal(str(payload['fare_amount']))
        return True
    except (ValueError, InvalidOperation) as e:
        logger.warning(f"Invalid data in payload: {payload}, Error: {e}")
        return False

def prepare_record(payload):
    trip_id = payload['trip_id']
    dropoff_datetime = payload['dropoff_datetime']
    day_key = dropoff_datetime.split(' ')[0]
    return {
        'trip_id': trip_id,
        'sort_key': f"RAW#END#{dropoff_datetime}",
        'event_type': 'end',
        'day_partition': day_key,
        'dropoff_datetime': dropoff_datetime,
        'fare_amount': Decimal(str(payload['fare_amount'])),
        'status': 'pending',
        'created_at': datetime.utcnow().isoformat()
    }

def lambda_handler(event, context):
    logger.info(f"Processing {len(event.get('Records', []))} records")
    
    if 'Records' not in event:
        logger.error("Missing 'Records' in event")
        return {'statusCode': 400, 'body': 'Missing Records in event'}

    items = []
    for record in event['Records']:
        try:
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            if validate_data(payload):
                item = prepare_record(payload)
                if item:
                    items.append(item)
        except Exception as e:
            logger.error(f"Error processing record: {e}")

    with table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)

    logger.info(f"Successfully ingested {len(items)} end events")
    return {
        'statusCode': 200,
        'body': f'Successfully processed {len(items)} end events'
    }
