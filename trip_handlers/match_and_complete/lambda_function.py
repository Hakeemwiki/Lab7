import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

def find_counterpart(trip_id, current_prefix):
    counterpart_prefix = 'END#' if current_prefix == 'START#' else 'START#'
    response = table.query(
        KeyConditionExpression="trip_id = :pk AND begins_with(sort_key, :sk)",
        ExpressionAttributeValues={':pk': trip_id, ':sk': counterpart_prefix}
    )
    return response['Items'][0] if response['Items'] else None

def create_completed_record(start_item, end_item):
    return {
        'trip_id': start_item['trip_id'],
        'sort_key': f"COMPLETED#{end_item['dropoff_datetime']}",
        'event_type': 'completed',
        'day_partition': end_item['day_partition'],
        'pickup_datetime': start_item['pickup_datetime'],
        'dropoff_datetime': end_item['dropoff_datetime'],
        'fare_amount': end_item['fare_amount'],
        'status': 'completed',
        'completed_time': datetime.utcnow().isoformat()
    }

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            continue
        new_item = record['dynamodb']['NewImage']
        trip_id = new_item['trip_id']['S']
        event_prefix = new_item['sort_key']['S'].split('#')[0]

        if event_prefix not in ['START', 'END']:
            continue

        counterpart = find_counterpart(trip_id, event_prefix)
        if counterpart:
            start_item = new_item if event_prefix == 'START' else counterpart
            end_item = counterpart if event_prefix == 'START' else new_item
            completed_item = create_completed_record(start_item, end_item)
            table.put_item(Item=completed_item)
            logger.info(f"Completed trip for {trip_id}")
    return {'statusCode': 200, 'body': 'Matching completed'}