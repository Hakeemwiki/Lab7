import boto3
import json
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Trips')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            payload = json.loads(record['kinesis']['data'])
            trip_id = payload.get("trip_id")
            dropoff_datetime = payload.get("dropoff_datetime")
            fare_amount = payload.get("fare_amount")

            if not trip_id or not dropoff_datetime or fare_amount is None:
                logger.warning(f"Incomplete trip end data: {payload}")
                continue