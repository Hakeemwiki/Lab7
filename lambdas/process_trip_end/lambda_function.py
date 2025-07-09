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

            # Read current state
            current = table.get_item(Key={'trip_id': trip_id}).get('Item', {})
            pickup_exists = 'pickup_datetime' in current

            new_status = 'completed' if pickup_exists else 'ended'

            update_expr = """
                SET dropoff_datetime = :dd,
                    fare_amount = :fa,
                    trip_status = :ts,
                    aggregation_flag = if_not_exists(aggregation_flag, :af)
            """

            expr_values = {
                ':dd': dropoff_datetime,
                ':fa': float(fare_amount),
                ':ts': new_status,
                ':af': False
            }

            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values
            )

            logger.info(f"Trip end processed: {trip_id}, status set to {new_status}")

        except Exception as e:
            logger.exception(f"Failed to process trip end event: {e}")
