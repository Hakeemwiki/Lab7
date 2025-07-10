import boto3
import json
import logging
import base64
from decimal import Decimal

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Trips')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode and parse Kinesis data
            decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(decoded_data)

            trip_id = payload.get("trip_id")
            dropoff_datetime = payload.get("dropoff_datetime")
            fare_amount = payload.get("fare_amount")

            if not trip_id or not dropoff_datetime or fare_amount is None:
                logger.warning(f"Incomplete trip end data: {payload}")
                continue

            # Check if pickup exists to determine trip status
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
                ':fa': Decimal(str(fare_amount)),  # Convert float string to Decimal
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
