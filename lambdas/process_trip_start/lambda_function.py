import boto3
import json
import logging
import base64
from decimal import Decimal
from datetime import datetime

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Trips')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode base64-encoded Kinesis data
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)

            trip_id = payload.get("trip_id")
            pickup_datetime = payload.get("pickup_datetime")

            if not trip_id or not pickup_datetime:
                logger.warning(f"Missing required fields in event: {payload}")
                continue

            # Generate day_partition (e.g., '2025-07-10')
            day_partition = pickup_datetime.split(" ")[0]

            update_expr = """
                SET pickup_datetime = :pd,
                    estimated_fare_amount = :fa,
                    trip_status = if_not_exists(trip_status, :ts),
                    day_partition = :dp,
                    aggregation_flag = if_not_exists(aggregation_flag, :af)
            """

            expr_values = {
                ':pd': pickup_datetime,
                ':fa': Decimal(str(payload.get("estimated_fare_amount", "0"))),  # Use Decimal for DynamoDB
                ':ts': 'started',
                ':dp': day_partition,
                ':af': False
            }

            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values
            )

            logger.info(f"Trip start processed: {trip_id}")

        except Exception as e:
            logger.exception(f"Failed to process trip start event: {e}")
