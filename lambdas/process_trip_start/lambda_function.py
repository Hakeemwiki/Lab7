import boto3
import json
import logging
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
            payload = json.loads(record['kinesis']['data'])
            trip_id = payload.get("trip_id")
            pickup_datetime = payload.get("pickup_datetime")

            if not trip_id or not pickup_datetime:
                logger.warning(f"Missing required fields in event: {payload}")
                continue

            # Generate day_partition
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
                ':fa': float(payload.get("estimated_fare_amount", 0)),
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
