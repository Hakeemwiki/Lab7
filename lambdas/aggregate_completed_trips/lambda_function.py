import boto3
import json
import os
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Trips")

s3 = boto3.client("s3")
S3_BUCKET = os.environ.get("S3_BUCKET", "nsp-bolt-trip-analytics")

def lambda_handler(event, context):
    print("Starting aggregation...")

    # Step 1: Scan all completed trips not yet aggregated
    response = table.scan(
        FilterExpression="trip_status = :s AND aggregation_flag = :f",
        ExpressionAttributeValues={
            ":s": "completed",
            ":f": False
        }
    )

    items = response.get("Items", [])
    if not items:
        print("No completed trips found for aggregation.")
        return

    # Step 2: Group trips by day_partition
    groups = defaultdict(list)
    for item in items:
        day = item.get("day_partition")
        if day:
            groups[day].append(item)

    now = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

    # Step 3: Aggregate KPIs per day
    for day, trips in groups.items():
        fares = [float(trip["fare_amount"]) for trip in trips if "fare_amount" in trip]

        if not fares:
            continue

        kpi = {
            "date": day,
            "total_fare": round(sum(fares), 2),
            "count_trips": len(fares),
            "average_fare": round(sum(fares) / len(fares), 2),
            "max_fare": round(max(fares), 2),
            "min_fare": round(min(fares), 2),
        }

        # Step 4: Save JSON to S3
        key = f"trip-kpis/dt={day}/kpi-{now}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(kpi),
            ContentType="application/json"
        )
        print(f"Saved KPI for {day} to s3://{S3_BUCKET}/{key}")