import boto3
import pandas as pd
import json
from datetime import datetime

dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')
TABLE_NAME = 'TripData'
S3_BUCKET = 'trip-analytics-bucket'
S3_BASE_PATH = f"metrics/{datetime.utcnow().strftime('%Y/%m/%d')}/"

def fetch_completed_data():
    items = []
    response = dynamodb.scan(
        TableName=TABLE_NAME,
        FilterExpression="begins_with(sort_key, :sk)",
        ExpressionAttributeValues={':sk': {'S': 'COMPLETED#'}}
    )
    items.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=TABLE_NAME,
            FilterExpression="begins_with(sort_key, :sk)",
            ExpressionAttributeValues={':sk': {'S': 'COMPLETED#'}},
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])
    return [{k: v['S'] if 'S' in v else float(v['N']) for k, v in item.items()} for item in items]

def generate_metrics_output():
    data = fetch_completed_data()
    if not data:
        return None, None

    df = pd.DataFrame(data)
    df['report_date'] = pd.to_datetime(df['pickup_datetime']).dt.date
    metrics = df.groupby('report_date').agg({
        'fare_amount': ['sum', 'count', 'mean', 'max', 'min']
    }).reset_index()
    metrics.columns = ['report_date', 'total_fare', 'trip_count', 'avg_fare', 'max_fare', 'min_fare']

    timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    output = {
        'metadata': {'generated_at': datetime.utcnow().isoformat(), 'total_records': len(df)},
        'daily_metrics': metrics.to_dict('records')
    }
    return json.dumps(output, indent=2), f"{S3_BASE_PATH}{timestamp}-metrics.json"

if __name__ == "__main__":
    metric_data, s3_key = generate_metrics_output()
    if metric_data:
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=metric_data, ContentType='application/json')
        print(f"Metrics saved to s3://{S3_BUCKET}/{s3_key}")
    else:
        print("No completed trips found")