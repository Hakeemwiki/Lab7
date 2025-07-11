import boto3
import pandas as pd
import io
import logging
from datetime import datetime

# --- Configuration ---
DYNAMODB_TABLE_NAME = 'TripData'  # Your DynamoDB table
S3_BUCKET_NAME = 'nsp-bolt-trip-analytics'  # Your S3 bucket
BASE_PATH = 'metrics/'  # Base path for metrics

# Generate timestamped path
now = datetime.utcnow()  # Use UTC to align with Lambda logs
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
S3_OUTPUT_KEY = f'{BASE_PATH}{year}/{month}/{day}/{timestamp}-daily_trip_kpis.csv'
LATEST_KEY = f'{BASE_PATH}latest/daily_trip_kpis.csv'

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()

# --- AWS Clients ---
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

# --- Function to Read Data from DynamoDB ---
def scan_dynamodb_table(table_name):
    """Scans a DynamoDB table and returns items with COMPLETED# prefix."""
    logger.info(f"Scanning DynamoDB table: {table_name}")
    items = []
    last_evaluated_key = None

    while True:
        try:
            if last_evaluated_key:
                response = dynamodb.scan(
                    TableName=table_name,
                    ExclusiveStartKey=last_evaluated_key,
                    FilterExpression="begins_with(sort_key, :sk)",
                    ExpressionAttributeValues={':sk': {'S': 'COMPLETED#'}}
                )
            else:
                response = dynamodb.scan(
                    TableName=table_name,
                    FilterExpression="begins_with(sort_key, :sk)",
                    ExpressionAttributeValues={':sk': {'S': 'COMPLETED#'}}
                )

            for item in response.get('Items', []):
                processed_item = {}
                for key, value in item.items():
                    if 'S' in value:
                        processed_item[key] = value['S']
                    elif 'N' in value:
                        processed_item[key] = float(value['N'])
                items.append(processed_item)

            last_evaluated_key = response.get('LastEvaluatedKey')
            logger.info(f"Scanned {len(items)} items so far")

            if not last_evaluated_key:
                break

        except Exception as e:
            logger.error(f"Error scanning DynamoDB table {table_name}: {e}")
            return None

    logger.info(f"Finished scanning. Total items retrieved: {len(items)}")
    return items

# --- Main Script Logic ---
if __name__ == "__main__":
    # 1. Read data from DynamoDB
    dynamodb_items = scan_dynamodb_table(DYNAMODB_TABLE_NAME)

    if not dynamodb_items:
        logger.error("No data retrieved from DynamoDB or an error occurred. Exiting.")
        exit(1)

    # 2. Load data into Pandas DataFrame
    try:
        df = pd.DataFrame(dynamodb_items)
        logger.info(f"Loaded {len(df)} items into Pandas DataFrame.")
        logger.info("DataFrame head:\n%s", df.head().to_string())
        logger.info("DataFrame info:\n%s", df.info())

        required_columns = ['pickup_datetime', 'fare_amount', 'trip_id']
        if not all(col in df.columns for col in required_columns):
            logger.error("Required columns (%s) not found in DataFrame.", required_columns)
            exit(1)

        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
        df.dropna(subset=['pickup_datetime'], inplace=True)
        logger.info(f"DataFrame after dropping invalid pickup_datetime: {len(df)} rows")

        df['fare_amount'] = pd.to_numeric(df['fare_amount'], errors='coerce')
        df.dropna(subset=['fare_amount'], inplace=True)
        logger.info(f"DataFrame after dropping invalid fare_amount: {len(df)} rows")

    except Exception as e:
        logger.error("Error creating or processing DataFrame: %s", e)
        exit(1)

    # 3. Calculate KPIs
    try:
        logger.info("Calculating KPIs...")
        df['pickup_date'] = df['pickup_datetime'].dt.date

        total_fare = df.groupby('pickup_date')['fare_amount'].sum().reset_index().rename(columns={'fare_amount': 'total_fare'})
        logger.info("Calculated total fare per day.")

        trip_count = df.groupby('pickup_date')['trip_id'].count().reset_index().rename(columns={'trip_id': 'trip_count'})
        logger.info("Calculated count of trips.")

        average_fare = df.groupby('pickup_date')['fare_amount'].mean().reset_index().rename(columns={'fare_amount': 'average_fare'})
        logger.info("Calculated average fare.")

        max_fare = df.groupby('pickup_date')['fare_amount'].max().reset_index().rename(columns={'fare_amount': 'max_fare'})
        logger.info("Calculated maximum fare.")

        min_fare = df.groupby('pickup_date')['fare_amount'].min().reset_index().rename(columns={'fare_amount': 'min_fare'})
        logger.info("Calculated minimum fare.")

    except Exception as e:
        logger.error("Error calculating KPIs: %s", e)
        exit(1)

    # 4. Combine KPIs into a single DataFrame
    try:
        logger.info("Combining KPIs...")
        kpi_df = total_fare
        for df_to_merge in [trip_count, average_fare, max_fare, min_fare]:
            kpi_df = pd.merge(kpi_df, df_to_merge, on='pickup_date', how='left')

        logger.info("Combined KPI DataFrame head:\n%s", kpi_df.head().to_string())

    except Exception as e:
        logger.error("Error combining KPIs: %s", e)
        exit(1)

    # 5. Write to CSV
    try:
        logger.info("Writing KPIs to CSV...")
        kpi_df['pickup_date'] = kpi_df['pickup_date'].astype(str)

        # Use StringIO to handle CSV content
        csv_buffer = io.StringIO()
        kpi_df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        logger.info("CSV content length: %d bytes", len(csv_content))  # Debug CSV size

        # Log a sample of the CSV content for verification
        logger.info("CSV sample (first 200 chars):\n%s", csv_content[:200] if csv_content else "Empty")

        # Upload to S3 with timestamped path
        logger.info(f"Uploading CSV to S3: s3://{S3_BUCKET_NAME}/{S3_OUTPUT_KEY}")
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_OUTPUT_KEY,
            Body=csv_content.encode('utf-8'),  # Ensure UTF-8 encoding
            ContentType='text/csv'
        )

        # Upload latest version
        logger.info(f"Uploading latest CSV to S3: s3://{S3_BUCKET_NAME}/{LATEST_KEY}")
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=LATEST_KEY,
            Body=csv_content.encode('utf-8'),  # Ensure UTF-8 encoding
            ContentType='text/csv'
        )

        logger.info(f"Successfully uploaded KPI results to {S3_OUTPUT_KEY}")
        logger.info(f"Also uploaded to 'latest' path: {LATEST_KEY}")

    except Exception as e:
        logger.error("Error writing to S3: %s", e)
        exit(1)

    logger.info("Glue job finished.")