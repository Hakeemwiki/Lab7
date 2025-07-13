# Trip Data Real-Time Event-Driven Pipeline

## Table of Contents
- [Overview](#overview)
- [Business Problem](#business-problem)
- [Architecture](#architecture)
  - [Components](#components)
  - [Data Flow](#data-flow)
- [Data Format and Sample Schema](#data-format-and-sample-schema)
  - [Input Data](#input-data)
  - [Intermediate Data](#intermediate-data)
  - [Output KPIs](#output-kpis)
- [Validation Rules](#validation-rules)
- [DynamoDB Table Structure and Access Patterns](#dynamodb-table-structure-and-access-patterns)
- [Pipeline Workflow Explanation](#pipeline-workflow-explanation)
  - [Ingestion Phase](#ingestion-phase)
  - [Processing Phase](#processing-phase)
  - [Aggregation Phase](#aggregation-phase)
- [Error-Handling, Retry, and Logging Logic](#error-handling-retry-and-logging-logic)
  - [Error Handling](#error-handling)
  - [Retry Logic](#retry-logic)
  - [Logging Configuration](#logging-configuration)
- [Instructions to Simulate or Test the Pipeline Manually](#instructions-to-simulate-or-test-the-pipeline-manually)
  - [Unit Testing](#unit-testing)
  - [Integration Testing](#integration-testing)
  - [End-to-End Testing](#end-to-end-testing)
- [Design Choices](#design-choices)
  - [Scalability](#scalability)
  - [Cost Optimization](#cost-optimization)
  - [Maintainability](#maintainability)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Deployment Steps](#deployment-steps)
- [Performance Considerations](#performance-considerations)
- [Security and Compliance](#security-and-compliance)
- [Troubleshooting](#troubleshooting)
- [Guide for Recreating the Project](#guide-for-recreating-the-project)
  - [Setup Instructions](#setup-instructions)
  - [Configuration Details](#configuration-details)
  - [Verification Steps](#verification-steps)
- [Contributing](#contributing)
- [License](#license)

## Overview
This project implements a real-time, event-driven data pipeline to process trip data from a ride-sharing platform. The pipeline ingests trip start and end events via Amazon Kinesis, processes them using AWS Lambda functions, aggregates completed trips in Amazon DynamoDB, and generates daily Key Performance Indicators (KPIs) using an AWS Glue job. The resulting KPIs are stored in Amazon S3 for analysis and reporting. The solution leverages automated deployment via GitHub Actions, includes comprehensive testing, and integrates logging and error handling with Amazon CloudWatch.

## Business Problem
The ride-sharing platform faces the challenge of efficiently processing and analyzing trip data in near real-time to provide actionable insights. Key business needs include:
- **Revenue Optimization**: Tracking daily revenue, trip counts, and average fares to adjust pricing dynamically.
- **Anomaly Detection**: Monitoring maximum and minimum fares to identify pricing irregularities or fraudulent activities.
- **Operational Insights**: Delivering up-to-date KPIs to stakeholders for fleet management, driver allocation, and customer satisfaction improvements.
- **Scalability**: Handling increasing trip volumes during peak hours without performance degradation.

Manual data processing is impractical due to its time-intensive nature and susceptibility to errors, delaying critical insights. This pipeline automates ingestion, processing, and KPI generation, enabling real-time monitoring, historical analysis, and data-driven decision-making.

**[Architecture Diagram]**  
![alt text](docs/lab7.drawio2.svg)

## Architecture
### Components
- **Amazon S3**: Stores input CSV files (`trip_start.csv`, `trip_end.csv`) and outputs KPIs in `nsp-bolt-trip-analytics/metrics/`.
- **Amazon Kinesis**: Streams trip events via `TripEventsStream` for real-time ingestion.
- **AWS Lambda**: Executes `process_trip_begin`, `process_trip_finish`, and `match_and_complete` to handle event processing and matching.
- **Amazon DynamoDB**: Maintains raw and completed trip data in the `TripData` table.
- **AWS Glue**: Runs `GenerateTripMetrics` to compute daily KPIs from `TripData`.
- **Amazon CloudWatch**: Provides logging, monitoring, and alerting for pipeline components.
- **GitHub Actions**: Automates CI/CD for Lambda functions and Glue job scripts.
- **AWS EventBridge**: Schedules the Glue job daily at 1:00 AM GMT.

### Data Flow
Data flows from CSV files ingested by `send_to_kinesis.py` into Kinesis, processed by Lambda functions, stored in DynamoDB, aggregated by the Glue job, and saved to S3.

**[INSERT IMAGE: Data Flow Diagram]**  
*(Placeholder for a detailed data flow chart showing CSV -> Kinesis -> Lambda -> DynamoDB -> Glue -> S3.)*

## Data Format and Sample Schema
### Input Data
- **Files**: CSV files `trip_start.csv` and `trip_end.csv` in a local `data/` directory.
- **Sample Schema**:
  - `trip_start.csv`: `trip_id, pickup_datetime, estimated_fare_amount`
    - Example: `trip_001, 2025-07-12 04:38:00, 10.00`
  - `trip_end.csv`: `trip_id, dropoff_datetime, fare_amount`
    - Example: `trip_001, 2025-07-12 04:53:00, 10.50`

### Intermediate Data
- **TripData Table**:
  - `trip_id` (String): Unique trip identifier.
  - `sort_key` (String): e.g., `RAW#START#2025-07-12 04:38:00`, `RAW#END#2025-07-12 04:53:00`, or `COMPLETED#2025-07-12T04:38:00`.
  - `event_type` (String): `start`, `end`, or `completed`.
  - `pickup_datetime`/`dropoff_datetime` (String): Trip timestamps.
  - `estimated_fare`/`fare_amount` (Decimal): Fare values.
  - `trip_status` (String): `pending` or `completed`.

### Output KPIs
- Stored in S3 (`nsp-bolt-trip-analytics/metrics/YYYY/MM/DD/`):
  - `pickup_date` (String): Date of trips (e.g., 2025-07-12).
  - `total_fare` (Number): Sum of fares for the day.
  - `trip_count` (Number): Number of completed trips.
  - `average_fare` (Number): Average fare per trip.
  - `max_fare` (Number): Highest fare.
  - `min_fare` (Number): Lowest fare.

**[KPI Sample Output]**  
![alt text](docs/Athena.png)

## Validation Rules
- **Malformed Data**: Reject records missing required fields (`trip_id`, `pickup_datetime`, `estimated_fare_amount` for start; `trip_id`, `dropoff_datetime`, `fare_amount` for end).
- **Data Format**: Validate `pickup_datetime` and `dropoff_datetime` against `%Y-%m-%d %H:%M:%S`, and ensure `estimated_fare_amount`/`fare_amount` are numeric.
- **Matching**: Lambda ensures `trip_id` consistency between start and end events to create `COMPLETED#` records.

## DynamoDB Table Structure and Access Patterns
- **TripData Table**:
  - **Partition Key**: `trip_id` (String).
  - **Sort Key**: `sort_key` (String).
  - **Access Pattern**: Query by `trip_id` and `sort_key` prefix (`RAW#START#`, `RAW#END#`, `COMPLETED#`) to match and aggregate trips.
  - **Attributes**: `event_type`, `pickup_datetime`, `dropoff_datetime`, `estimated_fare`, `fare_amount`, `trip_status`.

## Ad-Hoc Querying with Athena
To enable flexible analysis of KPIs, AWS Glue crawlers have been configured to crawl the KPI data stored in `s3://nsp-bolt-trip-analytics/metrics/`. This creates a table in the AWS Glue Data Catalog, allowing ad-hoc querying using Amazon Athena. The crawler runs after each Glue job execution to update the schema and partition metadata.

- **Crawler Configuration**:
  - **Database**: `trip_kpis_db`
  - **Table Name**: `daily_trip_kpis`
  - **S3 Path**: `s3://nsp-bolt-trip-analytics/metrics/`
  - **Schedule**: Triggered manually or post-Glue job (configurable via EventBridge).
- **Athena Query Example**:
  ```sql
  SELECT pickup_date, total_fare, average_fare
  FROM trip_kpis_db.daily_trip_kpis
  WHERE pickup_date = '2025-07-12'
  ORDER BY total_fare DESC;



## Pipeline Workflow Explanation
### Ingestion Phase
- `send_to_kinesis.py` reads `trip_start.csv` and `trip_end.csv`, validates records, and sends batches to `TripEventsStream`.

### Processing Phase
- `process_trip_begin` writes `RAW#START#` records to `TripData`.
- `process_trip_finish` writes `RAW#END#` records to `TripData`.
- `match_and_complete` (DynamoDB Stream trigger) matches pairs, creates `COMPLETED#` records, and deletes raw data.

### Aggregation Phase
- `GenerateTripMetrics` Glue job scans `COMPLETED#` records, computes KPIs, and uploads to S3.
- EventBridge schedules the job daily at 1:00 AM GMT.
![alt text](docs/EventBridgeScheduler.png)

**[INSERT IMAGE: Workflow Diagram]**  
*(Placeholder for a flowchart detailing ingestion, processing, and aggregation phases.)*

## Error-Handling, Retry, and Logging Logic
### Error Handling
- Lambda returns HTTP 400 for invalid events; Glue job exits with error logs.
- `send_to_kinesis.py` stores failed records in S3 (`failed/start/` or `failed/end/`).

### Retry Logic
- `send_to_kinesis.py` retries Kinesis batches with a 0.2-second delay.
- `match_and_complete` retries raw record deletion up to 3 times.

### Logging Configuration
- Uses CloudWatch with `INFO` level logging.
- Logs include timestamps, error details, and debug information (e.g., DataFrame heads).

## Instructions to Simulate or Test the Pipeline Manually
### Unit Testing
- Run `python -m unittest discover tests` after installing `boto3`, `pandas`, and `moto`.
- Verify `test_process_trip_begin.py`, `test_process_trip_finish.py`, `test_match_and_complete.py`, and `test_compute_daily_metrics.py`.

### Integration Testing
- Mock Kinesis and DynamoDB with `moto` to test Lambda functions locally.
- Example: `python -c "from moto import mock_kinesis; with mock_kinesis(): print('Test Kinesis')"`

### End-to-End Testing
1. **Prepare Test Data**:
   - Create `data/trip_start.csv` and `data/trip_end.csv`:
     - `trip_start.csv`: `trip_001,2025-07-12 04:38:00,10.00`
     - `trip_end.csv`: `trip_001,2025-07-12 04:53:00,10.50`
2. **Run Ingestion**:
   - Execute `python send_to_kinesis.py`.
3. **Verify Lambda Processing**:
   - Check `TripData` for `RAW#` records in DynamoDB.
   - Review CloudWatch logs (`/aws/lambda/`).
4. **Trigger Matching**:
   - Ensure `match_and_complete` creates `COMPLETED#` records.
5. **Run Glue Job**:
   - Start `GenerateTripMetrics` in Glue console.
   - Confirm S3 output at `s3://nsp-bolt-trip-analytics/metrics/2025/07/12/`.
6. **Simulate Failure**:
   - Use a malformed CSV and check error logs.

## Design Choices
### Scalability
- Lambda scales with Kinesis shard count; Glue uses dynamic worker allocation.
- DynamoDB’s pay-per-request billing supports variable loads.

### Cost Optimization
- EventBridge scheduling minimizes Glue runtime.
- Lambda’s on-demand pricing aligns with event volume.

### Maintainability
- Modular design separates ingestion, processing, and aggregation.
- GitHub Actions ensures consistent deployments.

## Getting Started
### Prerequisites
- AWS account with `s3:*`, `kinesis:*`, `lambda:*`, `glue:*`, and `dynamodb:*` permissions.
- AWS CLI configured (region: `eu-north-1`).
- Docker and Python 3.11 with `boto3`, `pandas`, `moto`.

### Deployment Steps
- Follow the "Guide for Recreating the Project" below.

## Performance Considerations
- **Kinesis Shards**: Increase shard count for high-volume periods.
- **Glue Workers**: Adjust `WorkerType` (e.g., `G.2X`) and `NumberOfWorkers` (e.g., 10) based on data size.
- **DynamoDB Throughput**: Monitor read/write capacity and enable auto-scaling if needed.

## Security and Compliance
- **IAM Roles**: Use least-privilege policies (e.g., `LambdaExecutionRole` with `dynamodb:PutItem`).
- **Encryption**: Enable Kinesis and S3 server-side encryption (e.g., SSE-KMS).
- **Access Control**: Restrict S3 bucket access with bucket policies.

## Troubleshooting
- **Kinesis Failures**: Check `send_to_kinesis.py` logs for throttling; increase `BATCH_SIZE` or add delays.
- **Lambda Errors**: Review CloudWatch for `InvalidOperation` (e.g., invalid `Decimal`); adjust `validate_data`.
- **Glue Job Failures**: Verify `TripData` has `COMPLETED#` records; check `NoRegionError` by setting `AWS_DEFAULT_REGION`.

## Guide for Recreating the Project
### Setup Instructions
1. **Set Up S3 Bucket**:
   - Create a bucket: `aws s3 mb s3://nsp-bolt-trip-analytics --region eu-north-1`.
   - Enable versioning: `aws s3api put-bucket-versioning --bucket nsp-bolt-trip-analytics --versioning-configuration Status=Enabled`.

2. **Create DynamoDB Table**:
   - `aws dynamodb create-table \
     --table-name TripData \
     --attribute-definitions AttributeName=trip_id,AttributeType=S AttributeName=sort_key,AttributeType=S \
     --key-schema AttributeName=trip_id,KeyType=HASH AttributeName=sort_key,KeyType=RANGE \
     --billing-mode PAY_PER_REQUEST \
     --region eu-north-1`

3. **Set Up Kinesis Stream**:
   - `aws kinesis create-stream --stream-name TripEventsStream --shard-count 1 --region eu-north-1`.

4. **Create IAM Roles**:
   - **LambdaExecutionRole**: Allows `lambda:InvokeFunction`, `kinesis:*`, `dynamodb:*`.
   - **GlueTripRole**: Allows `glue:*`, `s3:*`, `dynamodb:*`.
   - Example: `aws iam create-role --role-name LambdaExecutionRole --assume-role-policy-document file://trust-policy.json`.

### Configuration Details
- **Lambda Deployment**:
  - Package `process_trip_begin.py`: `zip process_trip_begin.zip process_trip_begin.py`.
  - Deploy: `aws lambda create-function \
    --function-name process_trip_begin \
    --runtime python3.11 \
    --role arn:aws:iam::your-account-id:role/LambdaExecutionRole \
    --handler process_trip_begin.lambda_handler \
    --zip-file fileb://process_trip_begin.zip \
    --region eu-north-1`
  - Repeat for `process_trip_finish` and `match_and_complete`, adding Kinesis and DynamoDB Stream triggers.

- **Glue Job**:
  - Upload `GenerateTripMetrics.py` to `s3://nsp-bolt-trip-analytics/scripts/`.
  - Create job: AWS Glue Console > Jobs > Add Job, name `GenerateTripMetrics`, role `GlueTripRole`, script location `s3://nsp-bolt-trip-analytics/scripts/GenerateTripMetrics.py`.

- **EventBridge Trigger**:
  - `aws events put-rule --name RunGenerateTripMetricsDaily --schedule-expression "cron(0 1 * * ? *)" --region eu-north-1`.
  - `aws events put-targets --rule RunGenerateTripMetricsDaily --targets "Id"="1","Arn"="arn:aws:glue:eu-north-1:your-account-id:job/GenerateTripMetrics"`.

### Verification Steps
- Run `python send_to_kinesis.py` with test data.
- Check `TripData` for `RAW#` and `COMPLETED#` records.
- Manually trigger `GenerateTripMetrics` and verify S3 output at `s3://nsp-bolt-trip-analytics/metrics/2025/07/12/`.
- Monitor CloudWatch logs for errors.

## Contributing
Suggest enhancements (e.g., a dashboard, retry queue). Submit issues or pull requests with detailed descriptions.
