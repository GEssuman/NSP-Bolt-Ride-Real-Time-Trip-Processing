# NSP Bolt Ride – Real-Time Trip Processing Project

**Objective**: 
Design and implement a real-time trip data ingestion and analytics pipeline for a ride-hailing service, simulating a real-world, event-driven architecture using AWS services.

### Project Context
NSP Bolt Ride is a ride-hailing company operating through a mobile app. This project aims to process and enrich trip data in near real time to support analytics and operational monitoring.

The solution demonstrates event-driven data streaming and real-time processing using serverless components.

Data Info:
- Two types of trip events are ingested through Amazon Kinesis:
1. **Trip StartSchema:**

| Field Name                  | Type     | Description                                                  |
|----------------------------|----------|--------------------------------------------------------------|
| `trip_id`                  | string   | Unique identifier for the trip                               |
| `pickup_location_id`       | string   | ID of the pickup location                                     |
| `dropoff_location_id`      | string   | ID of the estimated dropoff location                          |
| `vendor_id`                | string   | ID of the ride vendor (e.g., 1 = Bolt, 2 = Uber)              |
| `pickup_datetime`          | string   | Timestamp when the trip starts (`YYYY-MM-DD HH:MM:SS`)        |
| `estimated_dropoff_datetime` | string | Estimated end time of the trip (`YYYY-MM-DD HH:MM:SS`)        |
| `estimated_fare_amount`    | string   | Estimated fare in local currency (e.g., `"12.50"`)            |
         

2. **Trip End Schema:**


| Field Name         | Type     | Description                                                    |
|--------------------|----------|----------------------------------------------------------------|
| `dropoff_datetime` | string   | Actual time the trip ended (`YYYY-MM-DD HH:MM:SS`)             |
| `rate_code`        | string   | Rate code used for the trip (e.g., standard, night rate, etc.) |
| `passenger_count`  | string   | Number of passengers                                           |
| `trip_distance`    | string   | Distance traveled in kilometers or miles                       |
| `fare_amount`      | string   | Total fare amount for the trip                                 |
| `tip_amount`       | string   | Amount tipped by the passenger                                 |
| `payment_type`     | string   | Payment method used (e.g., 1 = Card, 2 = Cash)                 |
| `trip_type`        | string   | Type of trip (e.g., 1 = Standard, 2 = Shared)                  |
| `trip_id`          | string   | Unique identifier for the trip                                 |

### Daily KPI Aggregation Schema

| Field Name     | Type    | Description                                                                 |
|----------------|---------|-----------------------------------------------------------------------------|
| `pickup_date`  | date    | The day of trip pickup (`YYYY-MM-DD`), used as the partition key            |
| `total_fare`   | double  | Sum of all fares collected from completed trips on that day                 |
| `count_trips`  | bigint  | Total number of completed trips on that day                                 |
| `average_fare` | double  | Weighted average fare: `(sum(fare) / count)`                                |
| `max_fare`     | double  | Highest single fare recorded among completed trips                          |
| `min_fare`     | double  | Lowest fare recorded among completed trips                                  |


### AWS Servives Used:
- **Amazon Kinesis**  – Ingests raw trip start and end events
- **AWS Lambda** – Processes Kinesis stream events (validates & stores into DynamoDB)
- **Amazon DynamoDB** – Stores raw trip events and enables change tracking via streams
- **AWS Lambda** – Consumes DynamoDB streams and pushes completed trips into SQS
- **Amazon SQS** – Acts as a buffer for completed trips waiting to be aggregated
- **AWS Glue** – Polls SQS every 10 minutes, aggregates trip data, and performs incremental upserts into Delta Lake
- **Amazon S3**  – Storage layer for Delta Lake (partitioned KPI table)
- **Delta Lake** – Provides ACID-compliant, partitioned storage for daily KPI metrics

### Data Workflow
**1. Event Ingestion:**
Trip data (start and end) is published into Kinesis Data Streams by producers (simulated or mobile clients ).

**2. Lambda Processing(Kinesis Consumer):**
The stream triggers a Lambda function, which performs:

- Initial validation
- Basic processing
- Storage of event records into DynamoDB


**3. Conditional Triggering:**
Changes in the DynamoDB table are streamed via DynamoDB Streams. A Lambda function is triggered by these stream events.
This Lambda function:
- Checks if the event type is a MODIFY operation
- Validates whether the trip record has reached a complete state (both start and end timestamps exist)
- If it's a complete trip, the Lambda serializes the trip data and sends it to the Amazon SQS queue
This design ensures that only completed trips are forwarded for aggregation, reducing downstream noise and improving processing efficiency in AWS Glue.


**4. Data Aggregation:**
An AWS Glue job is scheduled to run every 10 minutes to process new trip data.
During each run, the Glue job:
- Polls messages from Amazon SQS (each message represents a completed trip)
- Upserts the aggregated metrics into a Delta Lake table stored in Amazon S3, partitioned by pickup_date
- Deletes processed messages from the SQS queue

This allows the system to maintain an incrementally updated and queryable data store for operational analytics.


## Project Structure
```
nsp-bolt-ride/
│
├── README.md                      # Project overview and setup instructions
├── requirements.txt               # Python dependencies for local development (if needed)
│
├── .github/
│   └── workflows/
│       ├── deploy-glue.yaml       # CI/CD for Glue Job (ETL)
│       └── deploy-lambda.yaml     # CI/CD for Lambda functions
│
├── src/
│   ├── glue_job/
│   │   └── glue_script/
│   │       └── nsp-bolt-trip-aggregation-job.py   # Glue script for KPI aggregation
│   │
│   └── lambda/
│       ├── bolt_trip_stream_processing/
│       │   └── lambda_function.py  # Lambda to process trip events from Kinesis
│       │
│       └── DDBStreamEvent/
│           └── lambda_function.py  # Lambda to filter complete trips from DynamoDB → SQS
│── docs/
│
│
└── data/
    └── trip_end.csv
    └── trip_start.csv
```


### Deployment
**Prerequisites**
- AWS CLI & Docker installed
- AWS IAM permission (ECS, Step functions, SNS, S3, lambda)
- AWS Services set in GitHub 
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_REGION
    - AWS_ACCOUNT_ID


 ## CI/CD Deployment with GitHub Actions
This project uses GitHub Actions to automate the deployment of AWS Lambda functions and Glue ETL jobs on code changes to specified branches (dev).