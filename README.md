# NSP Bolt Ride – Real-Time Trip Processing Project

**Objective**: 
Design and implement a real-time trip data ingestion and analytics pipeline for a ride-hailing service, simulating a real-world, event-driven architecture using AWS services.

### Project Context
NSP Bolt Ride is a ride-hailing company operating through a mobile app. This project aims to process and enrich trip data in near real time to support analytics and operational monitoring.

The solution demonstrates event-driven data streaming and real-time processing using serverless components.

Data Info:
Two types of trip events are ingested through Amazon Kinesis:
1. Trip Start
    Schema:
    ```

    ```
2. Trip End
    Schema:
    ```
    
    ```

### AWS Servives Used:
- Amazon Kinesis (for event ingestion)
- AWS Lambda (for stream processing)
- Amazon DynamoDB (for storage)
- AWS Glue or custom logic (for aggregation)
- Amazon S3 (for storing final output)

### Data Workflow
**1. Event Ingestion:**
Trip data (start and end) is published into Kinesis Data Streams by producers (simulated or real).

**2. Lambda Processing:**
The stream triggers a Lambda function, which performs:

- Initial validation
- Basic processing
- Storage of event records into DynamoDB


**3. Conditional Triggering:**
Based on certain conditions (e.g., trip completed, daily batch), DynamoDB Streams trigger a Glue job.

**4. Data Aggregation:**
AWS Glue performs aggregations (e.g., total trips per driver/region) from DynamoDB data and stores results in Amazon S3 in a structured format (e.g., Parquet or CSV).