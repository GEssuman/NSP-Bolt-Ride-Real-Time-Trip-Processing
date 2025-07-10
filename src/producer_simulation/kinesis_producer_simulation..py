import boto3
import json

kinesis = boto3.client('kinesis', region_name='eu-north-1')

data = {
    "trip_id": "123",
    "driver_id": "abc",
    "start_time": "2025-04-10T08:30:00Z"
}

response = kinesis.put_record(
    StreamName='bolt_ride_time',
    Data=json.dumps(data),
    PartitionKey='start_time'
)

print("Sent to Kinesis:", response)
