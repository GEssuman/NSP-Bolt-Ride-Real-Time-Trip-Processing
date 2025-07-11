import boto3
import os
import json
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load environment variables
queue_url = os.environ.get('QUEUE_URL')

if not queue_url:
    logger.error("QUEUE_URL environment variable not set.")
    raise EnvironmentError("QUEUE_URL must be defined in environment variables.")

# Initialize SQS client
try:
    sqs = boto3.client('sqs')
except (BotoCoreError, ClientError) as e:
    logger.exception("Failed to initialize SQS client.")
    raise e

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    success_count = 0
    failure_count = 0

    for record in event.get('Records', []):
        try:
            event_name = record.get('eventName')
            if event_name != 'MODIFY':
                logger.info(f"Skipping non-MODIFY event: {event_name}")
                continue

            new_image = record['dynamodb'].get('NewImage', {})
            if not new_image:
                logger.warning(f"No NewImage found in record: {json.dumps(record)}")
                continue

            # Convert DynamoDB JSON to regular JSON
            message = {k: list(v.values())[0] for k, v in new_image.items()}
            logger.info(f"Parsed message: {message}")

            # Send message to SQS
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )
            logger.info(f"Message sent to SQS with MessageId: {response.get('MessageId')}")
            success_count += 1

        except (BotoCoreError, ClientError) as sqs_err:
            logger.exception(f"Failed to send message to SQS: {sqs_err}")
            failure_count += 1
        except Exception as e:
            logger.exception(f"Unexpected error processing record: {e}")
            failure_count += 1

    logger.info(f"Lambda execution summary: {success_count} successful, {failure_count} failed")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "success": success_count,
            "failed": failure_count
        })
    }
