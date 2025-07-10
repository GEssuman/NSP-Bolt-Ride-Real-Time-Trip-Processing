import logging
import json
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    if not event or "Records" not in event:
        logger.info("No event available")
        return
    try:
        # Extract and decode the data
        for record in event['Records']:
            payload = record['kinesis']['data']
            decoded_bytes = base64.b64decode(payload)
            decoded_str = decoded_bytes.decode('utf-8')
            trip_data = json.loads(decoded_str)
            logger.info(trip_data)

            logger.info(f"Context: {context}")
    except Exception as error:
        logger.info(f"Error occured while logging event information:\n{error}")