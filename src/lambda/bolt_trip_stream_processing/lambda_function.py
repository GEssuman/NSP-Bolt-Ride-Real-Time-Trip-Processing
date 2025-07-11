import logging
import json
import base64
import boto3
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('nsp_bolt_trip_db')

def parse_datetime(value):
    """Ensure datetime is ISO 8601 string or return None."""
    try:
        if isinstance(value, str):
            return value  # assume already ISO formatted
        elif isinstance(value, datetime):
            return value.isoformat()
        return str(value)
    except:
        return None

def to_decimal(value):
    """Convert float to Decimal if needed."""
    if isinstance(value, float):
        return Decimal(str(value))
    return value

def lambda_handler(event, context):
    if not event or "Records" not in event:
        logger.info("No event available")
        return

    try:
        for record in event['Records']:
            payload = record['kinesis']['data']
            decoded_bytes = base64.b64decode(payload)
            decoded_str = decoded_bytes.decode('utf-8')
            trip_data = json.loads(decoded_str)
            logger.info(f"Trip Data Received: {trip_data}")

            trip_id = trip_data.get("trip_id")
            if not trip_id:
                logger.warning("Missing trip_id. Skipping record.")
                continue

            if trip_data.get("pickup_datetime"):
                table.update_item(
                    Key={"trip_id": trip_id},
                    UpdateExpression="""
                        SET 
                        pickup_location_id = :p,
                        dropoff_location_id = :d,
                        vendor_id = :v,
                        pickup_datetime = :pd,
                        estimated_dropoff_datetime = :edd,
                        estimated_fare_amount = :fare
                    """,
                    ExpressionAttributeValues={
                        ":p": trip_data.get("pickup_location_id"),
                        ":d": trip_data.get("dropoff_location_id"),
                        ":v": trip_data.get("vendor_id"),
                        ":pd": parse_datetime(trip_data.get("pickup_datetime")),
                        ":edd": parse_datetime(trip_data.get("estimated_dropoff_datetime")),
                        ":fare": to_decimal(trip_data.get("estimated_fare_amount")),
                    }
                )
                logger.info(f"Successfully updated Trip Start data for trip_id: {trip_id}")


            elif trip_data.get("dropoff_datetime"):
                table.update_item(
                    Key={"trip_id": trip_id},
                    UpdateExpression="""
                        SET 
                        dropoff_datetime = :dropoff_dt,
                        rate_code = :rate_code,
                        trip_distance = :trip_dist,
                        fare_amount = :fare_amt,
                        tip_amount = :tip_amt,
                        payment_type = :payment_type,
                        trip_type = :trip_type
                    """,
                    ExpressionAttributeValues={
                        ":dropoff_dt": parse_datetime(trip_data.get("dropoff_datetime")),
                        ":rate_code": to_decimal(trip_data.get("rate_code")),
                        ":trip_dist": to_decimal(trip_data.get("trip_distance")),
                        ":fare_amt": to_decimal(trip_data.get("fare_amount")),
                        ":tip_amt": to_decimal(trip_data.get("tip_amount")),
                        ":payment_type": to_decimal(trip_data.get("payment_type")),
                        ":trip_type": to_decimal(trip_data.get("trip_type")),
                    }
                )
                logger.info(f"Successfully updated Trip End data for trip_id: {trip_id}")
            else:
                logger.warning(f"Trip record missing pickup/dropoff datetimes: {trip_data}")

    except Exception as error:
        logger.error(f"Error occurred while processing trip event:\n{error}")
