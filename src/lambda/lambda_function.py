import logging
import json
import base64
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('nsp_bolt_trip_db')

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

           # Create or update the trip data in DynamoDB
            if trip_data["pickup_datetime"]:
                table.update_item(
                    Key={"trip_id": trip_data['trip_id']},
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
                        ":pd": trip_data.get("pickup_datetime"),
                        ":edd": trip_data.get("estimated_dropoff_datetime"),
                        ":fare": trip_data.get("estimated_fare_amount"),
                    }
                )
            elif trip_data["dropoff_datetime"]:
               table.update_item(
                Key={"trip_id": trip_data['trip_id']},
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
                    ":dropoff_dt": trip_data.get("dropoff_datetime"),
                    ":rate_code": trip_data.get("rate_code"),
                    ":trip_dist": trip_data.get("trip_distance"),
                    ":fare_amt": trip_data.get("fare_amount"),
                    ":tip_amt": trip_data.get("tip_amount"),
                    ":payment_type": trip_data.get("payment_type"),
                    ":trip_type": trip_data.get("trip_type"),
                }
            )
    except Exception as error:
        logger.info(f"Error occured while logging event information:\n{error}")