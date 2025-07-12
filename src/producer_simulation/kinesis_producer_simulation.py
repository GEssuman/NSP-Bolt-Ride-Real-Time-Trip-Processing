import boto3
import json
import pandas as pd
import random
import time
import logging
from botocore.exceptions import BotoCoreError, ClientError
import os

# ----------------------------------------
# Configuration Constants
# ----------------------------------------git s
REGION = os.getenv('AWS_DEFAULT_REGION')
STREAM_NAME = "bolt_ride_time"
TRIP_START_FILE = "./../../data/trip_start.csv"
TRIP_END_FILE = "./../../data/trip_end.csv"
SAMPLE_SIZE = 10  # Number of trip samples to simulate

# ----------------------------------------
# Logging Configuration
# ----------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def init_kinesis_client(region):
    """
    Initialize and return a boto3 Kinesis client for the given AWS region.
    """
    try:
        client = boto3.client("kinesis", region_name=region)
        logging.info("Kinesis client initialized.")
        return client
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to initialize Kinesis client: {e}")
        raise


def load_sampled_trip_data(start_file, end_file, sample_size):
    """
    Load and randomly sample a fixed number of records from both trip start and end CSV files.

    Args:
        start_file (str): Path to trip start CSV.
        end_file (str): Path to trip end CSV.
        sample_size (int): Number of records to sample from each.

    Returns:
        tuple: Sampled trip_start and trip_end records as lists of dictionaries.
    """
    try:
        start_df = pd.read_csv(start_file)
        end_df = pd.read_csv(end_file)

        # Sample the same indices from both datasets to ensure matched trip_id pairs
        sampled_indices = random.sample(range(min(len(start_df), len(end_df))), sample_size)
        start_sample = start_df.iloc[sampled_indices].to_dict(orient="records")
        end_sample = end_df.iloc[sampled_indices].to_dict(orient="records")

        logging.info(f"Sampled {sample_size} trips from each file.")
        return start_sample, end_sample
    except Exception as e:
        logging.error(f"Failed to load or sample trip data: {e}")
        raise


def send_to_kinesis(client, record, partition_key, record_type):
    """
    Send a single trip event record to the Kinesis stream.

    Args:
        client (boto3 client): Initialized Kinesis client.
        record (dict): The trip event data to send.
        partition_key (str): Key to partition the stream (usually driver_id).
        record_type (str): Label to indicate 'Trip Start' or 'Trip End'.
    """
    try:
        client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )
        logging.info(f"Sent {record_type}: {record['trip_id']}")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to send {record_type} for trip_id={record['trip_id']}: {e}")


def simulate_random_trip_events(kinesis_client, starts, ends):
    """
    Simulate trip event publishing by sending start and end events in random order to Kinesis.

    Args:
        kinesis_client (boto3 client): Kinesis client used for publishing.
        starts (list): List of trip start records.
        ends (list): List of trip end records.
    """
    # Merge and tag start/end events
    events = []
    for start, end in zip(starts, ends):
        events.append(("Trip Start", start["trip_id"], start))
        events.append(("Trip End", end["trip_id"], end))

    # Randomize event order to mimic real-world concurrency
    random.shuffle(events)
    logging.info("Starting randomized trip event simulation...")

    for event_type, driver_id, record in events:
        send_to_kinesis(kinesis_client, record, driver_id, event_type)
        time.sleep(random.uniform(0.5, 2))  # Random delay between events

    logging.info("Random trip simulation completed.")


def main():
    """
    Main entry point: Initializes the Kinesis client, loads sample data, and starts the simulation.
    """
    try:
        client = init_kinesis_client(REGION)
        starts, ends = load_sampled_trip_data(TRIP_START_FILE, TRIP_END_FILE, SAMPLE_SIZE)
        logging.info(f"Sample trip starts: {starts}")
        logging.info(f"Sample trip ends: {ends}")
        simulate_random_trip_events(client, starts, ends)
    except Exception as e:
        logging.error(f"Simulation failed: {e}")


if __name__ == "__main__":
    main()
