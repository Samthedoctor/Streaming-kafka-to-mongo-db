# consumer.py

import os
import json
from confluent_kafka import Consumer, KafkaError 
from pymongo import MongoClient
import logging
import time

# --- Configuration ---
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get configuration from environment variables
KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'yfinance_intraday_ohlcv')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'mongodb-stock-sink')
MONGO_URI = os.environ.get('MONGO_URI') # Your MongoDB connection string
MONGO_DB_NAME = os.environ.get('MONGO_DATABASE', 'stocks')
MONGO_COLLECTION_NAME = os.environ.get('MONGO_COLLECTION', 'intraday_data')

# Validate that the MONGO_URI is set
if not MONGO_URI:
    logging.error("MONGO_URI environment variable not set. Please provide your MongoDB connection string.")
    exit(1)

# --- MongoDB Client Setup ---
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    # Test connection
    mongo_client.admin.command('ping')
    logging.info(f"Successfully connected to MongoDB -> Database: '{MONGO_DB_NAME}', Collection: '{MONGO_COLLECTION_NAME}'")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# --- Kafka Consumer Setup ---
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest' # Start reading from the beginning of the topic if no offset is stored
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])
logging.info(f"Kafka Consumer subscribed to topic: {KAFKA_TOPIC}")

# --- Main Loop ---
def consume_and_store():
    """
    Consumes messages from Kafka and stores them in MongoDB.
    """
    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0) # Wait for 1 second

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: # <-- CHANGED: Use KafkaError
                    # This is expected, so we can just continue polling.
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # This is common when the consumer starts before the producer creates the topic.
                    # We log a warning and wait before trying again.
                    logging.warning(f"Topic '{KAFKA_TOPIC}' not available yet. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                else:
                    # For any other error, we log it as fatal and break the loop.
                    logging.error(f'Fatal Kafka error: {msg.error()}')
                    break

            # Message received successfully
            try:
                # 1. Decode the message from bytes to string
                message_value = msg.value().decode('utf-8')
                # 2. Parse the JSON string into a Python dictionary
                data = json.loads(message_value)

                # 3. Insert the data into MongoDB
                result = collection.insert_one(data)
                logging.info(f"Inserted message for ticker '{data.get('ticker')}' into MongoDB with ID: {result.inserted_id}")

            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from message: {msg.value()}")
            except Exception as e:
                logging.error(f"An error occurred while processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer process interrupted by user.")
    finally:
        # Clean up and close the consumer
        consumer.close()
        logging.info("Kafka consumer closed.")

if __name__ == '__main__':
    consume_and_store()