"""
alvaro_kafka_consumer.py

Consume messages from a Kafka topic and process them.
"""
import os
import sys
import time
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import logging


def setup_logging():
    """Sets up basic configuration for the logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("logs/consumer_log.log")],
    )
    logger = logging.getLogger(__name__)
    return logger


logger = setup_logging()


def create_kafka_consumer(topic: str, group_id: str):
    """Creates a Kafka Consumer instance."""
    try:
        bootstrap_servers = os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9092")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",  # Start from beginning
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: x.decode('utf-8'),
        )
        consumer.subscribe([topic])
        return consumer

    except Exception as e:
        logger.exception("Error creating Kafka consumer")
        return None


def get_kafka_topic():
    topic = os.environ.get("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id():
    group_id = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "default_consumer_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def process_message(message: str) -> None:
    try:
        decoded_message = json.loads(message)
        logger.info(f"Processing message: {decoded_message}")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding message: {e}, skipping message...")
    except Exception as e:
        logger.error(f"Error processing message: {e}, skipping message...")


def verify_services():
    try:
        broker_address = os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9092")
        import socket

        socket.create_connection((broker_address.split(":")[0], int(broker_address.split(":")[1])))
        logger.info("Services verified successfully.")
        return True

    except Exception as e:
        logger.exception(f"Error verifying services: {e}")
        return False


def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9092")
        )
        return producer
    except Exception as e:
        logger.exception("Error creating Kafka producer:")
        return None





def main():
    logger.info("START consumer.")
    if not verify_services():
        logger.critical("Failed to verify services. Exiting...")
        return

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()

    consumer = create_kafka_consumer(topic, group_id)
    if not consumer:
        logger.critical("Failed to create consumer. Exiting...")
        return

    logger.info(f"Starting message consumption from topic '{topic}'...")
    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info("END consumer.")


if __name__ == "__main__":
    main()
