"""
alvaro_kafka_producer.py

Produce some streaming buzz strings and send them to a Kafka topic.
"""
import os
import sys
import time
import json
from uuid import uuid4
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging


def setup_logging():
    """Sets up basic configuration for the logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("logs/project_log.log")],
    )
    logger = logging.getLogger(__name__)
    return logger


logger = setup_logging()


def get_kafka_topic():
    topic = os.getenv("KAFKA_TOPIC", "sports_data")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval():
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval



def generate_sports_data(producer, topic, interval_secs):
    try:
        while True:
            sports = ["Baseball", "Basketball", "Football", "Hockey"]
            sport = sports[int(time.time() % len(sports))]
            team1 = f"Team {int(time.time() % 10)}"
            team2 = f"Team {int(time.time() % 10 + 5)}"
            score1 = int(time.time() % 30 + 1)
            score2 = int(time.time() % 21 + 1)

            game_data = {
                "gameId": str(uuid4()),
                "sport": sport,
                "team1": team1,
                "team2": team2,
                "score1": score1,
                "score2": score2,
                "timestamp": datetime.now().isoformat(),
            }

            data_json = json.dumps(game_data)  # Convert to JSON string

            logger.info(f"Sending message: {data_json}")
            try:
                producer.send(topic=topic, value=data_json.encode('utf-8'))
            except Exception as e:
                logger.exception(f"Error sending message: {e}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")    
    finally:
        producer.close()
        logger.info("Kafka producer closed.")



def verify_services():
    # Replace with your actual service verification logic.
    # Example: Use a health check or similar mechanism
    try:
        broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
        
        #Check if the broker can be connected (very basic check, you will need comprehensive verification in a real application)
        import socket
        
        socket.create_connection((broker_address.split(':')[0], int(broker_address.split(':')[1])))
        logger.info("Services verified successfully.")
        return True

    except Exception as e:
        logger.exception(f"Error verifying services: {e}")
        return False

def create_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9092"))
        return producer
    except Exception as e:
        logger.exception("Error creating Kafka producer:")
        return None


def create_kafka_topic(topic_name):
    try:
        # Your actual topic creation logic here (e.g., using KafkaAdminClient).
        # In this placeholder, it's assumed the topic already exists.
        logger.info(f"Topic '{topic_name}' already exists.")
        return True
    except Exception as e:
        logger.exception(f"Error creating or verifying topic {topic_name}")
        return False


def main():
    try:
        logger.info("START producer.")
        if not verify_services():
            logger.critical("Failed to verify services. Exiting...")
            return

        topic = get_kafka_topic()
        interval_seconds = get_message_interval()
        producer = create_kafka_producer()

        if producer is None:
            logger.critical("Failed to create Kafka producer. Exiting...")
            return

        if not create_kafka_topic(topic):
            logger.critical(f"Failed to create topic: {topic}. Exiting...")
            return

        logger.info(f"Starting message production to topic '{topic}'...")
        generate_sports_data(producer, topic, interval_seconds)

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        logger.info("END producer.")


if __name__ == "__main__":
    main()
