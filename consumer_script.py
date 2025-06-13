import json
import time
import os
from kafka import KafkaConsumer # Import KafkaConsumer

# --- Kafka configuration ---
KAFKA_BROKER = "localhost:9092" # The address of your Kafka broker (set in docker-composer.yml)
KAFKA_TOPIC = "purchase_events" # The Kafka topic to consume messages from
CONSUMER_GROUP_ID = "purchase_logger_group" # A unique identifier for this consumer group
LOG_DIR = "kafka_logs" # The directory where the received messages will be saved as JSON files

def create_log_directory():
    """Ensures the log directory exists. If it doesn't, it creates it."""
    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"Log directory '{LOG_DIR}' ensured.")

def log_message_to_json_file(message_data, topic, partition, offset, timestamp, key):
    """
    Writes a single Kafka message's value and selected metadata to a unique JSON file.
    """
    try:
        # Construct a unique filename using topic, partition, offset, and a current timestamp.
        # This helps ensure files don't overwrite each other if multiple messages arrive very quickly.
        filename = os.path.join(LOG_DIR, f"{topic}-{partition}-{offset}-{int(time.time())}.json")

        # Create a dictionary that combines the original message data with Kafka metadata.
        # We ensure key is stored as a string.
        log_entry = {
            "kafka_metadata": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp": timestamp, # Kafka's timestamp (epoch milliseconds)
                "key": key # The message key
            },
            "data": message_data # The actual purchase data
        }

        # Open the file in write mode ('w') with UTF-8 encoding.
        # json.dump writes the Python dictionary to the file as a JSON string.
        # 'ensure_ascii=False' allows non-ASCII characters directly.
        # 'indent=4' makes the JSON output human-readable with 4-space indentation.
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False, indent=4)
        print(f"Logged message to: {filename}")
    except Exception as e:
        print(f"Error writing to JSON file for message (topic: {topic}, offset: {offset}): {e}")

if __name__ == "__main__":
    create_log_directory() # First, make sure the directory for logs exists
    print("Initializing Kafka Consumer...")

    consumer = None
    try:
        # Configure the Kafka Consumer.
        # bootstrap_servers: List of Kafka brokers.
        # group_id: Unique identifier for this consumer group.
        # auto_offset_reset: Start from 'earliest' messages if no offset found.
        # enable_auto_commit: Automatically commit offsets periodically.
        # auto_commit_interval_ms: How often offsets are committed.
        # value_deserializer: Function to convert message value bytes to Python dict.
        # key_deserializer: Function to convert message key bytes to Python string.
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
        )

        print(f"Consumer listening on topic: {KAFKA_TOPIC} in group: {CONSUMER_GROUP_ID}")
        print("-" * 30)

        # Main loop for consuming messages.
        for message in consumer:
            # `message` is a ConsumerRecord object, containing details about the message:
            # message.topic: The topic the message came from.
            # message.partition: The partition number within the topic.
            # message.offset: The sequential offset ID of the message in its partition.
            # message.timestamp: The timestamp of the message (in milliseconds since epoch).
            # message.key: The message key (if provided by producer).
            # message.value: The actual message payload (our purchase data).

            print(f"Received message from topic {message.topic}, partition {message.partition}, offset {message.offset}:")
            print(f"  Key: {message.key}")
            print(f"  Value: {message.value}")

            # Call the function to save the received message, now passing metadata.
            log_message_to_json_file(
                message.value,
                message.topic,
                message.partition,
                message.offset,
                message.timestamp, # Pass the Kafka timestamp
                message.key        # Pass the Kafka key
            )

    except KeyboardInterrupt:
        print("\n--- Consumer stopped by user. ---")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed.")
