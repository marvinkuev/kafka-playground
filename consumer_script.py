import json
import time
import os
from kafka import KafkaConsumer

# --- Kafka configuration ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "purchase_events"
CONSUMER_GROUP_ID = "purchase_logger_group"
LOG_DIR = "kafka_logs"

def create_log_directory():
    """Ensures the log directory exists."""
    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"Log directory '{LOG_DIR}' ensured.")

def log_message_to_json_file(message_data, topic, partition, offset, timestamp, key):
    """Writes a single Kafka message to a unique JSON file."""
    try:
        filename = os.path.join(LOG_DIR, f"{topic}-{partition}-{offset}-{int(time.time())}.json")
        log_entry = {
            "kafka_metadata": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp": timestamp,
                "key": key
            },
            "data": message_data
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False, indent=4)
        print(f"Logged message to: {filename}")
    except Exception as e:
        print(f"Error writing to JSON file for message (topic: {topic}, offset: {offset}): {e}")

if __name__ == "__main__":
    create_log_directory()
    print("Initializing Kafka Consumer...")
    consumer = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
        )
        print(f"Consumer listening on topic: {KAFKA_TOPIC}")
        print("-" * 30)

        for message in consumer:
            print(f"Received message from partition {message.partition}, offset {message.offset}:")
            log_message_to_json_file(
                message.value,
                message.topic,
                message.partition,
                message.offset,
                message.timestamp,
                message.key
            )
    except KeyboardInterrupt:
        print("\n--- Consumer stopped by user. ---")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed.")
