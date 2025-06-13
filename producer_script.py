import time
import random
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer # Import KafkaProducer from kafka-python

# --- Kafka configuration ---
KAFKA_BROKER = "localhost:9092" # Address of your Kafka broker (from docker-composer.yml)
KAFKA_TOPIC = "purchase_events" # The topic you created in Kafka

def generate_random_data():
    """Generates a single row of randomized purchase data."""
    names = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi",
        "Ivy", "Jack", "Karen", "Liam", "Mia", "Noah", "Olivia", "Peter",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
        "Yara", "Zoe"
    ]
    genders = ["Male", "Female", "Non-binary"]
    locations = [
        "New York", "London", "Paris", "Tokyo", "Berlin", "Sydney", "Mumbai", "Cairo",
        "Rio de Janeiro", "Rome", "Beijing", "Dubai", "Moscow", "Seoul", "Toronto",
        "Mexico City", "Buenos Aires", "Cape Town", "Nairobi", "Stockholm", "Amsterdam",
        "Madrid", "Bangkok", "Istanbul"
    ]

    name = random.choice(names)
    gender = random.choice(genders)
    location = random.choice(locations)
    purchase_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    price = round(random.uniform(10.00, 1000.00), 2)

    return {
        "name": name,
        "gender": gender,
        "location": location,
        "purchase_date": purchase_date,
        "price": price
    }

if __name__ == "__main__":
    print("Initializing Kafka Producer...")
    producer = None
    try:
        # Configure the Kafka Producer
        # bootstrap_servers: A list of host:port pairs for the Kafka brokers.
        # value_serializer: A function to convert Python objects (dictionaries in this case)
        #                   into byte arrays suitable for Kafka. We use JSON serialization.
        # key_serializer: A function to convert the message key to bytes. Using location as key
        #                 helps ensure messages for the same location go to the same partition.
        # acks: Controls the durability level. 'all' means the leader waits for all in-sync
        #       replicas to acknowledge the write, ensuring strong durability.
        # retries: Number of times to retry sending a message if there's a temporary failure.
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            acks='all',
            retries=5
        )

        print(f"Producer connected to {KAFKA_BROKER}. Sending data to topic: {KAFKA_TOPIC}")
        print("-" * 30)

        while True:
            data_row = generate_random_data()

            # Send the message to Kafka.
            # 'topic': The Kafka topic to send the message to.
            # 'value': The data payload of the message (will be serialized by value_serializer).
            # 'key': The message key (will be serialized by key_serializer). Using a key
            #        can help with ordering guarantees within partitions and data locality.
            future = producer.send(
                topic=KAFKA_TOPIC,
                value=data_row,
                key=data_row["location"]
            )

            # --- Optional: Blocking for message delivery confirmation ---
            # Uncomment the following block if you need to ensure each message is
            # delivered before proceeding, or to handle delivery errors immediately.
            # Be aware that this can significantly reduce throughput.
            # try:
            #     record_metadata = future.get(timeout=10) # Waits up to 10 seconds for delivery
            #     print(f"Message delivered to topic {record_metadata.topic}, "
            #           f"partition {record_metadata.partition}, offset {record_metadata.offset}")
            # except Exception as e:
            #     print(f"Message delivery failed for {data_row}: {e}")

            print(f"Produced: {data_row}")
            time.sleep(1) # Pause for 1 second before generating and sending the next message

    except KeyboardInterrupt:
        # This block is executed when the user presses Ctrl+C
        print("\n--- Data generation and production stopped. ---")
    except Exception as e:
        # Catch any other unexpected errors during the process
        print(f"An unexpected error occurred: {e}")
    finally:
        if producer:
            # 'flush()' blocks until all outstanding messages (those not yet delivered)
            # are sent or the timeout is reached. This is important to ensure no
            # messages are lost when the script exits.
            print("Flushing outstanding messages...")
            producer.flush()
            print("Producer shut down.")