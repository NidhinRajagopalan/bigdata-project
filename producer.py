from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Kafka server
TOPIC = 'realtime_test'     # Kafka topic

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert data to JSON
)

# Load CSV Data
csv_file = "realtime_test.csv"
df = pd.read_csv(csv_file)

# Send Each Transaction to Kafka
for _, row in df.iterrows():
    transaction = row.to_dict()  # Convert row to dictionary
    producer.send(TOPIC, transaction)
    print(f"Sent: {transaction}")
    time.sleep(1)  # Simulate real-time streaming

# Close the Producer
producer.flush()
producer.close()
