from kafka import KafkaConsumer
import json
import pymongo

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
collection = db["live_transactions"]

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'realtime_test',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # Ensures we read from the start if needed
    group_id="consumer_group_1"
)

print("Consumer started and waiting for messages...")
message_count = 0

for message in consumer:
    print(f"📥 Received message: {message.value}")  
    collection.insert_one(message.value)  # Insert into MongoDB
    message_count += 1  # Increase count
    print(f"✅ Data inserted. Total messages processed: {message_count}")

    # Stop after consuming all available messages
    if consumer.assignment():  
        end_offsets = consumer.end_offsets(consumer.assignment())  # Get last offset
        if all(consumer.position(tp) >= end_offsets[tp] for tp in consumer.assignment()):
            print(f"✅ All {message_count} messages processed. Exiting...")
            break

consumer.close()  # Close the consumer
print("Consumer stopped.")
