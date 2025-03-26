import pandas as pd
from pymongo import MongoClient

# MongoDB Connection Details
MONGO_URI = "mongodb://localhost:27017"  # Update if using a different host/port
DB_NAME = "fraud_detection"
COLLECTION_NAME = "transactions"

# CSV File Path
CSV_FILE_PATH = "fraudTrain.csv"  # Update with your actual file path

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Load CSV in chunks (useful for large datasets)
chunk_size = 10000  # Adjust based on system memory
for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=chunk_size):
    records = chunk.to_dict(orient="records")  # Convert DataFrame to list of dictionaries
    collection.insert_many(records)  # Bulk insert

print(f"✅ Successfully loaded data into MongoDB ({DB_NAME}.{COLLECTION_NAME})")

# Verify count of inserted documents
print(f"Total documents in collection: {collection.count_documents({})}")
