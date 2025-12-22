# -----------------------
# Imports
# -----------------------
import json
import boto3
import time
import uuid
from kafka import KafkaConsumer

# -----------------------
# MinIO Connection
# -----------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)

bucket_name = "bronze-transactions"

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")

# -----------------------
# Kafka Consumer
# -----------------------
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer streaming and saving to MinIO...")

# -----------------------
# Main Loop
# -----------------------
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")

    # Use UUID to avoid overwriting if multiple messages arrive at the same second
    unique_id = uuid.uuid4().hex
    key = f"{symbol}/{unique_id}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )

    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")
