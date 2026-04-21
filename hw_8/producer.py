import csv
import json
import os
import random
import time
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer

sys.stdout.reconfigure(line_buffering=True)

# Configuration

KAFKA_BROKER  = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC   = os.environ.get("KAFKA_TOPIC", "tweets")
CSV_PATH      = os.environ.get("CSV_PATH", "/data/twcs.csv")
MIN_RATE      = float(os.environ.get("MIN_RATE", "10"))
MAX_RATE      = float(os.environ.get("MAX_RATE", "15"))

# Kafka Producer

print(f"Connecting to Kafka broker at {KAFKA_BROKER} ...", flush=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

print(f"Connected. Streaming to topic '{KAFKA_TOPIC}' ...", flush=True)

# Stream Tweets (read the CSV row by row and send each tweet as a message to simulate a live stream)

sent = 0

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        row["created_at"] = datetime.now(timezone.utc).isoformat()

        producer.send(KAFKA_TOPIC, value=row)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            print(f"Sent {sent} messages...", flush=True)

        delay = 1.0 / random.uniform(MIN_RATE, MAX_RATE)
        time.sleep(delay)

producer.flush()
print(f"Done! Total messages sent: {sent}", flush=True)