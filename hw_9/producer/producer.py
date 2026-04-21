import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

sys.stdout.reconfigure(line_buffering=True)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC  = os.environ.get("KAFKA_TOPIC", "tweets")
CSV_PATH     = os.environ.get("CSV_PATH", "/data/twcs.csv")
MIN_RATE     = float(os.environ.get("MIN_RATE", "10"))
MAX_RATE     = float(os.environ.get("MAX_RATE", "15"))

print(f"Connecting to Kafka broker at {KAFKA_BROKER} ...")

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

print(f"Connected. Streaming to topic '{KAFKA_TOPIC}' ...")

sent = 0

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        row["created_at"] = datetime.now(timezone.utc).isoformat()

        producer.produce(KAFKA_TOPIC, value=json.dumps(row).encode("utf-8"))
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            print(f"Sent {sent} messages...", flush=True)

        delay = 1.0 / random.uniform(MIN_RATE, MAX_RATE)
        time.sleep(delay)

producer.flush()
print(f"Done! Total messages sent: {sent}", flush=True)