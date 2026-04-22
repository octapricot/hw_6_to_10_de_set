import json
import os
import sys
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

sys.stdout.reconfigure(line_buffering=True)

KAFKA_BROKER        = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_INPUT_TOPIC   = os.environ.get("KAFKA_INPUT_TOPIC", "input")
WIKIPEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/page-create"

# Kafka Producer

print(f"Connecting to Kafka broker at {KAFKA_BROKER} ...")

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

print(f"Connected. Streaming to topic '{KAFKA_INPUT_TOPIC}' ...")

# SSE Stream Reader

sent = 0

print(f"Connecting to Wikipedia stream at {WIKIPEDIA_STREAM_URL} ...")

HEADERS = {
    "User-Agent": "hw10-wikipedia-stream/1.0 (data-engineering-homework; contact@example.com)"
}

response = requests.get(
    WIKIPEDIA_STREAM_URL,
    stream=True,
    timeout=60,
    headers=HEADERS
)

print("Connected to Wikipedia stream! Sending events to Kafka now ...")

for line in response.iter_lines():
    if not line:
        continue

    line = line.decode("utf-8")

    if not line.startswith("data:"):
        continue

    json_str = line[len("data:"):].strip()

    try:
        event = json.loads(json_str)
    except json.JSONDecodeError:
        continue

    message = {
        "user_id":     str(event.get("performer", {}).get("user_id", "")),
        "user_is_bot": event.get("performer", {}).get("user_is_bot", False),
        "domain":      event.get("meta", {}).get("domain", ""),
        "page_title":  event.get("page_title", ""),
        "created_at":  datetime.now(timezone.utc).isoformat()
    }

    producer.produce(
        KAFKA_INPUT_TOPIC,
        value=json.dumps(message).encode("utf-8")
    )
    sent += 1

    if sent % 50 == 0:
        producer.flush()
        print(f"Sent {sent} events to Kafka, you should be catching them now ...", flush=True)