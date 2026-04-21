import csv
import json
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

sys.stdout.reconfigure(line_buffering=True)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC  = os.environ.get("KAFKA_TOPIC", "tweets")
OUTPUT_DIR   = os.environ.get("OUTPUT_DIR", "/output")

print(f"Connecting to Kafka broker at {KAFKA_BROKER} ...")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "tweets_consumer_group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([KAFKA_TOPIC])
print(f"Connected. Reading from topic '{KAFKA_TOPIC}'...")

def get_file_path(created_at_str: str) -> str:
    try:
        dt = datetime.fromisoformat(created_at_str)
    except ValueError:
        dt = datetime.now(timezone.utc)
    filename = dt.strftime("tweets_%d_%m_%Y_%H_%M.csv")
    return os.path.join(OUTPUT_DIR, filename)

def write_row(file_path: str, author_id: str, created_at: str, text: str):
    file_exists = os.path.exists(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["author_id", "created_at", "text"])
            print(f"New file created: {os.path.basename(file_path)}", flush=True)
        writer.writerow([author_id, created_at, text])

processed = 0

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f"Consumer error: {msg.error()}", flush=True)
        continue

    tweet = json.loads(msg.value().decode("utf-8"))

    author_id  = tweet.get("author_id", "")
    created_at = tweet.get("created_at", "")
    text       = tweet.get("text", "")

    file_path = get_file_path(created_at)
    write_row(file_path, author_id, created_at, text)

    processed += 1
    if processed % 100 == 0:
        print(f"Processed {processed} messages ...", flush=True)