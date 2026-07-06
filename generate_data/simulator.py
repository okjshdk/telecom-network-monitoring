import time
import json
import random
import uuid
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# ==========================
# Load environment variables
# ==========================
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")

SUBSCRIBER_COUNT = int(os.getenv("SUBSCRIBER_COUNT", 1000))
TOWER_COUNT = int(os.getenv("TOWER_COUNT", 50))

STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", 1))
ERROR_RATE = float(os.getenv("ERROR_RATE", 0.1))

# ==========================
# Pub/Sub Client
# ==========================
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ==========================
# Master Data
# ==========================

PROVINCES = [
    "Ha Noi",
    "Ho Chi Minh",
    "Da Nang",
    "Hai Phong",
    "Can Tho"
]

subscriber_ids = [
    f"SUB{i:06d}"
    for i in range(1, SUBSCRIBER_COUNT + 1)
]

towers = []

for i in range(1, TOWER_COUNT + 1):

    technology = random.choice(["4G", "5G"])

    towers.append({
        "tower_id": f"BTS{i:03d}",
        "province": random.choice(PROVINCES),
        "technology": technology
    })


# ==========================
# Generate Event
# ==========================

def generate_network_event():

    tower = random.choice(towers)

    signal = random.randint(-110, -60)

    technology = tower["technology"]

    if technology == "5G":

        if signal > -75:
            download = random.uniform(300, 900)
            upload = random.uniform(50, 150)
            latency = random.uniform(8, 15)
            packet_loss = random.uniform(0, 0.5)

        elif signal > -90:
            download = random.uniform(150, 400)
            upload = random.uniform(30, 80)
            latency = random.uniform(15, 30)
            packet_loss = random.uniform(0.5, 1)

        else:
            download = random.uniform(20, 150)
            upload = random.uniform(5, 30)
            latency = random.uniform(30, 80)
            packet_loss = random.uniform(1, 3)

    else:

        if signal > -75:
            download = random.uniform(80, 200)
            upload = random.uniform(20, 60)
            latency = random.uniform(20, 35)
            packet_loss = random.uniform(0.3, 1)

        elif signal > -90:
            download = random.uniform(30, 120)
            upload = random.uniform(10, 30)
            latency = random.uniform(35, 60)
            packet_loss = random.uniform(1, 2)

        else:
            download = random.uniform(5, 40)
            upload = random.uniform(1, 10)
            latency = random.uniform(60, 120)
            packet_loss = random.uniform(2, 5)

    record = {
        "event_id": str(uuid.uuid4()),
        "subscriber_id": random.choice(subscriber_ids),
        "tower_id": tower["tower_id"],
        "province": tower["province"],
        "technology": technology,
        "download_mb": round(download, 2),
        "upload_mb": round(upload, 2),
        "latency_ms": round(latency, 2),
        "packet_loss": round(packet_loss, 2),
        "signal_strength_dbm": signal,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    # ==========================
    # Inject Errors
    # ==========================
    if random.random() < ERROR_RATE:

        error_type = random.choice([
            "missing_field",
            "negative_value",
            "out_of_range"
        ])

        if error_type == "missing_field":
            field = random.choice(list(record.keys()))
            record[field] = None

        elif error_type == "negative_value":
            field = random.choice([
                "download_mb",
                "upload_mb",
                "latency_ms",
                "packet_loss"
            ])

            record[field] = -100

        elif error_type == "out_of_range":
            field = random.choice([
                "packet_loss",
                "signal_strength_dbm"
            ])

            if field == "packet_loss":
                record[field] = 120       
            else: 
                record[field] = -250 

    return record


print("Starting Network Usage Event Simulator... Press Ctrl+C to stop.")

while True:

    event = generate_network_event()

    message_json = json.dumps(event)

    publisher.publish(
        topic_path,
        message_json.encode("utf-8")
    )

    print(f"Published: {message_json}")

    time.sleep(STREAM_INTERVAL)