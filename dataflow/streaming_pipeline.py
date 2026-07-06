import os
import json
from dotenv import load_dotenv
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


# ==========================
# Load environment variables
# ==========================

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
GOLD_NETWORK_EVENTS_TABLE = os.getenv("GOLD_NETWORK_EVENTS_TABLE")
GOLD_TOWER_KPI_TABLE = os.getenv("GOLD_TOWER_KPI_TABLE")
GOLD_TIME_KPI_TABLE = os.getenv("GOLD_TIME_KPI_TABLE")
TEMP_LOCATION = os.getenv("TEMP_LOCATION")
STAGING_LOCATION = os.getenv("STAGING_LOCATION")
REGION = os.getenv("REGION")

# ==========================
# Parse data
# ==========================

def parse_json(message):
    try:
        return json.loads(message)
    except:
        return None


# ==========================
# Validate data
# ==========================

def validate_data(record):
    try:
        if record is None:
            return False
        
        event_id = record.get("event_id")
        subscriber_id = record.get("subscriber_id")
        tower_id = record.get("tower_id")
        province = record.get("province")
        technology = record.get("technology")
        download_mb = record.get("download_mb")
        upload_mb = record.get("upload_mb")
        latency_ms = record.get("latency_ms")
        packet_loss = record.get("packet_loss")
        signal_strength_dbm = record.get("signal_strength_dbm")
        timestamp = record.get("timestamp")

        # Missing values
        required_fields = [
            event_id,
            subscriber_id,
            tower_id,
            province,
            technology,
            download_mb,
            upload_mb,
            latency_ms,
            packet_loss,
            signal_strength_dbm,
            timestamp,
        ]

        if any(field is None for field in required_fields):
            return False

        # Negative values
        if (download_mb < 0 or upload_mb < 0 or latency_ms < 0 or packet_loss < 0):
            return False

        # Outlier values
        if not (0 <= packet_loss <= 100):
            return False

        if not (-150 <= signal_strength_dbm <= -30):
            return False
        
        return True
    except:
        return False


# ==========================
# Enrich data
# ==========================

def enrich_data(record):
    if record is None:
        return None

    download = record.get("download_mb")
    upload = record.get("upload_mb")
    latency = record.get("latency_ms")
    packet_loss = record.get("packet_loss")
    signal = record.get("signal_strength_dbm")
    timestamp = record.get("timestamp")

    # 1. Signal level
    if signal >= -75:
        record["signal_level"] = "Excellent"
    elif signal >= -90:
        record["signal_level"] = "Good"
    else:
        record["signal_level"] = "Poor"

    # 2. Latency category
    if latency < 20:
        record["latency_category"] = "Low"
    elif latency < 50:
        record["latency_category"] = "Medium"
    else:
        record["latency_category"] = "High"

    # 3. Packet loss category
    if packet_loss < 0.5:
        record["packet_loss_category"] = "Low"
    elif packet_loss < 2:
        record["packet_loss_category"] = "Medium"
    else:
        record["packet_loss_category"] = "High"

    # 4. Network quality
    if download >= 300 and latency <= 15 and packet_loss <= 0.5:
        record["network_quality"] = "Excellent"
    elif download >= 100 and latency <= 40 and packet_loss <= 1:
        record["network_quality"] = "Good"
    else:
        record["network_quality"] = "Poor"

    # 5. Throughput ratio
    record["throughput_ratio"] = (
        round(download / upload, 2) if upload and upload > 0 else None
    )

    # 6. Time features
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    record["hour"] = dt.hour
    record["day_of_week"] = dt.strftime("%A")
    record["is_peak_hour"] = 18 <= dt.hour <= 22

    # 7. Bandwidth total
    record["total_throughput"] = download + upload

    # 8. Upload ratio
    record["upload_ratio"] = round(upload / download, 4) if download > 0 else None

    return record

# ==========================
# Gold Schemas
# ==========================


GOLD_NETWORK_EVENTS_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING"},
        {"name": "subscriber_id", "type": "STRING"},
        {"name": "tower_id", "type": "STRING"},
        {"name": "province", "type": "STRING"},
        {"name": "technology", "type": "STRING"},

        {"name": "download_mb", "type": "FLOAT"},
        {"name": "upload_mb", "type": "FLOAT"},
        {"name": "latency_ms", "type": "FLOAT"},
        {"name": "packet_loss", "type": "FLOAT"},
        {"name": "signal_strength_dbm", "type": "FLOAT"},

        {"name": "signal_level", "type": "STRING"},
        {"name": "latency_category", "type": "STRING"},
        {"name": "packet_loss_category", "type": "STRING"},
        {"name": "network_quality", "type": "STRING"},

        {"name": "throughput_ratio", "type": "FLOAT"},
        {"name": "total_throughput", "type": "FLOAT"},
        {"name": "upload_ratio", "type": "FLOAT"},

        {"name": "hour", "type": "INTEGER"},
        {"name": "day_of_week", "type": "STRING"},
        {"name": "is_peak_hour", "type": "BOOLEAN"},

        {"name": "timestamp", "type": "TIMESTAMP"}
    ]
}

GOLD_TOWER_KPI_SCHEMA = {
    "fields": [
        {"name": "tower_id", "type": "STRING"},

        {"name": "event_count", "type": "INTEGER"},

        {"name": "avg_download_mb", "type": "FLOAT"},
        {"name": "avg_upload_mb", "type": "FLOAT"},
        {"name": "avg_latency_ms", "type": "FLOAT"},
        {"name": "avg_packet_loss", "type": "FLOAT"},
        {"name": "avg_signal_strength_dbm", "type": "FLOAT"},
        {"name": "avg_total_throughput", "type": "FLOAT"}
    ]
}

GOLD_TIME_KPI_SCHEMA = {
    "fields": [
        {"name": "hour", "type": "INTEGER"},
        {"name": "day_of_week", "type": "STRING"},
        {"name": "is_peak_hour", "type": "BOOLEAN"},

        {"name": "event_count", "type": "INTEGER"},

        {"name": "avg_download_mb", "type": "FLOAT"},
        {"name": "avg_upload_mb", "type": "FLOAT"},
        {"name": "avg_latency_ms", "type": "FLOAT"},
        {"name": "avg_packet_loss", "type": "FLOAT"},
        {"name": "avg_signal_strength_dbm", "type": "FLOAT"},
        {"name": "avg_total_throughput", "type": "FLOAT"}
    ]
}

# ==========================
# Pipeline Options
# ==========================

pipeline_options = PipelineOptions(
    project=PROJECT_ID,
    region=REGION,
    staging_location=STAGING_LOCATION,
    temp_location=TEMP_LOCATION,
    streaming=True,
    save_main_session=True
)


with beam.Pipeline(options=pipeline_options) as p:

    # Bronze 
    bronze_data = (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | "Decode to string" >> beam.Map(lambda x: x.decode("utf-8"))
        | "Window Bronze Data" >> beam.WindowInto(FixedWindows(60)) 
    )

    bronze_data | "Write Bronze to GCS" >> beam.io.WriteToText(
        BRONZE_PATH + "raw_data",
        file_name_suffix=".json"
    )

    # ------------------- Silver Layer -------------------
    silver_data = (
        bronze_data
        | "Parse JSON" >> beam.Map(parse_json)
        | "Filter Invalid Records" >> beam.Filter(validate_data)
        | "Enrich with Risk Score" >> beam.Map(enrich_data)
        | "Window Silver Data" >> beam.WindowInto(FixedWindows(60))
    )

    # Write Silver layer to GCS
    silver_data | "Write Silver to GCS" >> beam.io.WriteToText(
        SILVER_PATH + "cleaned_data",
        file_name_suffix=".json"
    )

    # ------------------- Gold Layer -------------------

    # GOLD 1: Network Events (event-level)
    gold_network_events = (
        silver_data
        | "Gold Event Window" >> beam.WindowInto(FixedWindows(60))
    )

    gold_network_events | "Write Gold Events" >> beam.io.WriteToBigQuery(
        table=GOLD_NETWORK_EVENTS_TABLE,
        schema=GOLD_NETWORK_EVENTS_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )


    # ------------------- GOLD 2 : Tower KPI -------------------

    def tower_to_kv(record):
        return (record["tower_id"], record)


    def aggregate_tower(kv):
        tower_id, records = kv
        records = list(records)

        count = len(records)

        return {
            "tower_id": tower_id,
            "event_count": count,
            "avg_download_mb": round(
                sum(r["download_mb"] for r in records) / count, 2
            ),
            "avg_upload_mb": round(
                sum(r["upload_mb"] for r in records) / count, 2
            ),
            "avg_latency_ms": round(
                sum(r["latency_ms"] for r in records) / count, 2
            ),
            "avg_packet_loss": round(
                sum(r["packet_loss"] for r in records) / count, 2
            ),
            "avg_signal_strength_dbm": round(
                sum(r["signal_strength_dbm"] for r in records) / count, 2
            ),
            "avg_total_throughput": round(
                sum(r["total_throughput"] for r in records) / count, 2
            )
        }


    gold_tower_kpi = (
        silver_data
        | "Tower Window" >> beam.WindowInto(FixedWindows(60))
        | "Tower KV" >> beam.Map(tower_to_kv)
        | "Group Tower" >> beam.GroupByKey()
        | "Aggregate Tower KPI" >> beam.Map(aggregate_tower)
    )

    gold_tower_kpi | "Write Gold Tower KPI" >> beam.io.WriteToBigQuery(
        table=GOLD_TOWER_KPI_TABLE,
        schema=GOLD_TOWER_KPI_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )


    # ------------------- GOLD 3 : Time KPI -------------------

    def time_to_kv(record):
        return (
            (
                record["hour"],
                record["day_of_week"],
                record["is_peak_hour"]
            ),
            record
        )


    def aggregate_time(kv):
        (hour, day_of_week, is_peak_hour), records = kv
        records = list(records)

        count = len(records)

        return {
            "hour": hour,
            "day_of_week": day_of_week,
            "is_peak_hour": is_peak_hour,

            "event_count": count,

            "avg_download_mb": round(
                sum(r["download_mb"] for r in records) / count, 2
            ),
            "avg_upload_mb": round(
                sum(r["upload_mb"] for r in records) / count, 2
            ),
            "avg_latency_ms": round(
                sum(r["latency_ms"] for r in records) / count, 2
            ),
            "avg_packet_loss": round(
                sum(r["packet_loss"] for r in records) / count, 2
            ),
            "avg_signal_strength_dbm": round(
                sum(r["signal_strength_dbm"] for r in records) / count, 2
            ),
            "avg_total_throughput": round(
                sum(r["total_throughput"] for r in records) / count, 2
            )
        }


    gold_time_kpi = (
        silver_data
        | "Time Window" >> beam.WindowInto(FixedWindows(60))
        | "Time KV" >> beam.Map(time_to_kv)
        | "Group Time" >> beam.GroupByKey()
        | "Aggregate Time KPI" >> beam.Map(aggregate_time)
    )

    gold_time_kpi | "Write Gold Time KPI" >> beam.io.WriteToBigQuery(
        table=GOLD_TIME_KPI_TABLE,
        schema=GOLD_TIME_KPI_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )