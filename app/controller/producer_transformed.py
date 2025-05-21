import os
import json
import pandas as pd
import math
from app.utils.kafka_helper import get_kafka_producer
from app.service.cleaner import clean_data
from app.service.transformer import transform_data
from app.utils.deduplicator import is_duplicate
from app.config.config import settings

TOPIC = settings.transformed_topic
RAW_FILE_PATH = os.path.join(settings.raw_data_path, "patients.csv")
TRANSFORMED_SAVE_PATH = settings.transformed_data_path
seen_pids = set()

DROP_COLS = {"GENDER", "YOB", "home", "RACE", "initial", "region"}

def sanitize_nan_values(data: dict) -> dict:
    return {
        k: (None if pd.isna(v) or (isinstance(v, float) and math.isnan(v)) else v)
        for k, v in data.items()
    }

async def produce_transformed_data():
    df = pd.read_csv(RAW_FILE_PATH)

    # Drop unwanted columns
    df.drop(columns=[col for col in DROP_COLS if col in df.columns], inplace=True)

    # Drop rows with less than 50% non-null values
    threshold = df.shape[1] / 2
    df.dropna(thresh=threshold, inplace=True)

    transformed_records = []

    # Clean and transform
    for _, row in df.iterrows():
        cleaned = clean_data(row.to_dict())
        transformed = transform_data(cleaned)
        transformed = sanitize_nan_values(transformed)

        pid = transformed.get("pid")
        if pid and not is_duplicate(pid, seen_pids):
            transformed_records.append(transformed)

    # Save to CSV
    os.makedirs(TRANSFORMED_SAVE_PATH, exist_ok=True)
    csv_path = os.path.join(TRANSFORMED_SAVE_PATH, "final_transformed.csv")
    pd.DataFrame(transformed_records).to_csv(csv_path, index=False)
    print(f"Saved transformed data to {csv_path}")

    # Produce to Kafka
    producer = await get_kafka_producer()
    try:
        for record in transformed_records:
            await producer.send_and_wait(TOPIC, json.dumps(record).encode("utf-8"))
            print(f"🚀 Produced to Kafka: pid={record.get('pid')}")
    finally:
        await producer.stop()
