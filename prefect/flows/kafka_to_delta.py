import json
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer
from prefect import flow, task


@task
def consume_and_process():
    """Consume data from Kafka topic."""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    records = [msg.value for msg in consumer]
    print(f"Consumed {len(records)} records from Kafka")
    return records


@task
def save_to_delta(records):
    """Save records to Delta Lake style parquet files."""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    path = "/opt/delta-lake/raw"
    os.makedirs(path, exist_ok=True)
    file_name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(f"{path}/{file_name}", index=False)
    print(f"Saved {len(df)} records to Delta Lake")


@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake."""
    records = consume_and_process()
    save_to_delta(records)


if __name__ == "__main__":
    kafka_to_delta_flow()
