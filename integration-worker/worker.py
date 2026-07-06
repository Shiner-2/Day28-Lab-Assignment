import hashlib
import json
import os
import time
from datetime import datetime

import pandas as pd
import redis
from kafka import KafkaConsumer
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import Distance, PointStruct, VectorParams

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
DELTA_PATH = os.environ.get("DELTA_PATH", "/opt/delta-lake/raw")
COLLECTION_NAME = "documents"
VECTOR_SIZE = 384


def deterministic_embedding(text: str) -> list[float]:
    values = []
    for idx in range(VECTOR_SIZE):
        digest = hashlib.sha256(f"{text}:{idx}".encode()).digest()
        value = int.from_bytes(digest[:4], "big") / 4294967295
        values.append((value * 2) - 1)
    return values


def ensure_collection(client: QdrantClient) -> None:
    try:
        client.get_collection(COLLECTION_NAME)
    except UnexpectedResponse:
        client.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )


def wait_for_dependencies() -> tuple[redis.Redis, QdrantClient]:
    while True:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
            )
            redis_client.ping()
            qdrant_client = QdrantClient(url=QDRANT_URL)
            ensure_collection(qdrant_client)
            return redis_client, qdrant_client
        except Exception as exc:
            print(f"Waiting for dependencies: {exc}")
            time.sleep(5)


def persist_delta(records: list[dict]) -> None:
    if not records:
        return
    os.makedirs(DELTA_PATH, exist_ok=True)
    frame = pd.DataFrame(records)
    path = os.path.join(DELTA_PATH, f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet")
    frame.to_parquet(path, index=False)
    print(f"Saved {len(records)} records to {path}")


def persist_features(redis_client: redis.Redis, records: list[dict]) -> None:
    for record in records:
        redis_client.set(
            f"feature:{record['id']}",
            json.dumps(
                {
                    "text": record["text"],
                    "timestamp": record.get("timestamp", time.time()),
                    "processed": True,
                }
            ),
        )


def persist_vectors(qdrant_client: QdrantClient, records: list[dict]) -> None:
    points = []
    for index, record in enumerate(records):
        point_id = int(hashlib.sha256(record["id"].encode()).hexdigest()[:12], 16)
        points.append(
            PointStruct(
                id=point_id,
                vector=deterministic_embedding(record["text"]),
                payload=record,
            )
        )
    qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)


def seed_sample_data(redis_client: redis.Redis, qdrant_client: QdrantClient) -> None:
    sample_records = [
        {
            "id": "seed_001",
            "text": "Platform engineering enables self-service developer workflows.",
            "timestamp": time.time(),
        },
        {
            "id": "seed_002",
            "text": "Observability combines logs, metrics, and traces for AI systems.",
            "timestamp": time.time(),
        },
    ]
    persist_delta(sample_records)
    persist_features(redis_client, sample_records)
    persist_vectors(qdrant_client, sample_records)
    print("Seeded sample records into Delta Lake, Redis, and Qdrant")


def main() -> None:
    redis_client, qdrant_client = wait_for_dependencies()
    seed_sample_data(redis_client, qdrant_client)

    while True:
        try:
            consumer = KafkaConsumer(
                "data.raw",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="lab28-integration-worker",
                consumer_timeout_ms=3000,
                value_deserializer=lambda value: json.loads(value.decode()),
            )

            while True:
                batch = [message.value for message in consumer]
                if not batch:
                    break
                for record in batch:
                    record.setdefault("timestamp", time.time())
                persist_delta(batch)
                persist_features(redis_client, batch)
                persist_vectors(qdrant_client, batch)
                print(f"Processed batch of {len(batch)} Kafka records")
        except Exception as exc:
            print(f"Worker loop failed: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
