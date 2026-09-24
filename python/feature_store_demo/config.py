import os
from pathlib import Path


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


DATA_DIR = Path(env("DATA_DIR", "/data"))
KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FEAST_SERVER_URL = env("FEAST_SERVER_URL", "http://feast:6566")
FEAST_REPO_PATH = env("FEAST_REPO_PATH", "/workspace/feast")
RAW_TOPIC = "raw.transactions.v1"
FEATURE_TOPIC = "features.customer_30d.v1"
LATE_TOPIC = "transactions.late.v1"
INVALID_TOPIC = "transactions.invalid.v1"
PUSH_DLQ_TOPIC = "features.customer_30d.push-dlq.v1"
