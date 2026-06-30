import argparse
import csv
import logging
import time
from pathlib import Path
from typing import Dict, Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

from retail_kafka_pipeline.config import PipelineSettings
from retail_kafka_pipeline.records import normalize_record

LOGGER = logging.getLogger("retail_kafka_pipeline.producer")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish retail CSV records to Confluent Kafka using Avro")
    parser.add_argument("--csv-path", default="retail_data.csv", help="Path to retail CSV file")
    parser.add_argument("--topic", default="retail_data_topic", help="Kafka topic name")
    parser.add_argument("--schema-subject", default="retail_data_topic-value", help="Schema Registry subject")
    parser.add_argument("--message-delay-ms", type=int, default=0, help="Delay between messages")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of records to publish")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    return parser


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _load_schema(schema_client: SchemaRegistryClient, subject: str) -> str:
    schema = schema_client.get_latest_version(subject)
    return schema.schema.schema_str


def _create_producer(settings: PipelineSettings, schema_str: str) -> SerializingProducer:
    key_serializer = StringSerializer("utf_8")
    schema_client = SchemaRegistryClient(settings.schema_registry_config())
    avro_serializer = AvroSerializer(schema_client, schema_str)
    producer_config: Dict[str, object] = settings.kafka_config()
    producer_config.update(
        {
            "key.serializer": key_serializer,
            "value.serializer": avro_serializer,
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    return SerializingProducer(producer_config)


def run(csv_path: Path, topic: str, schema_subject: str, message_delay_ms: int, limit: Optional[int]) -> int:
    settings = PipelineSettings.from_env()
    schema_client = SchemaRegistryClient(settings.schema_registry_config())
    schema_str = _load_schema(schema_client, schema_subject)
    producer = _create_producer(settings, schema_str)

    delivered = 0
    failed = 0

    def on_delivery(err, msg) -> None:
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            LOGGER.error("Delivery failed for key=%s: %s", msg.key(), err)
            return
        delivered += 1

    LOGGER.info("Publishing records from %s to topic=%s", csv_path, topic)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break

            value = normalize_record(row)
            producer.produce(topic=topic, key=str(index), value=value, on_delivery=on_delivery)
            producer.poll(0)

            if message_delay_ms > 0:
                time.sleep(message_delay_ms / 1000)

            if (index + 1) % 500 == 0:
                LOGGER.info("Queued %s messages", index + 1)

    producer.flush()
    LOGGER.info("Publishing complete. delivered=%s failed=%s", delivered, failed)
    return 1 if failed else 0


def main() -> int:
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        LOGGER.error("CSV file not found: %s", csv_path)
        return 1

    try:
        return run(
            csv_path=csv_path,
            topic=args.topic,
            schema_subject=args.schema_subject,
            message_delay_ms=args.message_delay_ms,
            limit=args.limit,
        )
    except KeyboardInterrupt:
        LOGGER.warning("Producer interrupted by user")
        return 1
    except Exception as exc:
        LOGGER.exception("Producer failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
