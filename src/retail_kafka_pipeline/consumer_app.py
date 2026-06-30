import argparse
import json
import logging
from typing import Dict, Optional

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv

from retail_kafka_pipeline.config import PipelineSettings

LOGGER = logging.getLogger("retail_kafka_pipeline.consumer")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Avro messages from Confluent Kafka")
    parser.add_argument("--topic", default="retail_data_topic", help="Kafka topic name")
    parser.add_argument("--group-id", default="G1", help="Consumer group id")
    parser.add_argument("--offset-reset", default="latest", choices=["earliest", "latest"], help="Offset reset policy")
    parser.add_argument("--schema-subject", default="retail_data_topic-value", help="Schema Registry subject")
    parser.add_argument("--poll-timeout", type=float, default=1.0, help="Poll timeout in seconds")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after consuming N messages")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    return parser


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_consumer(settings: PipelineSettings, schema_subject: str, group_id: str, offset_reset: str) -> DeserializingConsumer:
    schema_client = SchemaRegistryClient(settings.schema_registry_config())
    schema_str = schema_client.get_latest_version(schema_subject).schema.schema_str

    config: Dict[str, object] = settings.kafka_config()
    config.update(
        {
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": AvroDeserializer(schema_client, schema_str),
        }
    )

    return DeserializingConsumer(config)


def run(
    topic: str,
    group_id: str,
    offset_reset: str,
    schema_subject: str,
    poll_timeout: float,
    max_messages: Optional[int],
) -> int:
    settings = PipelineSettings.from_env()
    consumer = _build_consumer(settings, schema_subject=schema_subject, group_id=group_id, offset_reset=offset_reset)
    consumer.subscribe([topic])
    LOGGER.info("Subscribed to topic=%s group=%s offset_reset=%s", topic, group_id, offset_reset)

    consumed = 0
    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                LOGGER.error("Consumer error: %s", msg.error())
                continue

            consumed += 1
            payload = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key(),
                "value": msg.value(),
            }
            print(json.dumps(payload, ensure_ascii=True))

            if max_messages is not None and consumed >= max_messages:
                LOGGER.info("Reached max_messages=%s", max_messages)
                break
    except KeyboardInterrupt:
        LOGGER.warning("Consumer interrupted by user")
    finally:
        consumer.close()

    LOGGER.info("Consumer shutdown complete. consumed=%s", consumed)
    return 0


def main() -> int:
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)

    try:
        return run(
            topic=args.topic,
            group_id=args.group_id,
            offset_reset=args.offset_reset,
            schema_subject=args.schema_subject,
            poll_timeout=args.poll_timeout,
            max_messages=args.max_messages,
        )
    except Exception as exc:
        LOGGER.exception("Consumer failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
