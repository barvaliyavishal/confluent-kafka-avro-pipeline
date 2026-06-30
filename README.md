# Retail Kafka Avro Pipeline (Confluent Cloud)

Production-style data engineering project that streams retail transactions from CSV to Confluent Kafka with Avro serialization and Schema Registry.

## Why This Project Is Portfolio-Worthy

- Uses schema-first event design (Avro + Schema Registry)
- Uses secure Confluent Cloud setup with SASL/SSL
- Implements reusable app modules instead of one-off scripts
- Adds configurable CLI for producer and consumer workflows
- Adds reliability-focused producer settings (`acks=all`, idempotence)
- Adds tests for transformation logic
- Preserves backward-compatible entry scripts for easy demoing

## Architecture

```text
retail_data.csv
  -> Producer (retail_kafka_pipeline.producer_app)
  -> Avro serialization + schema validation
  -> Kafka topic: retail_data_topic
  -> Consumer(s) (retail_kafka_pipeline.consumer_app)
  -> JSON event output (ready for sink extensions)
```

## Project Structure

```text
.
|-- confluent_avro_data_producer.py
|-- confluent_avro_data_consumer.py
|-- confluent_avro_data_consumer_2.py
|-- pyproject.toml
|-- requirements.txt
|-- requirements-dev.txt
|-- retail_data.csv
|-- reatil_data_avro_schema.json
|-- src/
|   `-- retail_kafka_pipeline/
|       |-- __init__.py
|       |-- config.py
|       |-- producer_app.py
|       |-- consumer_app.py
|       `-- records.py
`-- tests/
    `-- test_records.py
```

## Setup

### 1. Prerequisites

- Python 3.10+
- Confluent Cloud Kafka cluster
- Confluent Schema Registry
- Kafka and Schema Registry API credentials

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

For development:

```bash
pip install -r requirements-dev.txt
pip install -e .
```

### 3. Configure environment

Copy `.env.example` to `.env` and fill these values:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`
- `SCHEMA_REGISTRY_URL`
- `SCHEMA_REGISTRY_USERNAME`
- `SCHEMA_REGISTRY_PASSWORD`

## Running the Pipeline

### Producer

Legacy wrapper:

```bash
python confluent_avro_data_producer.py --csv-path retail_data.csv --topic retail_data_topic
```

Package module:

```bash
python -m retail_kafka_pipeline.producer_app --csv-path retail_data.csv --topic retail_data_topic
```

### Consumers

Group `G1` from latest offset:

```bash
python confluent_avro_data_consumer.py --topic retail_data_topic --group-id G1 --offset-reset latest
```

Group `G2` from earliest offset:

```bash
python confluent_avro_data_consumer_2.py --topic retail_data_topic
```

## CLI Reference

### Producer options

- `--csv-path`: path to input CSV
- `--topic`: Kafka topic name
- `--schema-subject`: schema subject in registry
- `--message-delay-ms`: optional delay between records
- `--limit`: max number of records to publish
- `--log-level`: DEBUG, INFO, WARNING, ERROR

### Consumer options

- `--topic`: Kafka topic name
- `--group-id`: consumer group id
- `--offset-reset`: earliest or latest
- `--schema-subject`: schema subject in registry
- `--poll-timeout`: poll timeout in seconds
- `--max-messages`: stop after N messages
- `--log-level`: DEBUG, INFO, WARNING, ERROR

## Test

```bash
pytest
```

## Interview Talking Points

- Tradeoffs between throughput and ordering in Kafka producer tuning
- Why schema evolution discipline matters in event-driven systems
- Difference in behavior of consumer groups and offsets (`latest` vs `earliest`)
- How to extend this to medallion/lakehouse ingestion with connectors and dbt

## High-Value Next Steps

1. Add dead-letter topic and retry policy for malformed events
2. Add metrics/tracing (OpenTelemetry + Prometheus)
3. Add Docker Compose profile for local Kafka integration tests
4. Add CI (lint, tests, build, security checks)
