# Confluent Kafka Avro Retail Data Pipeline

A Python project demonstrating real-time retail data streaming using Apache Kafka with Avro schema serialization and Confluent Cloud.

## Project Overview

This project contains:
- **Producer** (`confluent_avro_data_producer.py`): Reads retail data from CSV and publishes to Kafka
- **Consumers** (`confluent_avro_data_consumer.py`, `confluent_avro_data_consumer_2.py`): Subscribes to Kafka topic and displays messages
- **Avro Schema** (`reatil_data_avro_schema.json`): Defines retail transaction data structure
- **Sample Data** (`retail_data.csv`): Sample retail transaction dataset

## Architecture

```
┌──────────────────┐
│   retail_data    │
│      .csv        │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────────┐
│  confluent_avro_data_producer.py             │
│  • Reads CSV rows                            │
│  • Validates with Avro Schema                │
│  • Publishes to Kafka Topic                  │
└────────┬─────────────────────────────────────┘
         │
         │ Avro Serialized Messages
         ▼
┌──────────────────────────────────────────────────────────┐
│         Confluent Cloud - Kafka Cluster                  │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Topic: retail_data_topic  (Partitioned)          │ │
│  │  ┌──────────┬──────────┬──────────┬──────────────┐ │ │
│  │  │Partition │Partition │Partition │ Partition 0  │ │ │
│  │  │  0       │  1       │  2       │              │ │ │
│  │  └──────────┴──────────┴──────────┴──────────────┘ │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Schema Registry (Schema Validation)               │ │
│  │  └─ retail_data_topic-value: Purchase Record      │ │
│  └────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
         ▲
         │ Subscribe to Topic
    ┌────┴──────────────────────────────┬──────────────────┐
    │                                    │                  │
    ▼                                    ▼                  ▼
┌─────────────────────────┐  ┌──────────────────────┐  (Additional)
│ confluent_avro_data_    │  │ confluent_avro_data_ │  Consumer
│   consumer.py           │  │   consumer_2.py      │  Group
│ (Group: G1, Latest)     │  │ (Group: G2, Earliest)│
│ • Subscribes to topic   │  │ • Subscribes to topic│
│ • Consumes messages     │  │ • Consumes messages  │
│ • Deserializes Avro     │  │ • Deserializes Avro  │
│ • Displays records      │  │ • Displays records   │
└─────────────────────────┘  └──────────────────────┘
```

### Data Flow

1. **CSV Input**: Retail transaction data loaded from `retail_data.csv`
2. **Producer Processing**:
   - Validates each row against Avro schema
   - Serializes to Avro binary format
   - Publishes to Kafka topic with message key (index)
3. **Kafka Storage**:
   - Messages stored in distributed partitions
   - Schema registered in Schema Registry
4. **Consumer Processing**:
   - Consumer Group G1 reads latest messages
   - Consumer Group G2 reads from earliest offset
   - Deserializes Avro messages
   - Displays record content

## Setup Instructions

### 1. Prerequisites
- Python 3.8+
- A Confluent Cloud account with:
  - Kafka cluster
  - Schema Registry
  - API credentials

### 2. Install Dependencies
```bash
pip install confluent-kafka pandas python-dotenv
```

### 3. Configure Environment Variables

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Open `.env` and fill in your credentials from Confluent Cloud:
   - `KAFKA_BOOTSTRAP_SERVERS`: Your Kafka cluster bootstrap server
   - `KAFKA_USERNAME`: Your Kafka API Key
   - `KAFKA_PASSWORD`: Your Kafka API Secret
   - `SCHEMA_REGISTRY_URL`: Your Schema Registry URL
   - `SCHEMA_REGISTRY_USERNAME`: Your Schema Registry API Key
   - `SCHEMA_REGISTRY_PASSWORD`: Your Schema Registry API Secret

### 4. Running the Project

**Start the Producer** (publishes data to Kafka):
```bash
python confluent_avro_data_producer.py
```

**Start a Consumer** (in a separate terminal):
```bash
python confluent_avro_data_consumer.py
```

Or start the second consumer:
```bash
python confluent_avro_data_consumer_2.py
```

## Data Schema

The Avro schema defines the following fields for retail transactions:
- `Invoice`: Invoice number (string)
- `StockCode`: Product stock code (string)
- `Description`: Product description (string, nullable)
- `Quantity`: Quantity ordered (integer)
- `InvoiceDate`: Date of transaction (string)
- `Price`: Unit price (double)
- `CustomerID`: Customer identifier (integer)
- `Country`: Customer country (string)

## Key Features

✅ **Avro Serialization**: Binary, schema-based data format for efficient transmission
✅ **Schema Registry**: Centralized schema management and validation
✅ **Multiple Consumers**: Supports different consumer groups with different offsets
✅ **Error Handling**: Built-in delivery reports and error handling
✅ **SASL/SSL Security**: Secure authentication with Confluent Cloud

## Security Notes

⚠️ **Never commit credentials to version control**
- All sensitive data should be stored in `.env` file
- `.env` is automatically ignored by `.gitignore`
- Use environment variables for all sensitive configuration

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Authentication error | Verify your API credentials in `.env` file |
| Topic not found | Ensure topic `retail_data_topic` exists in your Kafka cluster |
| Schema not found | Register the schema with Schema Registry first |
| No messages received | Check consumer group offset - use `earliest` or `latest` in `auto.offset.reset` |

## Further Reading

- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Avro Format Documentation](https://avro.apache.org/)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
