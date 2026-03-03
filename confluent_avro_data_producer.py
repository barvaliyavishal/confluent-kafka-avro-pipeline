# Make sure to install confluent-kafka python package
# pip install confluent-kafka
# pip install pandas
# pip install python-dotenv

from decimal import *
from uuid import UUID 
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

# Define Kafka configuration
# Replace these environment variables in your .env file or system environment
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),  # e.g., 'pkc-xxx.region.aws.confluent.cloud:9092'
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_USERNAME', ''),  # Confluent Cloud API Key
    'sasl.password': os.getenv('KAFKA_PASSWORD', '')   # Confluent Cloud API Secret
}

# Create a Schema Registry client
# Replace these environment variables in your .env file or system environment
schema_registry_client = SchemaRegistryClient({
  'url': os.getenv('SCHEMA_REGISTRY_URL', ''),  # e.g., 'https://psrc-xxx.region.gcp.confluent.cloud'
  'basic.auth.user.info': '{}:{}'.format(
      os.getenv('SCHEMA_REGISTRY_USERNAME', ''),  # Confluent Cloud API Key
      os.getenv('SCHEMA_REGISTRY_PASSWORD', '')   # Confluent Cloud API Secret
  )
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_topic-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registery---")
print(schema_str)
print("=====================")

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})



# Load the CSV data into a pandas DataFrame
df = pd.read_csv('retail_data.csv')
# Handle missing values - use 0 for numeric fields, empty string for text
numeric_cols = ['Quantity', 'CustomerID', 'Price']
df[numeric_cols] = df[numeric_cols].fillna(0)
df = df.fillna('')

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    data_value = row.to_dict()
    print(data_value)
    # Convert numeric fields to correct types
    data_value["CustomerID"] = int(data_value["CustomerID"])
    data_value["Quantity"] = int(data_value["Quantity"])
    data_value["StockCode"] = str(data_value["StockCode"])
    data_value["Invoice"] = str(data_value["Invoice"])
    # Produce to Kafka
    producer.produce(
        topic='retail_data_topic', 
        key=str(index), 
        value=data_value,  
        on_delivery=delivery_report 
    )
    producer.flush()
    time.sleep(1)

print("All Data successfully published to Kafka")