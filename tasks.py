import datetime
import json
import pytz
import io
import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer
import os
import numpy as np
import pandas as pd
from feast import Client
from invoke import task
from config import offline_table_name
from scripts import register_feature
from scripts.features import offline_feature_table, online_feature_table


def get_feast_client():

    os.environ["FEAST_CORE_URL"] = "core:6565"
    os.environ["FEAST_SERVING_URL"] = "online_serving:6566"
    os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "output/historical_feature_output"
    os.environ["FEAST_SPARK_STAGING_LOCATION"] = "output/staging"
    client = Client()
    return client


def get_sample_entities():
    return np.random.choice(999999, size=100, replace=False)


def get_sample_entities_with_timestamp():
    entities = get_sample_entities()
    entities_with_timestamp = pd.DataFrame(columns=['entity_id', 'datetime'])
    entities_with_timestamp['entity_id'] = np.random.choice(entities, 10, replace=False)
    entities_with_timestamp['datetime'] = pd.to_datetime(np.random.randint(
        datetime.datetime.now().timestamp(),
        datetime.datetime.now().timestamp(),
        size=10), unit='s')
    return entities_with_timestamp


@task
def register_features():
    client = get_feast_client()
    register_feature.register_entity_and_features(client)


@task
def get_historical_training_data():

    client = get_feast_client()
    job = client.get_historical_features(
        feature_refs=[
            "driver_statistics:avg_daily_trips",
            "driver_statistics:conv_rate",
            "driver_statistics:acc_rate",
            "driver_trips:trips_today"
        ],
        entity_source=get_sample_entities_with_timestamp()
    )
    job.get_output_file_uri()


@task
def populate_online_store_from_offline_data():
    client = get_feast_client()
    job = client.start_offline_to_online_ingestion(
        offline_feature_table,
        datetime.datetime.now() - datetime.timedelta(days=5),
        datetime.datetime.now()
    )
    job.get_status()


@task
def get_online_features():
    client = get_feast_client()
    features = client.get_online_features(
        feature_refs=[f"{offline_table_name}:V27", f"{offline_table_name}:V28"],
        entity_rows=get_sample_entities()).to_dict()


@task
def start_streaming_to_online_store():
    client = get_feast_client()
    job = client.start_stream_to_online_ingestion(
        online_feature_table
    )


def send_avro_record_to_kafka(topic, record):
    value_schema = avro.schema.parse(avro_schema_json)
    writer = DatumWriter(value_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)

    producer = Producer({
        "bootstrap.servers": 'localhost:9092',
    })
    producer.produce(topic=topic, value=bytes_writer.getvalue())
    producer.flush()

