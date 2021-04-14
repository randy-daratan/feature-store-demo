import datetime
import json
import pytz
from pyarrow.parquet import ParquetDataset
from urllib.parse import urlparse
import io
import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer
import os
import numpy as np
import pandas as pd
from feast import Client
from invoke import task
from settings import offline_table_name, avro_schema_json, from_date_obj, online_table_name, topic_name
from scripts import register_feature
from scripts.features import offline_feature_table, online_feature_table
from scripts.convert_data_to_parquet import convert_to_parquet
'''
1. Convert credit card csv to dataframe for feast historical store
2. Register feature table definition to feast-core
3. Inspect feature and metadata
4. Get historical data for training purpose
5. Sync historical data to online store
6. Get feature from online store
7. Update feature store from kafka
8. Prediction module
9. Package prediction into an api of some sort
'''


@task
def ingest_data(c):
    '''
    1. Convert credit card csv to dataframe for feast historical store
    '''
    client = get_feast_client()
    df_offline, df_online = convert_to_parquet()
    client.ingest(offline_feature_table, df_offline)

@task
def register_features(c):
    '''
    2. Register feature table definition to feast-core
    '''
    client = get_feast_client()
    register_feature.register_entity_and_features(client)


@task
def inspect_features(c):
    """
    3. Inspect feature and metadata
    """
    client = get_feast_client()
    print('############################')
    print('1. LIST OF ALL FEATURE TABLES')
    print(client.list_feature_tables())
    print('############################')
    print('############################')
    print('2. OFFLINE TABLE')
    feature_table = client.get_feature_table("credit_card_batch")
    print(feature_table.created_timestamp)
    print()
    print(client.get_feature_table("credit_card_batch").to_yaml())
    print('############################')


@task
def get_historical_data(c):
    '''
    4. Get historical data for training purpose
    '''
    client = get_feast_client()
    job = client.get_historical_features(
        feature_refs=[
            "credit_card_batch:V1",
            "credit_card_batch:V2",
            "credit_card_batch:Time",
            "credit_card_batch:Class",
        ],
        entity_source=get_sample_entities_with_timestamp()
    )
    output_file_uri=job.get_output_file_uri()
    print(output_file_uri)
    df = read_parquet(output_file_uri)
    print(df)

@task
def sync_offline_to_online(c):
    '''
    5. Sync historical data to online store
    '''
    client = get_feast_client()
    job = client.start_offline_to_online_ingestion(
        offline_feature_table,
        datetime.datetime(2020, 10, 10),
        datetime.datetime.now()
    )
    job.get_status()


@task
def get_online_features(c):
    '''
    6. Get feature from online store
    '''
    client = get_feast_client()
    features = client.get_online_features(
        feature_refs=[f"{offline_table_name}:V28", f"{offline_table_name}:V27"],
        entity_rows=[{'entity_id': 2000} for i in range(1)]).to_dict()
    print(features)


@task
def create_topic(c):
    """
    7. Update feature store from kafka
    """
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({'bootstrap.servers': 'localhost:9094'})
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
    admin.create_topics([new_topic])


@task
def start_streaming_to_online_store(c):
    """
    7. Update feature store from kafka
    """
    client = get_feast_client()
    job = client.start_stream_to_online_ingestion(
        offline_feature_table
    )


@task
def send_record_to_kafka(c):
    """
    7. Update feature store from kafka
    """
    record = {f'V{i}': 1000 for i in range(1, 29)}
    record.update({
        'datetime': datetime.datetime.now().replace(tzinfo=pytz.utc),
        'entity_id': 2000,
        'Class': '1',
        'Time': 1,
        'Amount': 1,
    })
    send_avro_record_to_kafka(topic=topic_name, record=record)


def get_feast_client():

    os.environ["FEAST_REDIS_HOST"] = 'localhost'
    os.environ["FEAST_SPARK_HOME"] = '/Users/ailabadmin/opt/anaconda3/lib/python3.8/site-packages/pyspark'
    os.environ["FEAST_SPARK_LAUNCHER"] = 'standalone'
    os.environ["FEAST_CORE_URL"] = "localhost:6565"
    os.environ["FEAST_SERVING_URL"] = "localhost:6566"
    os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "file:///Users/ailabadmin/feature-store-demo/output/historical_feature_output"
    os.environ["FEAST_SPARK_STAGING_LOCATION"] = "file:///Users/ailabadmin/feature-store-demo/output/staging"
    client = Client()
    return client


def read_parquet(uri):
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "file":
        return pd.read_parquet(parsed_uri.path)
    elif parsed_uri.scheme == 's3':
        import s3fs
        fs = s3fs.S3FileSystem()
        files = ["s3://" + path for path in fs.glob(uri + '/part-*')]
        ds = ParquetDataset(files, filesystem=fs)
        return ds.read().to_pandas()
    elif parsed_uri.scheme == 'wasbs':
        import adlfs
        fs = adlfs.AzureBlobFileSystem(
            account_name=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_NAME'), account_key=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY')
        )
        uripath = parsed_uri.username + parsed_uri.path
        files = fs.glob(uripath + '/part-*')
        ds = ParquetDataset(files, filesystem=fs)
        return ds.read().to_pandas()
    else:
        raise ValueError(f"Unsupported URL scheme {uri}")


def get_sample_entities():
    return np.arange(0, 2000).tolist()


def get_sample_entities_with_timestamp():
    entities_with_timestamp = pd.DataFrame(columns=['entity_id', 'event_timestamp'])
    entities_with_timestamp['entity_id'] = np.arange(0, 2000)
    entities_with_timestamp['event_timestamp'] = pd.to_datetime(from_date_obj)
    return entities_with_timestamp

@task
def delete_feature_table(c):
    client = get_feast_client()
    client.delete_feature_table('credit_card_batch')
    client.delete_feature_table('credit_card_online')


@task
def display_result(c):
    df = read_parquet('file:///Users/ailabadmin/feature-store-demo/output/historical_feature_output/b3e8b2a3-2ec7-4ffb-aaf9-8cf0eb636d46')
    print(df)


def send_avro_record_to_kafka(topic, record):
    value_schema = avro.schema.parse(avro_schema_json)
    writer = DatumWriter(value_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)

    producer = Producer({
        "bootstrap.servers": 'localhost:9094',
    })
    producer.produce(topic=topic, value=bytes_writer.getvalue())
    producer.flush()

