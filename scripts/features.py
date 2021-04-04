import os

from feast import FeatureTable
from feast.data_source import FileSource, KafkaSource
from feast.data_format import ParquetFormat, ProtoFormat

from .config import (entity_name, offline_table_name, offline_parquet_path,
                     offline_feature_list, online_table_name,
                     online_feature_list)

offline_feature_table = FeatureTable(
    name=offline_table_name,
    entities=[entity_name],
    features=offline_feature_list,
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url=offline_parquet_path,
    )
)

online_feature_table = FeatureTable(
    name=online_table_name,
    entities=[entity_name],
    features=online_feature_list,
    stream_source=KafkaSource(
        bootstrap_servers="localhost:9094",
        message_format=ProtoFormat(class_path="class.path"),
        topic="credit_card",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
)
