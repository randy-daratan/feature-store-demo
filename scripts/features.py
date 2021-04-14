import json
from feast import Feature, FeatureTable, Entity, ValueType
from feast.data_source import FileSource, KafkaSource
from feast.data_format import ParquetFormat, AvroFormat

from settings import offline_table_name, offline_parquet_path, online_table_name, avro_schema_json

entity_name = 'entity_id'
entity = Entity(entity_name, description='entity_id', value_type=ValueType.INT64)

offline_feature_list = [Feature(f'V{i}', ValueType.DOUBLE, labels={'name': f'V{i}'}) for i in range(1, 29)] + \
                       [Feature('Amount', ValueType.DOUBLE), Feature('Class', ValueType.STRING), Feature('Time', ValueType.INT64)]

offline_feature_table = FeatureTable(
    name=offline_table_name,
    entities=[entity_name],
    features=offline_feature_list,
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url='file://'+str(offline_parquet_path),
    ),
    stream_source=KafkaSource(
        bootstrap_servers="localhost:9094",
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        topic="credit_card",
        message_format=AvroFormat(avro_schema_json)
    )
)

online_feature_list = [Feature('V27', ValueType.DOUBLE), Feature('V28', ValueType.DOUBLE)]


online_feature_table = FeatureTable(
    name=online_table_name,
    entities=[entity_name],
    features=online_feature_list,
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url='file://'+str(offline_parquet_path),
    ),
    stream_source=KafkaSource(
        bootstrap_servers="localhost:9094",
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        topic="credit_card",
        message_format=AvroFormat(avro_schema_json)
    )
)
