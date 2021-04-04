import json
from feast import Feature, FeatureTable, Entity, ValueType
from feast.data_source import FileSource, KafkaSource
from feast.data_format import ParquetFormat, AvroFormat

from config import offline_table_name, offline_parquet_path, online_table_name

entity_name = 'entity_id'
entity = Entity(entity_name, description='entity_id', value_type=ValueType.INT64)

offline_feature_list = [Feature(f'V{i}', ValueType.Float) for i in range(1, 29)] + \
                       [Feature('Amount', ValueType.Float), ('Class', ValueType.STRING), ('Time', ValueType.INT32)]

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

online_feature_list = [Feature('V27', ValueType.Float), Feature('V28', ValueType.Float)]

avro_schema_json = json.dumps({
    "type": "record",
    "name": online_table_name,
    "fields": [
        {"name": "entity_id", "type": "long"},
        {"name": "V27", "type": "float"},
        {"name": "V28", "type": "float"},
    ],
})

online_feature_table = FeatureTable(
    name=online_table_name,
    entities=[entity_name],
    features=online_feature_list,
    stream_source=KafkaSource(
        bootstrap_servers="localhost:9094",
        event_timestamp_column="datetime",
        created_timestamp_column="datetime",
        topic="credit_card",
        message_format=AvroFormat(avro_schema_json)
    )
)
