import datetime
import json
from pathlib import Path
data_dir = Path(__file__).parent / 'data'
output_dir = Path(__file__).parent / 'output'
credit_card_data_path = Path(data_dir) / 'creditcard.csv'
offline_parquet_path = Path(data_dir) / 'offline.parquet'
online_parquet_path = Path(data_dir) / 'online.parquet'
from_date_str = '2021-01-01 00:00:00'
from_date_obj = datetime.datetime.strptime(from_date_str, '%Y-%m-%d %H:%M:%S')
offline_table_name = 'credit_card_batch'
online_table_name = 'credit_card_online'
model_path = Path(data_dir) / 'lgbm.model'
topic_name = 'credit_card'

avro_schema_json = json.dumps({
    "type": "record",
    "name": topic_name,
    "fields": [
        {"name": "entity_id", "type": "long"},
        {"name": "Class", "type": "string"},
        {"name": "Amount", "type": "double"},
        {"name": "Time", "type": "long"},
        *[{"name": f"V{i}", "type": "double"} for i in range(1, 29)],
        {
            "name": "datetime",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },

    ],
})