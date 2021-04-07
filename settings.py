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

avro_schema_json = json.dumps({
    "type": "record",
    "name": online_table_name,
    "fields": [
        {"name": "entity_id", "type": "long"},
        {"name": "V27", "type": "float"},
        {"name": "V28", "type": "float"},
    ],
})