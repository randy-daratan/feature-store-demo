from pathlib import Path
data_dir = 'data'
output_dir = 'output'
credit_card_data_path = Path(data_dir) / 'creditcard.csv'
offline_parquet_path = Path(data_dir) / 'offline.parquet'
online_parquet_path = Path(data_dir) / 'online.parquet'
offline_table_name = 'credit_card_batch'
online_table_name = 'credit_card_online'
model_path = Path()