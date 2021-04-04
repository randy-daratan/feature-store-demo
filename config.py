from pathlib import Path
root_dir = Path(__file__).parent.parent
data_dir = 'data'
output_dir = 'output'
credit_card_data_path = Path(root_dir) / data_dir / 'creditcard.csv'
offline_parquet_path = Path(root_dir) / data_dir / 'offline.parquet'
online_parquet_path = Path(root_dir) / data_dir / 'online.parquet'
offline_table_name = 'credit_card_batch'
online_table_name = 'credit_card_online'
