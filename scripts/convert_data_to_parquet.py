import datetime
import numpy as np
import pandas as pd

from config import credit_card_data_path, offline_parquet_path, online_parquet_path

df = pd.read_csv(credit_card_data_path)
offline_cols = [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class', 'Time']
df_offline = df[offline_cols]

# add entity id
df_offline['entity_id'] = np.arange(0, df_offline.shape[0])
df_offline.set_index('entity_id')

# add datetime field
df_offline['datetime'] = pd.to_datetime(datetime.datetime.now()) + pd.to_timedelta(df_offline['Time'], unit='s')
df_offline['created'] = pd.to_datetime(datetime.datetime.now())

df_offline.to_parquet(offline_parquet_path, allow_truncated_timestamps=True)

df_online = df_offline[['V27', 'V28']]
df_online.to_parquet(online_parquet_path, allow_truncated_timestamps=True)
