import datetime
import numpy as np
import pandas as pd

import settings


def convert_to_parquet():
    df = pd.read_csv(settings.credit_card_data_path)
    df['Class'] = df['Class'].astype(str)
    df['Time'] = df['Time'].astype(int)
    offline_cols = [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class', 'Time']
    df_offline = df[offline_cols]

    # add entity id
    df_offline['entity_id'] = np.arange(0, df_offline.shape[0])
    df_offline.set_index('entity_id')

    # add datetime field
    df_offline['datetime'] = pd.to_datetime(settings.from_date_obj)  # + pd.to_timedelta(df_offline['Time'], unit='s')
    df_offline['created'] = pd.to_datetime(settings.from_date_obj)

    df_offline.to_parquet(settings.offline_parquet_path, allow_truncated_timestamps=True)

    df_online = df_offline[['V27', 'V28']]
    df_online.to_parquet(settings.online_parquet_path, allow_truncated_timestamps=True)

convert_to_parquet()