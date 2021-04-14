import datetime
import numpy as np
import pandas as pd

import settings


def convert_to_parquet():
    df = pd.read_csv(settings.credit_card_data_path)
    df.loc[:, 'Class'] = df['Class'].astype(str)
    df.loc[:, 'Time'] = df['Time'].astype(int)
    offline_cols = [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class', 'Time']
    df_offline = df[offline_cols]

    # add entity id
    df_offline.loc[:, 'entity_id'] = np.arange(0, df_offline.shape[0])
    df_offline.set_index('entity_id')

    # add datetime field
    df_offline.loc[:, 'datetime'] = pd.to_datetime(settings.from_date_obj)  # + pd.to_timedelta(df_offline['Time'], unit='s')
    df_offline.loc[:, 'created'] = pd.to_datetime(settings.from_date_obj)

    df_online = df_offline[['V27', 'V28']]
    return df_offline, df_online
