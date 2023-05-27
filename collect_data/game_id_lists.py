import pandas as pd, numpy as np
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import math
import pickle
import threading

from collect_data.common import *

base_dir = "collect_data"
_pkl_file_path = f'{base_dir}/game_id_lists.pkl'

_game_id_lists = pickle.load(open(_pkl_file_path, 'rb'))

def get_game_id_list_year(year, force_fetch = False):
    print(f'getting game_id list {year}')
    global _game_id_lists
    data = None
    if year not in _game_id_lists or force_fetch:
        schedule = statsapi.schedule(start_date = f"{year}-04-01", end_date = get_end_date(year))
        df_schedule = pd.json_normalize(schedule)
        data = list(df_schedule["game_id"].drop_duplicates())
        if data is not None:
            _game_id_lists[year] = data
    elif year in _game_id_lists:
        data = _game_id_lists[year]
        
    print(f'done getting game_id list {year}')
    return data

def dump_game_ids():
    pickle.dump(_game_id_lists, open(_pkl_file_path, 'wb'))


