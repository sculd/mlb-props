import pandas as pd, numpy as np
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import pickle

from collect_data.common import *

base_dir = "collect_data"
_pkl_file_path = f'{base_dir}/schedules.pkl'

_schedules = pickle.load(open(_pkl_file_path, 'rb'))

def get_end_date(year):
    end_date = f"{year+1}-03-01"
    if year == 2022:
        end_date = f"{year}-12-01"
    elif year == 2023:
        end_date = (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
    return end_date

def fetch_schdules_year(year):
    print(f'fetch_schdules_year {year}')
    global _schedules
    data = None

    schedules = statsapi.schedule(start_date = f"{year}-04-01", end_date = get_end_date(year))
    for sc in schedules:
        _schedules[sc['game_id']] = sc

    print(f'done fetch_schdules_year {year}')

def dump_schedule_data():
    pickle.dump(_schedules, open(_pkl_file_path, 'wb'))

