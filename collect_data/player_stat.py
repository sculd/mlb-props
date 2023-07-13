import pandas as pd, numpy as np
import statsapi
from statsapi import player_stat_data
import requests
from datetime import datetime, timedelta
import numpy as np
import math
import os, pickle
import threading

from collect_data.common import *

base_dir = "collect_data"
_pkl_file_path = f'{base_dir}/player_stats.pkl'

_player_stats = {}
if os.path.isfile(_pkl_file_path):
    _player_stats = pickle.load(open(_pkl_file_path, 'rb'))

def get_player_stat_data(player_id, group, force_fetch = False):
    global _player_stats
    data = None
    key = f"{player_id}_{group}"
    if key not in _player_stats or force_fetch:
        try:
            print(f'calling statsapi.player_stat_data for player_id: {player_id}, group: {group}, key not in _player_stats: {key not in _player_stats}, force_fetch: {force_fetch}')
            data = statsapi.player_stat_data(personId = player_id, group=group, type="yearByYear", sportId=1)
        except Exception:
            print(f'Exception: {Exception} while fetching player_stat_data')
        if data is not None:
            _player_stats[key] = data
    elif key in _player_stats:
        data = _player_stats[key]
    
    return data

def dump_player_stat_data_cache():
    global _player_stats
    pickle.dump(_player_stats, open(_pkl_file_path, 'wb'))

