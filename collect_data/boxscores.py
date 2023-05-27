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
from collect_data.game_id_lists import *

base_dir = "collect_data"
_pkl_file_path = f'{base_dir}/boxscores.pkl'

_boxscores = pickle.load(open(_pkl_file_path, 'rb'))
_boxscores_lock = threading.Lock()

def get_boxscore_data(game_id, force_fetch = False):
    global _boxscores
    _boxscores_lock.acquire()
    data = None
    if game_id not in _boxscores or force_fetch:
        try:
            #print(f'calling statsapi.boxscore_data for game_id: {game_id}')
            data = statsapi.boxscore_data(game_id)
        except Exception:
            print(f'Exception: {Exception} while fetching boxscore')
        if data is not None:
            _boxscores[game_id] = data
    elif game_id in _boxscores:
        data = _boxscores[game_id]
    
    _boxscores_lock.release()
    return data

def get_boxscore_data_batch(game_ids, force_fetch = False):
    print(f'{game_ids[:2]} total {len(game_ids)}')
    for i, game_id in enumerate(game_ids):
        if i % 100 == 0:
            print(f'{i} of {game_ids[:2]} total {len(game_ids)}')
        # first fetch and store in cache
        get_boxscore_data(game_id, force_fetch = force_fetch)

    return [get_boxscore_data(game_id, force_fetch = force_fetch) for game_id in game_ids]

def ingest_boxscore_year(year):
    print(f'year: {year}')
    game_id_list = get_game_id_list_year(year)

    game_id_list_splits = np.array_split(game_id_list, 8)
    ths = []
    # single thread is too slong (1+ hour per year) and the bottlenecked in network
    for i, game_id_list_split in enumerate(game_id_list_splits):
        print(f'th {i}')
        th = threading.Thread(target=get_boxscore_data_batch, args=(game_id_list_split, ))
        th.start()
        ths.append(th)

    for th in ths:
        th.join()
    
    print(f'done ingest_boxscore_year {year}')    

def dump_boxscore_cache():
    global _boxscores
    pickle.dump(_boxscores, open(_pkl_file_path, 'wb'))
