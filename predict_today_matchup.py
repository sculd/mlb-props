import os

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import math
import collect_data.schedules
import collect_data.game_matchup
import odds_data.query_bq_odds
import update_data.live_prediction

import pycaret
from pycaret import classification
import model.common, model.odds_eval

import numpy as np

date_str = "2023-07-19"

schedules = collect_data.schedules.fetch_schedule_date(date_str)
print(f'schedules: {len(schedules)}')

#game_ids = collect_data.game_id_lists.fetch_game_id_between(date_str, date_str)
#print(f'game_ids[:2]: {game_ids[:2]}')

update_data.live_prediction.update_prediction_db_today()

df_game_matchup_today = collect_data.game_matchup.get_df_game_date(date_str)
print(df_game_matchup_today)

feature_columns, target_column, model_file_name = model.common.features_1hits_recorded, model.common.target_1hits_recorded, model.common.model_1hits_file_name
regression_model = pycaret.classification.load_model(model_file_name)

df_prediction_today = model.odds_eval.df_prediction_add_odd(df_game_matchup_today[['game_id'] + feature_columns + [target_column]], regression_model)

print(df_prediction_today)
