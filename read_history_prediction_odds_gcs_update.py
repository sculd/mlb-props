import datetime, os, json

import update_data.common

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import odds_data.query_bq_odds

property = "all"
df_history_odds = odds_data.query_bq_odds.read_df_property_2023(property=property)
df_history_odds_hits = df_history_odds[(df_history_odds.property=="Hits") ]
df_history_odds_strikeouts = df_history_odds[(df_history_odds.property=="Strikeouts")]

pkl_file_name_odds_hits =  f'odds_data/df_odds_history_hits.pkl'
pkl_file_name_odds_strikeouts =  f'odds_data/df_odds_history_strikeouts.pkl'
df_history_odds_hits.to_pickle(pkl_file_name_odds_hits)
df_history_odds_strikeouts.to_pickle(pkl_file_name_odds_strikeouts)
update_data.common.upload_file_to_public_gcs(pkl_file_name_odds_hits, pkl_file_name_odds_hits, rewrite=True)
update_data.common.upload_file_to_public_gcs(pkl_file_name_odds_strikeouts, pkl_file_name_odds_strikeouts, rewrite=True)

import update_data.prediction

df_history_prediction = update_data.prediction.read_df_prediction_bq_2023()
df_history_prediction_1hits = df_history_prediction[df_history_prediction.property_name == "batting_1hits_recorded"]
df_history_prediction_1strikeouts = df_history_prediction[df_history_prediction.property_name == "batting_1strikeOuts_recorded"]
pkl_file_name_prediction_1hits = 'update_data/df_history_prediction_batting_1hits_recorded.pkl'
pkl_file_name_prediction_1strikeouts = 'update_data/df_history_prediction_batting_1strikeOuts_recorded.pkl'
df_history_prediction_1hits.to_pickle(pkl_file_name_prediction_1hits)
df_history_prediction_1strikeouts.to_pickle(pkl_file_name_prediction_1strikeouts)
update_data.common.upload_file_to_public_gcs(pkl_file_name_prediction_1hits, pkl_file_name_prediction_1hits, rewrite=True)
update_data.common.upload_file_to_public_gcs(pkl_file_name_prediction_1strikeouts, pkl_file_name_prediction_1strikeouts, rewrite=True)

