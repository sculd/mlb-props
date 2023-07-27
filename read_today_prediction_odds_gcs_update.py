import datetime, os, json

import update_data.common

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import odds_data.query_bq_odds

property = "all"
df_live_odds = odds_data.query_bq_odds.read_df_property_today(property=property)
df_live_odds_hits = df_live_odds[(df_live_odds.property=="Hits") ]
df_live_odds_strikeouts = df_live_odds[(df_live_odds.property=="Strikeouts")]

pkl_file_name_odds_hits =  f'odds_data/df_odds_today_hits.pkl'
pkl_file_name_odds_strikeouts =  f'odds_data/df_odds_today_strikeouts.pkl'
df_live_odds_hits.to_pickle(pkl_file_name_odds_hits)
df_live_odds_strikeouts.to_pickle(pkl_file_name_odds_strikeouts)
update_data.common.upload_file_to_public_gcs(pkl_file_name_odds_hits, pkl_file_name_odds_hits, rewrite=True)
update_data.common.upload_file_to_public_gcs(pkl_file_name_odds_strikeouts, pkl_file_name_odds_strikeouts, rewrite=True)

import update_data.live_prediction

df_live_prediction = update_data.live_prediction.read_df_live_prediction_bq_today()
df_live_prediction_1hits = df_live_prediction[df_live_prediction.property_name == "batting_1hits_recorded"]
df_live_prediction_2hits = df_live_prediction[df_live_prediction.property_name == "batting_2hits_recorded"]
df_live_prediction_1strikeouts = df_live_prediction[df_live_prediction.property_name == "batting_1strikeOuts_recorded"]
pkl_file_name_prediction_1hits = 'update_data/df_live_prediction_batting_1hits_recorded.pkl'
pkl_file_name_prediction_2hits = 'update_data/df_live_prediction_batting_2hits_recorded.pkl'
pkl_file_name_prediction_1strikeouts = 'update_data/df_live_prediction_batting_1strikeOuts_recorded.pkl'
df_live_prediction_1hits.to_pickle(pkl_file_name_prediction_1hits)
df_live_prediction_2hits.to_pickle(pkl_file_name_prediction_2hits)
df_live_prediction_1strikeouts.to_pickle(pkl_file_name_prediction_1strikeouts)
update_data.common.upload_file_to_public_gcs(pkl_file_name_prediction_1hits, pkl_file_name_prediction_1hits, rewrite=True)
update_data.common.upload_file_to_public_gcs(pkl_file_name_prediction_2hits, pkl_file_name_prediction_2hits, rewrite=True)
update_data.common.upload_file_to_public_gcs(pkl_file_name_prediction_1strikeouts, pkl_file_name_prediction_1strikeouts, rewrite=True)

