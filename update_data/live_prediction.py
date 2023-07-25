import os

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import datetime, json, math

import pytz
import collect_data.schedules
import collect_data.game_matchup
import odds_data.query_bq_odds
import model.common, model.odds_eval

import pycaret
from pycaret import classification
from google.cloud import datastore
from google.cloud import bigquery
import update_data.common

import pandas as pd, numpy as np

gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "live_prediction_batter_prop"
bq_table_full_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'
ds_entity_type = "MLBBatterPropPredictionOdds"

_ds_client = datastore.Client()
_sd_write_size_batch = 30

_regression_model_1hits = pycaret.classification.load_model(model.common.model_1hits_file_name)
_regression_model_1hstrikeouts = pycaret.classification.load_model(model.common.model_1hstrikeouts_file_name)

_bq_client = bigquery.Client()


def write_df_prediction_datastore(df_prediction_odds, property_column_name):
    entities = []
    for _, prediction_odds in df_prediction_odds.iterrows():
        key = _ds_client.key(ds_entity_type)
        entity = datastore.Entity(key)
        entity_properties = \
            {
                "date": prediction_odds.game_date,
                "date_str": str(prediction_odds.game_date.date()),

                "game_venue": prediction_odds.game_venue,
                "game_id": prediction_odds.game_id,
                "pitching_name": prediction_odds.pitching_name,
                "pitching_id": prediction_odds.pitching_id,
                "batting_name": prediction_odds.batting_name,
                "batting_id": prediction_odds.batting_id,

                "property_name": property_column_name,

                "prediction_label": prediction_odds.prediction_label,
                "prediction_score": prediction_odds.prediction_score,
                "theo_odds": prediction_odds.theo_odds,
            }

        entity.update(entity_properties)
        entities.append(entity)

    if len(entities) == 0:
        return

    n_batches = math.ceil(len(entities) / _sd_write_size_batch)
    entities_batches = np.array_split(entities, n_batches)
    for entities_batch in entities_batches:
        entities_batch = list(entities_batch)
        _ds_client.put_multi(entities_batch)


def write_df_prediction_local_temp(df_prediction, property_column_name):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    pkl_file_name = f'update_data/temp/df_live_prediction_{property_column_name}_{date_today}.pkl'
    df_prediction.to_pickle(pkl_file_name)
    pkl_gcs_public_file_name = f'update_data/df_live_prediction_{property_column_name}.pkl'
    update_data.common.upload_file_to_public_gcs(pkl_file_name, pkl_gcs_public_file_name, rewrite=True)

    json_file_name = f'update_data/temp/df_live_prediction_{property_column_name}_{date_today}.txt'
    with open(json_file_name, 'w') as jf:
        for _, prediction in df_prediction.iterrows():
            payload = \
                {
                    "date": prediction.game_date.date(),

                    "game_venue": prediction.game_venue,
                    "game_id": prediction.game_id,
                    "pitching_name": prediction.pitching_name,
                    "pitching_id": prediction.pitching_id,
                    "batting_name": prediction.batting_name,
                    "batting_id": prediction.batting_id,

                    "property_name": property_column_name,

                    "prediction_label": prediction.prediction_label,
                    "prediction_score": prediction.prediction_score,
                    "theo_odds": prediction.theo_odds,

                    "batting_teamName": prediction.batting_teamName,
                    "batting_shortName": prediction.batting_shortName,
                    "ingestion_datetime": datetime.datetime.now(),
                }

            jf.write(json.dumps(payload, cls=update_data.common.NpEncoder) + '\n')

    return json_file_name, pkl_file_name

def write_df_prediction_bq(df_prediction, property_column_name):
    json_file_name, pkl_file_name = write_df_prediction_local_temp(df_prediction, property_column_name)
    print(f'{json_file_name}\n{pkl_file_name}')
    schema = \
        [
            bigquery.SchemaField("date", "DATE", "REQUIRED"),
            bigquery.SchemaField("game_venue", "STRING", "REQUIRED"),
            bigquery.SchemaField("game_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("pitching_name", "STRING", "REQUIRED"),
            bigquery.SchemaField("pitching_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("batting_name", "STRING", "REQUIRED"),
            bigquery.SchemaField("batting_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("property_name", "STRING", "REQUIRED"),
            bigquery.SchemaField("prediction_label", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("prediction_score", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("theo_odds", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("batting_teamName", "STRING"),
            bigquery.SchemaField("batting_shortName", "STRING"),
            bigquery.SchemaField("ingestion_datetime", "DATETIME"),
        ]
    update_data.common.upload_newline_delimited_json_file_to_gcs_then_import_bq(
        json_file_name, bq_table_full_id, schema, rewrite=True
    )


def update_prediction_datastore_between(start_date_str, end_date_str):
    print(f'update_prediction_odds_datastore_between {start_date_str} {end_date_str}')
    schedules = collect_data.schedules.fetch_schedule_between(start_date_str, end_date_str)
    print(f'schedules: {len(schedules)}')

    df_game_matchup = collect_data.game_matchup.get_df_game_between(start_date_str, end_date_str)

    df_prediction_1hits = model.odds_eval.df_prediction_add_odd(df_game_matchup[['game_id'] + model.common.features_1hits_recorded + [model.common.target_1hits_recorded]], _regression_model_1hits)
    write_df_prediction_datastore(df_prediction_1hits, "batting_1hits_recorded")

    df_prediction_1strikeouts = model.odds_eval.df_prediction_add_odd(df_game_matchup[['game_id'] + model.common.features_1hstrikeouts_recorded + [model.common.target_1hstrikeouts_recorded]], _regression_model_1hstrikeouts)
    write_df_prediction_datastore(df_prediction_1strikeouts, "batting_1strikeOuts_recorded")


def update_prediction_bq_between(start_date_str, end_date_str):
    print(f'update_prediction_bq_between {start_date_str} {end_date_str}')
    schedules = collect_data.schedules.fetch_schedule_between(start_date_str, end_date_str)
    print(f'schedules: {len(schedules)}')

    df_game_matchup = collect_data.game_matchup.get_df_game_between(start_date_str, end_date_str)

    df_prediction_1hits = model.odds_eval.df_prediction_add_odd(df_game_matchup[['game_id'] + model.common.features_1hits_recorded + [model.common.target_1hits_recorded]], _regression_model_1hits)
    write_df_prediction_bq(df_prediction_1hits, "batting_1hits_recorded")

    df_prediction_1strikeouts = model.odds_eval.df_prediction_add_odd(df_game_matchup[['game_id'] + model.common.features_1hstrikeouts_recorded + [model.common.target_1hstrikeouts_recorded]], _regression_model_1hstrikeouts)
    write_df_prediction_bq(df_prediction_1strikeouts, "batting_1strikeOuts_recorded")


def update_prediction_db_between(start_date_str, end_date_str):
    update_prediction_bq_between(start_date_str, end_date_str)
    update_prediction_datastore_between(start_date_str, end_date_str)

def update_prediction_db_ndays_prior(days):
    date_ndays_prior = (datetime.datetime.now(pytz.timezone('US/Eastern')) - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    print(f'update_prediction_bq_ndays_prior days: {days}, {date_ndays_prior}')
    ret = update_prediction_db_between(date_ndays_prior, date_ndays_prior)
    print(f'done update_prediction_bq_ndays_prior {date_ndays_prior}')
    return ret

def update_prediction_db_yesterday():
    return update_prediction_db_ndays_prior(1)

def update_prediction_db_today():
    return update_prediction_db_ndays_prior(0)

def read_df_live_prediction_bq_between(start_date_str, end_date_str):
    print(f'read_df_live_prediction_bq_between {start_date_str} and {end_date_str}')

    query = f"""
        SELECT * 
        FROM `trading-290017.major_league_baseball.live_prediction_batter_prop`
        where date >= "{start_date_str}" AND date <= "{end_date_str}"
        """
    print(f'running bq query\b{query}')

    query_job = _bq_client.query(query)
    rows = query_job.result()  # Waits for query to finish
    row_dicts = []
    for row in query_job:
        # Row values can be accessed by field name or index.
        row_dict = {k: v for k, v in row.items()}
        row_dicts.append(row_dict)

    df_live_prediction = pd.DataFrame(row_dicts)
    if len(df_live_prediction) == 0:
        print('df_live_prediction is empty')
        return df_live_prediction
    
    dedupe_keys = ["game_id", "pitching_name", "batting_name", "property_name"]
    df_live_prediction = df_live_prediction.sort_values(dedupe_keys + ["ingestion_datetime"]).drop_duplicates(dedupe_keys, keep='last')
    print(f'done read_rediction_bq_between {start_date_str} to {end_date_str}')

    return df_live_prediction

def read_df_live_prediction_bq_today():
    print(f'read_df_live_prediction_bq_today')
    date_str_today = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d")
    return read_df_live_prediction_bq_between(date_str_today, date_str_today)



