import os

import datetime, json, math

import pytz

from google.cloud import bigquery
import update_data.common

import pandas as pd, numpy as np

gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_live_id = "matchup_live"
_hisotrybq_table_id = "matchup_history"
bq_table_live_full_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_live_id}'
bq_table_hisotry_full_id = f'{gcp_project_id}.{bq_dataset_id}.{_hisotrybq_table_id}'

_bq_client = bigquery.Client()


def write_df_matchup_local_temp(df_matchup):
    date_today = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    json_file_name = f'update_data/temp/df_live_prediction_{date_today}.txt'
    now = datetime.datetime.now()
    with open(json_file_name, 'w') as jf:
        for _, matchup in df_matchup.iterrows():
            payload = \
                {
                    "game_id": matchup.game_id,
                    "pitching_gamesPlayed": matchup.pitching_gamesPlayed,
                    "pitching_runs_per_game": matchup.pitching_runs_per_game,
                    "pitching_hits_per_game": matchup.pitching_hits_per_game,
                    "pitching_cur_hits_per_pitch": matchup.pitching_cur_hits_per_pitch,
                    "pitching_cur_strikeOuts_per_pitch": matchup.pitching_cur_strikeOuts_per_pitch,
                    "pitching_name": matchup.pitching_name,
                    "batting_gamesPlayed": matchup.batting_gamesPlayed,
                    "batting_runs_per_game": matchup.batting_runs_per_game,
                    "batting_strikeOuts_per_game": matchup.batting_strikeOuts_per_game,
                    "batting_hits_per_game": matchup.batting_hits_per_game,
                    "batting_rbi": matchup.batting_rbi,
                    "batting_cur_season_avg": matchup.batting_cur_season_avg,
                    "batting_name": matchup.batting_name,
                    "temp": matchup.temp,
                    "game_venue": matchup.game_venue,
                    "pitching_id": matchup.pitching_id,
                    "batting_id": matchup.batting_id,
                    "game_date": matchup.game_date.date(),
                    "game_year": matchup.game_year.year,
                    "batting_teamName": matchup.batting_teamName,
                    "batting_shortName": matchup.batting_shortName,
                    "ingestion_datetime": now,
                }

            jf.write(json.dumps(payload, cls=update_data.common.NpEncoder) + '\n')

    return json_file_name

def write_df_matchup_bq(df_matchup, is_live):
    json_file_name = write_df_matchup_local_temp(df_matchup)
    print(f'{json_file_name}\n')
    schema = \
        [
            bigquery.SchemaField("game_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("pitching_gamesPlayed", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("pitching_runs_per_game", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("pitching_hits_per_game", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("pitching_cur_hits_per_pitch", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("pitching_cur_strikeOuts_per_pitch", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("pitching_name", "STRING", "REQUIRED"),
            bigquery.SchemaField("batting_gamesPlayed", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("batting_runs_per_game", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("batting_strikeOuts_per_game", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("batting_hits_per_game", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("batting_rbi", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("batting_cur_season_avg", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("batting_name", "STRING", "REQUIRED"),
            bigquery.SchemaField("temp", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("game_venue", "STRING", "REQUIRED"),
            bigquery.SchemaField("pitching_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("batting_id", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("game_date", "DATE", "REQUIRED"),
            bigquery.SchemaField("game_year", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("batting_teamName", "STRING", "REQUIRED"),
            bigquery.SchemaField("batting_shortName", "STRING", "REQUIRED"),
            bigquery.SchemaField("ingestion_datetime", "DATETIME"),
        ]

    bq_table_full_id = bq_table_live_full_id if is_live else bq_table_hisotry_full_id
    update_data.common.upload_newline_delimited_json_file_to_gcs_then_import_bq(
        json_file_name, bq_table_full_id, schema, rewrite=True
    )

