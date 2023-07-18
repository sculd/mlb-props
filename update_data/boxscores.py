import datetime, os, json, math

import pytz
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
import collect_data.boxscores
import update_data.common
import statsapi

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "boxscore"
table_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

_bq_client = bigquery.Client()

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)


def write_boxscores_local_temp(boxscores, b_i):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    json_file_name = f'update_data/temp/boxscore_{date_today}_{b_i}.txt'
    with open(json_file_name, 'w') as jf:
        for game_id, boxscore in boxscores.items():
            # gameId (not game_id) is part of the boxscore e.g. '2023/05/01/atlmlb-nynmlb-1'
            # game_id is the unique identifier
            dte = datetime.datetime.strptime('/'.join(boxscore['gameId'].split('/')[:3]), '%Y/%m/%d').date()
            payload = {
                'game_id': game_id,
                'date': dte,
                'boxscore': boxscore,
            }
            jf.write(json.dumps(payload, cls=NpEncoder) + '\n')

    return json_file_name

def upload_boxscores_to_gcs(boxscores):
    l = len(boxscores)
    if l == 0:
        return
    n_batches = math.ceil(l / 1000.0)
    print(f'[upload_boxscores_to_gcs] n_batches: {n_batches}, boxscores: {l}')
    game_ids_batch = np.array_split(list(boxscores.keys()), n_batches)

    for i, game_ids in enumerate(game_ids_batch):
        print(f'batch {i}, size: {len(game_ids)}')
        boxscore_batch_file_name = write_boxscores_local_temp({game_id: boxscores[game_id] for game_id in game_ids}, i)
        if boxscore_batch_file_name is None:
            print('filed to create local batch for {i}')
            continue

        print(f'{boxscore_batch_file_name}')
        update_data.common.upload_newline_delimited_json_file_to_gcs_then_import_bq(
            boxscore_batch_file_name, table_id,
            [
                bigquery.SchemaField("game_id", "STRING", "REQUIRED"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("boxscore", "JSON", "REQUIRED"),
            ]
        )

def upload_boxscores_to_gcs_between(start_date_str, end_date_str):
    print(f'upload_boxscores_to_gcs_between {start_date_str} and {end_date_str}')

    schedules = statsapi.schedule(start_date = start_date_str, end_date = end_date_str)
    game_ids = [sc['game_id'] for sc in schedules]

    print(f'[upload_boxscores_to_gcs_between] game_ids: {len(game_ids)}')
    boxscores = collect_data.boxscores.ingest_boxscore_game_ids(game_ids)
    upload_boxscores_to_gcs(boxscores)

    print(f'done upload_boxscores_to_gcs_between {start_date_str} to {end_date_str}')
    return boxscores

def upload_boxscores_to_gcs_ndays_prior(days):
    date_ndays_prior = (datetime.datetime.now(pytz.timezone('US/Pacific')) - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    print(f'upload_boxscores_to_gcs_ndays_prior days: {days}, {date_ndays_prior}')
    ret = upload_boxscores_to_gcs_between(date_ndays_prior, date_ndays_prior)
    print(f'done upload_boxscores_to_gcs_ndays_prior {date_ndays_prior}')
    return ret

def upload_boxscores_to_gcs_yesterday():
    return upload_boxscores_to_gcs_ndays_prior(1)

def upload_boxscores_to_gcs_2days_prior():
    return upload_boxscores_to_gcs_ndays_prior(2)

def read_boxscores_bq(start_date_str, end_date_str):
    print(f'read_boxscores_bq {start_date_str} and {end_date_str}')

    query = f"""
        SELECT game_id, boxscore 
        FROM `trading-290017.major_league_baseball.boxscore`
        where date >= "{start_date_str}" AND date <= "{end_date_str}"
        """
    print(f'running bq query\b{query}')

    query_job = _bq_client.query(query)
    rows = query_job.result()  # Waits for query to finish
    boxscores = {int(row.game_id): json.loads(row.boxscore) for row in rows}
    print(f'done read_boxscores_bq {start_date_str} to {end_date_str}')

    return boxscores