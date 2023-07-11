import datetime, os, json, math

import numpy as np
from google.cloud import bigquery
from google.cloud import storage
import collect_data.boxscores
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


# load the jsonfied data to bq
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("game_id", "STRING", "REQUIRED"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("boxscore", "JSON", "REQUIRED"),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)


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


def write_boxscores_to_gcs_one_batch(boxscores, b_i):
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

    # uoload the jsonfied data to gcs
    storage_client = storage.Client()
    if storage.Blob(bucket=storage_client.bucket(gs_bucket_name), name=json_file_name).exists(storage_client):
        print(f'{json_file_name} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
        return None

    return json_file_name


def upload_boxscores_to_gcs(boxscores):
    l = len(boxscores)
    n_batches = math.ceil(l / 1000.0)
    print(f'[upload_boxscores_to_gcs] n_batches: {n_batches}, l: {l}')
    game_ids_batch = np.array_split(list(boxscores.keys()), n_batches)

    for i, game_ids in enumerate(game_ids_batch):
        print(f'batch {i}, size: {len(game_ids)}')
        boxscore_batch_file_name = write_boxscores_to_gcs_one_batch({game_id: boxscores[game_id] for game_id in game_ids}, i)
        if boxscore_batch_file_name is None:
            continue

        print(f'{boxscore_batch_file_name}')
        json_file_name = boxscore_batch_file_name

        # uoload the jsonfied data to gcs
        storage_client = storage.Client()
        if storage.Blob(bucket=storage_client.bucket(gs_bucket_name), name=json_file_name).exists(storage_client):
            print(f'{json_file_name} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
            continue

        bucket = storage_client.bucket(gs_bucket_name)
        blob = bucket.blob(json_file_name)

        generation_match_precondition = 0
        blob.upload_from_filename(json_file_name, if_generation_match=generation_match_precondition)

        print(
            f"File {json_file_name} uploaded to {json_file_name}."
        )

        # ingest to bq
        load_job = _bq_client.load_table_from_uri(
            f"gs://{gs_bucket_name}/{json_file_name}",
            table_id,
            location="US",  # Must match the destination dataset location.
            job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.

def upload_boxscores_to_gcs_between(start_date_str, end_date_str):
    print(f'upload_boxscores_to_gcs_between {start_date_str} and {end_date_str}')

    schedules = statsapi.schedule(start_date = start_date_str, end_date = end_date_str)
    game_ids = [sc['game_id'] for sc in schedules]

    boxscores = collect_data.boxscores.ingest_boxscore_game_ids(game_ids)
    upload_boxscores_to_gcs(boxscores)

    print(f'done upload_boxscores_to_gcs_between {start_date_str} to {end_date_str}')
    return boxscores


def upload_boxscores_to_gcs_yesterday():
    date_yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f'upload_boxscores_to_gcs_yesterday {date_yesterday}')
    ret = upload_boxscores_to_gcs_between(date_yesterday, date_yesterday)
    print(f'done upload_boxscores_to_gcs_yesterday {date_yesterday}')
    return ret
