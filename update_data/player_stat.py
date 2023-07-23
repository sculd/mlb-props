import datetime, os, json, math

import numpy as np
from google.cloud import bigquery
from google.cloud import storage
import collect_data.boxscores
import update_data.common

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "player_stat"
table_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

_bq_client = bigquery.Client()


def write_player_stats_to_gcs_one_batch(player_stats, b_i):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    json_file_name = f'update_data/temp/player_stats_{date_today}_{b_i}.txt'
    with open(json_file_name, 'w') as jf:
        for player_id_side, player_stat in player_stats.items():
            payload = {
                'player_id_side': player_id_side,
                'ingestion_date': datetime.datetime.now().date(),
                'player_stat': player_stat,
            }
            jf.write(json.dumps(payload, cls=update_data.common.NpEncoder) + '\n')

    # uoload the jsonfied data to gcs
    storage_client = storage.Client()
    if storage.Blob(bucket=storage_client.bucket(gs_bucket_name), name=json_file_name).exists(storage_client):
        print(f'{json_file_name} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
        return None

    return json_file_name


def upload_player_stats_to_gcs(player_stats):
    l = len(player_stats)
    if l == 0:
        return
    n_batches = math.ceil(l / 1000.0)
    print(f'[upload_player_stats_to_gcs] n_batches: {n_batches}, l: {l}')
    player_stats_batch = np.array_split(list(player_stats.keys()), n_batches)

    for i, player_id_sides in enumerate(player_stats_batch):
        print(f'batch {i}, size: {len(player_id_sides)}')
        player_stats_batch_file_name = write_player_stats_to_gcs_one_batch({player_id_side: player_stats[player_id_side] for player_id_side in player_id_sides}, i)
        if player_stats_batch_file_name is None:
            continue

        print(f'{player_stats_batch_file_name}')
        update_data.common.upload_newline_delimited_json_file_to_gcs_then_import_bq(
            player_stats_batch_file_name, table_id,
            [
                bigquery.SchemaField("player_id_side", "STRING", "REQUIRED"),
                bigquery.SchemaField("ingestion_date", "DATE", "REQUIRED"),
                bigquery.SchemaField("player_stat", "JSON", "REQUIRED"),
            ]
        )
