import datetime, os, json

import pytz
from google.cloud import bigquery
from google.cloud import storage

import update_data.common

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "odds_batter_prop"
bq_table_full_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

def upload_df_today_odds_to_gcs_bq(df_odds, property):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    pkl_file_name = f'odds_data/odds_{property}_{date_today}.pkl'
    df_odds.to_pickle(pkl_file_name)

    json_file_name = f'odds_data/odds_{property}_{date_today}.txt'
    with open(json_file_name, 'w') as jf:
        for _, row in df_odds.iterrows():
            dict_odds = row.to_dict()
            dict_odds["ingested_datetime"] = datetime.datetime.now()
            jf.write(json.dumps(dict_odds, cls=update_data.common.NpEncoder) + '\n')

    bq_schema = [
        bigquery.SchemaField("game_id", "STRING", "REQUIRED"),
        bigquery.SchemaField("game_date", "DATE", "REQUIRED"),
        bigquery.SchemaField("team_away", "STRING", "REQUIRED"),
        bigquery.SchemaField("team_home", "STRING", "REQUIRED"),
        bigquery.SchemaField("player_name", "STRING", "REQUIRED"),
        bigquery.SchemaField("property", "STRING", "REQUIRED"),
        bigquery.SchemaField("over_odds", "FLOAT", "REQUIRED"),
        bigquery.SchemaField("over_line", "FLOAT", "REQUIRED"),
        bigquery.SchemaField("under_odds", "FLOAT", "REQUIRED"),
        bigquery.SchemaField("under_line", "FLOAT", "REQUIRED"),
        bigquery.SchemaField("ingested_datetime", "DATETIME"),
    ]
    update_data.common.upload_newline_delimited_json_file_to_gcs_then_import_bq(json_file_name, bq_table_full_id, bq_schema, rewrite=True)

bq_client = bigquery.Client()

# fetch today's odds, hits, runs, homeruns, strikeouts, doubles, stolenbasse
import odds_data.odds_hits
import odds_data.odds_runs
import odds_data.odds_homeruns
import odds_data.odds_strikeouts
import odds_data.odds_doubles
import odds_data.odds_stolenbases

print(f"{fetch_print_prefix}fetch today's odds")

df_odds_hits = odds_data.odds_hits.fetch_df_hits_odd_today()
print(f'df_odds_hits\n{df_odds_hits}')
upload_df_today_odds_to_gcs_bq(df_odds_hits, "hits")

df_odds_runs = odds_data.odds_runs.fetch_df_runs_odd_today()
print(f'df_odds_runs\n{df_odds_runs}')
upload_df_today_odds_to_gcs_bq(df_odds_runs, "runs")

df_odds_homeruns = odds_data.odds_homeruns.fetch_df_homeruns_odd_today()
print(f'df_odds_homeruns\n{df_odds_homeruns}')
upload_df_today_odds_to_gcs_bq(df_odds_homeruns, "homeruns")

df_odds_strikeouts = odds_data.odds_strikeouts.fetch_df_strikeouts_odd_today()
print(f'df_odds_strikeouts\n{df_odds_strikeouts}')
upload_df_today_odds_to_gcs_bq(df_odds_strikeouts, "strikeouts")

df_odds_doubles = odds_data.odds_doubles.fetch_df_doubles_odd_today()
print(f'df_odds_doubles\n{df_odds_doubles}')
upload_df_today_odds_to_gcs_bq(df_odds_doubles, "doubles")

df_odds_stolenbases = odds_data.odds_stolenbases.fetch_df_stolenbases_odd_today()
print(f'df_odds_stolenbases\n{df_odds_stolenbases}')
upload_df_today_odds_to_gcs_bq(df_odds_stolenbases, "stolenbases")

