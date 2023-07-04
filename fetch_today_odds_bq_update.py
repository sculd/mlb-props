import datetime, os, json

from google.cloud import bigquery
from google.cloud import storage

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "odds_batter_prop_copy"
table_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

# load the jsonfied data to bq
job_config = bigquery.LoadJobConfig(
    schema=[
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
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

def upload_df_odds_to_gcs(df_odds, property):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    json_file_name = f'odds_data/odds_{property}_{date_today}.txt'
    with open(json_file_name, 'w') as jf:
        for _, row in df_odds.iterrows():
            jf.write(json.dumps(row.to_dict()) + '\n')

    # uoload the jsonfied data to gcs
    storage_client = storage.Client()
    if storage.Blob(bucket=storage_client.bucket(gs_bucket_name), name=json_file_name).exists(storage_client):
        print(f'{json_file_name} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
        return

    bucket = storage_client.bucket(gs_bucket_name)
    blob = bucket.blob(json_file_name)

    generation_match_precondition = 0
    blob.upload_from_filename(json_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {json_file_name} uploaded to {json_file_name}."
    )

    # ingest to bq
    load_job = bq_client.load_table_from_uri(
        f"gs://{gs_bucket_name}/{json_file_name}",
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.


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
print(f'df_odds_hits {df_odds_hits}')
upload_df_odds_to_gcs(df_odds_hits, "hits")

df_odds_runs = odds_data.odds_runs.fetch_df_runs_odd_today()
print(f'df_odds_runs {df_odds_runs}')
upload_df_odds_to_gcs(df_odds_runs, "runs")

df_odds_homeruns = odds_data.odds_homeruns.fetch_df_homeruns_odd_today()
print(f'df_odds_homeruns {df_odds_homeruns}')
upload_df_odds_to_gcs(df_odds_homeruns, "homeruns")

df_odds_strikeouts = odds_data.odds_strikeouts.fetch_df_strikeouts_odd_today()
print(f'df_odds_strikeouts {df_odds_strikeouts}')
upload_df_odds_to_gcs(df_odds_strikeouts, "strikeouts")

df_odds_doubles = odds_data.odds_doubles.fetch_df_doubles_odd_today()
print(f'df_odds_doubles {df_odds_doubles}')
upload_df_odds_to_gcs(df_odds_doubles, "doubles")

df_odds_stolenbases = odds_data.odds_stolenbases.fetch_df_stolenbases_odd_today()
print(f'df_odds_stolenbases {df_odds_stolenbases}')
upload_df_odds_to_gcs(df_odds_stolenbases, "stolenbases")

