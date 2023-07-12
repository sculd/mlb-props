'''
import collect_data.game_id_lists

_game_id_lists = collect_data.game_id_lists._game_id_lists
game_id_list_2023 = collect_data.game_id_lists.get_game_id_list_year(2023)

import collect_data.game_matchup

df_game_matchup_updated = collect_data.game_matchup.get_df_game_matchup_for_game_id(286884)
print(df_game_matchup_updated)
'''

'''
import collect_data.boxscores
_boxscores = collect_data.boxscores._boxscores
'''

'''
import update_data.boxscores
boxscores = update_data.boxscores.read_boxscores_bq("2023-04-01", "2023-07-09")
print(f'boxscores: {len(boxscores)}')

import pickle
pickle.dump(boxscores, open('collect_data/boxscores_2023_0401_0709.pkl', 'wb'))
#'''

'''
import collect_data.player_stat
_player_stats = collect_data.player_stat._player_stats

import update_data.player_stat
update_data.player_stat.upload_player_stats_to_gcs(_player_stats)
'''


#'''
import collect_data.venue_game_temperatures
import update_data.venue_game_temperatures

_venue_game_temperatures = collect_data.venue_game_temperatures._venue_game_temperatures

update_data.venue_game_temperatures.upload_venue_game_temperatures_to_gcs(_venue_game_temperatures)

#venue_game_temperatures = update_data.venue_game_temperatures.upload_venue_game_temperatures_to_gcs_between("2023-07-07", "2023-07-07")

#venue_game_temperatures = update_data.venue_game_temperatures.read_venue_game_temperatures_bq("2023-07-09", "2023-07-09")
#print(f'venue_game_temperatures: {len(venue_game_temperatures)}')
#'''


'''
from google.cloud import bigquery
import os

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "venue_game_temperature_copy"
table_id = f'{gcp_project_id}.{bq_dataset_id}.{bq_table_id}'

_bq_client = bigquery.Client()

# load the jsonfied data to bq
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("location_datetime", "STRING", "REQUIRED"),
        bigquery.SchemaField("game_temperature", "FLOAT", "REQUIRED"),
        bigquery.SchemaField("date", "DATE", "REQUIRED"),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

json_file_name = 'update_data/temp/venue_game_temperatures_2023-07-12_14.txt.aa'

# ingest to bq
load_job = _bq_client.load_table_from_uri(
    f"gs://{gs_bucket_name}/{json_file_name}",
    table_id,
    location="US",  # Must match the destination dataset location.
    job_config=job_config
)
load_job.result()  # Waits for the job to complete.
'''