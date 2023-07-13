import datetime, os, json, math

import numpy as np
from google.cloud import bigquery
from google.cloud import storage
import collect_data.venue_game_temperatures
import statsapi

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

fetch_print_prefix = "\n### "

gs_bucket_name = "major-league-baseball"
gcp_project_id = "trading-290017"
bq_dataset_id = "major_league_baseball"
bq_table_id = "venue_game_temperature"
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


def write_venue_game_local_temp(venue_game_temperatures, b_i):
    date_today = datetime.datetime.today().strftime("%Y-%m-%d")
    json_file_name = f'update_data/temp/venue_game_temperatures_{date_today}_{b_i}.txt'
    with open(json_file_name, 'w') as jf:
        for location_datetime, game_temperatures in venue_game_temperatures.items():
            # "(39.90588575, -75.16541101747245, '2011-04-01T17:05:00Z')"
            datetime_str = location_datetime.replace(')', '').replace('(', '').replace("'", '').split(',')[-1].strip()
            dte = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').date()
            payload = {
                'location_datetime': location_datetime,
                'game_temperature': game_temperatures,
                'date': dte,
            }
            payload_str = json.dumps(payload, cls=NpEncoder)
            if 'nan' in payload_str.lower():
                continue
            jf.write(payload_str + '\n')

    return json_file_name


def upload_venue_game_temperatures_to_gcs(venue_game_temperatures):
    l = len(venue_game_temperatures)
    if l == 0:
        return
    n_batches = math.ceil(l / 1000.0)
    print(f'[upload_venue_game_temperatures_to_gcs] n_batches: {n_batches}, venue_game_temperatures: {l}')
    location_datetimes_batch = np.array_split(list(venue_game_temperatures.keys()), n_batches)

    for i, location_datetimes in enumerate(location_datetimes_batch):
        print(f'batch {i}, size: {len(location_datetimes)}')
        batch_file_name = write_venue_game_local_temp({location_datetime: venue_game_temperatures[location_datetime] for location_datetime in location_datetimes}, i)
        if batch_file_name is None:
            print('filed to create local batch for {i}')
            continue

        print(f'{batch_file_name}')
        json_file_name = batch_file_name

        # uoload the jsonfied data to gcs
        storage_client = storage.Client()
        if storage.Blob(bucket=storage_client.bucket(gs_bucket_name), name=json_file_name).exists(storage_client):
            print(f'{json_file_name} already present in the bucket {gs_bucket_name} thus not proceeding further')
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

def upload_venue_game_temperatures_to_gcs_between(start_date_str, end_date_str):
    print(f'upload_venue_game_temperatures_to_gcs_between {start_date_str} and {end_date_str}')

    schedules = statsapi.schedule(start_date = start_date_str, end_date = end_date_str)
    game_ids = [sc['game_id'] for sc in schedules]

    print(f'[upload_venue_game_temperatures_to_gcs_between] game_ids: {len(game_ids)}')
    venue_game_temperatures = collect_data.venue_game_temperatures.ingest_venue_game_temperatures_for_game_id_list(game_ids, schs={schedule['game_id']: schedule for schedule in schedules})
    upload_venue_game_temperatures_to_gcs(venue_game_temperatures)

    print(f'done upload_venue_game_temperatures_to_gcs_between {start_date_str} to {end_date_str}')
    return venue_game_temperatures

def upload_venue_game_temperatures_to_gcs_ndays_prior(days):
    date_ndays_prior = (datetime.datetime.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    print(f'upload_venue_game_temperatures_to_gcs_ndays_prior days: {days}, {date_ndays_prior}')
    ret = upload_venue_game_temperatures_to_gcs_between(date_ndays_prior, date_ndays_prior)
    print(f'done upload_venue_game_temperatures_to_gcs_ndays_prior {date_ndays_prior}')
    return ret

def upload_venue_game_temperatures_to_gcs_yesterday():
    return upload_venue_game_temperatures_to_gcs_ndays_prior(1)

def upload_venue_game_temperatures_to_gcs_2days_prior():
    return upload_venue_game_temperatures_to_gcs_ndays_prior(2)

def read_venue_game_temperatures_bq(start_date_str, end_date_str):
    print(f'read_venue_game_temperatures_bq {start_date_str} and {end_date_str}')

    query = f"""
        SELECT location_datetime, game_temperature 
        FROM `trading-290017.major_league_baseball.venue_game_temperature`
        where date >= "{start_date_str}" AND date <= "{end_date_str}"
        """
    print(f'running bq query\b{query}')

    query_job = _bq_client.query(query)
    rows = query_job.result()  # Waits for query to finish
    venue_game_temperatures = {row.location_datetime: row.game_temperature for row in rows}
    print(f'done read_venue_game_temperatures_bq {start_date_str} to {end_date_str}')

    return venue_game_temperatures
