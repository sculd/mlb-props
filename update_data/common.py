import datetime, os, json, math

from google.cloud import bigquery
from google.cloud import storage
import numpy as np

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

gs_bucket_name = "major-league-baseball"

_storage_client = storage.Client()
_bq_client = bigquery.Client()

def upload_file_to_gcs(filename, rewrite=False):
    # uoload the file to gcs
    if storage.Blob(bucket=_storage_client.bucket(gs_bucket_name), name=filename).exists(_storage_client):
        if rewrite:
            bucket = _storage_client.bucket(gs_bucket_name)
            blob = bucket.blob(filename)
            blob.delete(if_generation_match=None)
        else:
            print(f'{filename} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
            return False

    bucket = _storage_client.bucket(gs_bucket_name)
    blob = bucket.blob(filename)
    generation_match_precondition = 0
    blob.upload_from_filename(filename, if_generation_match=generation_match_precondition)

    print(
        f"File {filename} uploaded to {filename}."
    )
    return True

def upload_newline_delimited_json_file_to_gcs_then_import_bq(json_filename, bq_table_id, bq_schema, rewrite=False):
    if not upload_file_to_gcs(json_filename, rewrite=rewrite):
        return

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # ingest to bq
    load_job = _bq_client.load_table_from_uri(
        f"gs://{gs_bucket_name}/{json_filename}",
        bq_table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.
    print(f'bq import from {json_filename} done')

def upload_pkl_file_to_gcs(pkl_filename, rewrite=False):
    upload_file_to_gcs(pkl_filename, rewrite=rewrite)

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
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)
