import datetime, os, json, math

from google.cloud import bigquery
from google.cloud import storage

if os.path.exists('credential.json'):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

gs_bucket_name = "major-league-baseball"

_storage_client = storage.Client()
_bq_client = bigquery.Client()


def upload_newline_delimited_json_file_to_gcs_then_import_bq(json_filename, bq_table_id, bq_schema, rewrite=False):
    # uoload the file to gcs
    if not rewrite and storage.Blob(bucket=_storage_client.bucket(gs_bucket_name), name=json_filename).exists(_storage_client):
        print(f'{json_filename} already present in the bucket {gs_bucket_name} thus not proceeding further for {property}')
        return

    bucket = _storage_client.bucket(gs_bucket_name)
    blob = bucket.blob(json_filename)

    generation_match_precondition = 0
    blob.upload_from_filename(json_filename, if_generation_match=generation_match_precondition)

    print(
        f"File {json_filename} uploaded to {json_filename}."
    )

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
