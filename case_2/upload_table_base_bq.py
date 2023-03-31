from google.cloud import bigquery
from google.cloud import storage
# from google.oauth2 import service_account
# from google.cloud import bigquery_storage

import os

credential_path = "C:\paulo.alcantara\Documents\ProjetosVSC\GoogleAds\B2w-bee-u-dados-e-insights-prd-8f576d661bd8.json"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

client_BigQuery = bigquery.Client()

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def bigquery_upload(table_id, uri):
    # Configure the external data source

    job_config = bigquery.LoadJobConfig(autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    time_partitioning=bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="date",  # Name of the column to use for partitioning.
    #expiration_ms=7776000000,  # 90 days.
    ),
    )

    load_job = client_BigQuery.load_table_from_uri(
    uri, table_id, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = client_BigQuery.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


upload_blob(
    bucket_name="equipe_dados",
    source_file_name='union_bases.csv',
    destination_blob_name='case_gb/union_bases.csv'
)

bigquery_upload(
    bigquery_client=client_BigQuery,
    table_id="b2w-bee-u-dados-e-insights-stg.Case_gb.table_base",
    uri="gs://equipe_dados/case_gb/union_bases.csv"
)