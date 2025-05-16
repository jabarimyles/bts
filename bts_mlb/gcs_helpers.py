import os
import sys
import pickle
from google.cloud import storage
import io
from io import BytesIO
import pandas as pd

import tempfile
import json

cred_path = "/tmp/service_account.json"
with open(cred_path, "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS_JSON"])

# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

def read_csv_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data))

def write_csv_to_gcs(df, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

def upload_pickle_to_gcs(bucket_name, destination_blob_name, obj):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Serialize object to bytes
    pickle_buffer = BytesIO()
    pickle.dump(obj, pickle_buffer)
    pickle_buffer.seek(0)

    # Upload to GCS
    blob.upload_from_file(pickle_buffer, content_type='application/octet-stream')
    print(f"Pickle file uploaded to gs://{bucket_name}/{destination_blob_name}")

def download_pickle_from_gcs(bucket_name, source_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download blob content as bytes
    pickle_buffer = BytesIO()
    blob.download_to_file(pickle_buffer)
    pickle_buffer.seek(0)

    # Deserialize pickle object
    obj = pickle.load(pickle_buffer)
    return obj