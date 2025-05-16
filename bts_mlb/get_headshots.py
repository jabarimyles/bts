import os
import pickle
import time
import requests
from tqdm import tqdm
from pybaseball import playerid_reverse_lookup
import pandas as pd
from .gcs_helpers import *

from google.cloud import storage

import tempfile
import json

service_account_info = {
  "type": "service_account",
  "project_id": "artful-hexagon-459902-q1",
  "private_key_id": "aaa874f8affd2dfc867f0bb6dc7308a85f781dc5",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC2SIa1+JON4NIH\nkF5vtqSyxCnfCRbUDLW8GQOgVImKXTlP3P50hrTrNFtWTaNsfG24oz/Mz/SaSHpY\nD9Srnct1sIDus3IxIkBMa8Bzwm7abBobCBWjmnocv3FB8ptrwj+QE7/qFRutO7q3\nCKHUfzqvVgIixV8mU0e9hu7XoYJpfXji/VImmdRcnOtmfh885A1Sc/sog09bP/IR\nHtd6rvgI46VnNVqA4CyXWzwHMfAd/rP00GkE+p8cyeR2lRPTBa73vml0JiVH8qwu\nw7ec5eWsfVxHApt6rzEXIpZ8Tl/pdfTB2TSGS+xExhrLQlK9PkiGeX7n8NJifkh2\nLBdYJxi7AgMBAAECggEARD8pOI5F6HPJDw3tXZQbW9b3+kpz4paToEYZRnkAOe6n\nW5BZMJWSvREQNWLCEgcQKXXtmCgv42fJbpkWvd5JY9nenABRe7XgLvyUxIKCcILS\nz1Yai/N1Trgall9X82N52t6aFvEqOJTJVmgD9wRfm2/vQsd01WuOy5XubItKwWWX\n5Y8CE9LExt8mTphvRvQPp31wNa520MGjc8jM/zItp3CbnUXRK4YSMzHVRH8PUUQv\n6EO4PqsKgRziMRh7So3tV4QpHq0gQRZnHD6ylvDCZX8TWJqtFsfMWnie39n/AM2O\n91cdmHcqSz2GKvBjy7GVlZ50738Lk36KxM8m6vdvcQKBgQDiCnT507BMTYG/p7TJ\nHdC3OzhWsZypU4EIKwqBMEcwrH6riRpuUp9T0eMIVXNyKgzsEvJZR5EjmFsTXdbt\nfDbAuGFPZ4yZVaHzKQzMOeX5hUCqHqnEPAb3lV400UEwMfeUs0xJ66xKa3hsVLYH\nLep/K+lyOKkkZum5FQ6ybUOjEwKBgQDOcWFcH5JrEZ8OLh9BXZvWl0zBjRCFt10M\nQD2fpYxWnPEqrOsflOSRWmn0rJjcftDaCrT7BqEhqi/JWFDY50EHix/fepLmw/l6\nYop5xc8iGc367sQOSSIdVZqCZxdBh0PzmsL+E6bFY/6Ci3ikSHtTV2lTeX83BHqG\nxK4NvyjAuQKBgQDYn/rg3Z3MUk8xNHDOeRNoNonUk5y2rb8v68fCbVkcbYNrsxYw\nemAU/UWd2/6qf2Ao8jNtmmee/Ej0M29h4zO52Dnx1iPpYya0mTeZlTcvvSNupbo+\nxORMa8p/xba6kHhb+sT25rQUEhCziS91i+x6ecPc4i4/I52D8YlHN+2lHwKBgQCR\n5kWFovaK3vhHQEdsneieP23KuJR9vDpxhxFGO+yz5dT3cR/2wPbM11ZcyoJ6CtI1\n1y1S37uPHEULinQQ51bpKuUKvwkFOGmfmfb92tPp6MzPVGGRKxSGINLC6HLiJ+PZ\nTX4TrPXHOUVNI57OlD88hmF00kAbNPoXNvc/1eLKWQKBgQCOL1HaQT3ebGdV4lSU\ndKMkTrRYmz9n3FRUIkhiwAMeaPth+oyBap/vmR6Llv9qHQTozYMvh9MKEqKgLidk\nxUvkF5WkGHQaACZuxw5lecXts46nFG79C/3qCiGhJoCltnQMSFRbtnUUtWYF/Ikl\n2ivVX+Z4l0dn+o0pbYrEp9C3UQ==\n-----END PRIVATE KEY-----\n",
  "client_email": "bts-storage-rw@artful-hexagon-459902-q1.iam.gserviceaccount.com",
  "client_id": "113325116033005552068",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bts-storage-rw%40artful-hexagon-459902-q1.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path


# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path


save_dir = 'images'
#os.makedirs(save_dir, exist_ok=True)

def get_mlb_headshot_url(mlbam_id):
    """Generate MLB headshot URL for a given MLBAM ID."""
    return f"https://midfield.mlbstatic.com/v1/people/{mlbam_id}/spots/120"

def download_image_to_gcs(url, bucket_name, blob_path):
    """Download an image from URL and upload it to GCS."""
    try:
        res = requests.get(url, timeout=5)
        res.raise_for_status()

        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Upload image bytes directly
        blob.upload_from_file(BytesIO(res.content), content_type=res.headers.get('Content-Type', 'image/jpeg'))

        print(f"Image saved to gs:// + {blob}")
        return True
    except Exception as e:
        print(f"Failed to download or upload {url}: {e}")
        return False


td = download_pickle_from_gcs('bts-mlb', 'table_dict.pickle')

# Get unique batter and pitcher IDs
bat_ids = list(pd.Series(td['statcast']['batter'].unique()).dropna().astype(int).unique())
pit_ids = list(pd.Series(td['statcast']['pitcher'].unique()).dropna().astype(int).unique())
ids = list(set(bat_ids + pit_ids))

# Map MLBAM IDs to Baseball-Reference IDs
print("Looking up Baseball-Reference IDs...")
try:
    mapping_df = playerid_reverse_lookup(ids)
except Exception as e:
    print("Error with pybaseball reverse lookup:", e)
    exit()
# Loop through player IDs and download headshot images
for _, row in tqdm(mapping_df.iterrows(), total=mapping_df.shape[0], desc="Downloading images"):
    mlbam_id = int(row['key_mlbam'])
    img_path = os.path.join(save_dir, f"{mlbam_id}.jpg")
    
    # Skip if the image already exists
    if os.path.exists(img_path):
        continue
    
    # Generate the URL for the player's headshot
    img_url = get_mlb_headshot_url(mlbam_id)
    print(f"Fetching image for MLBAM ID {mlbam_id} from {img_url}")

    # Download and save the image
    download_image_to_gcs(img_url, 'bts-mlb', img_path)

    # Add a delay to prevent hitting rate limits
    time.sleep(3)  # Adjust the sleep time as needed (e.g., 1-2 seconds)
