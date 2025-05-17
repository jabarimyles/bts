import os
import pickle
import time
import requests
from tqdm import tqdm
from pybaseball import playerid_reverse_lookup
import pandas as pd
from gcs_helpers import *

from google.cloud import storage

import tempfile
import json

# Your service account JSON string from an env var or secret manager
service_account_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])

# Write it to a temporary file
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path


# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path


save_dir = 'images'
#os.makedirs(save_dir, exist_ok=True)

def get_mlb_headshot_url(mlbam_id):
    """Generate MLB headshot URL for a given MLBAM ID."""
    return f"https://midfield.mlbstatic.com/v1/people/{mlbam_id}/spots/120"

def download_image_locally(url, save_path):
    """Download an image from URL and save it locally."""
    try:
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(res.content)
        print(f"Saved image to {save_path}")
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

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
    img_path = os.path.join('/Users/jabarimyles/Documents/bts-mlb/bts_mlb/static/images', f"{mlbam_id}.jpg")
    # Skip if the image already exists
    if os.path.exists(img_path):
        continue
    
    # Generate the URL for the player's headshot
    img_url = get_mlb_headshot_url(mlbam_id)
    print(f"Fetching image for MLBAM ID {mlbam_id} from {img_url}")

    # Download and save the image
    download_image_locally(img_url, img_path)
    download_image_to_gcs(img_url, 'bts-mlb', img_path)

    # Add a delay to prevent hitting rate limits
    time.sleep(3)  # Adjust the sleep time as needed (e.g., 1-2 seconds)
