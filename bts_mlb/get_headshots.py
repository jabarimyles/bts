import os
import pickle
import time
import requests
from tqdm import tqdm
from pybaseball import playerid_reverse_lookup
import pandas as pd

# Load your data
with open('./data/table_dict.pickle', 'rb') as f:
    td = pickle.load(f)

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

save_dir = './static/images'
os.makedirs(save_dir, exist_ok=True)

def get_mlb_headshot_url(mlbam_id):
    """Generate MLB headshot URL for a given MLBAM ID."""
    return f"https://midfield.mlbstatic.com/v1/people/{mlbam_id}/spots/120"

def download_image(url, save_path):
    """Download and save the image to the specified path."""
    try:
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(res.content)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

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
    download_image(img_url, img_path)

    # Add a delay to prevent hitting rate limits
    time.sleep(3)  # Adjust the sleep time as needed (e.g., 1-2 seconds)
