
#-- Base packages
import os

#-- Pypi packages
import pandas as pd
pd.set_option('display.max_columns', 100)
from statcast import *
from .gcs_helpers import *

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

def get_player_meta(table_dict={}):
    if table_dict == {}:
        orig_data = read_csv_from_gcs('bts-mlb','statcast.csv')
    else:
        orig_data = table_dict['statcast']
    batter_cols = ['batter', 'game_date', 'inning_topbot']
    home_cols = ['home_team', 'away_team']
    batters = orig_data[batter_cols + home_cols + ['pitch_number']].sort_values(by=['game_date', 'pitch_number'], ascending=True).drop_duplicates()
    batters['cur_team'] = ""
    batters.loc[batters['inning_topbot']=='Bot', 'cur_team'] = batters.loc[batters['inning_topbot']=='Bot', 'home_team']
    batters.loc[batters['inning_topbot']=='Top', 'cur_team'] = batters.loc[batters['inning_topbot']=='Top', 'away_team']
    keep_cols = ['batter', 'cur_team', 'game_date']
    batters= batters[keep_cols]
    batters.groupby(['batter', 'cur_team'])['game_date'].first().reset_index()
    batters['pos'] = 'batter'
    stance = orig_data[['batter', 'stand']].drop_duplicates()
    print(stance['stand'].value_counts())
    stance['stand'] = stance['stand'].astype(str)
    stance = stance.groupby('batter')['stand'].unique().reset_index()
    stance['stand'] = stance['stand'].apply(lambda x: ", ".join(sorted(x)))
    stance['stand'] = stance['stand'].rename({'L, R': 'S', 'B': 'S'})
    batters = pd.merge(batters, stance, how='left', on='batter')
    batters = batters.rename(columns={'batter': 'player'})

    #--- Get DF with starting pitchers for each game to merge onto pitchers df
    starter_cols = ['game_pk', 'game_date', 'inning_topbot', 'pitcher', 'pitch_number']
    starter_ind_df = orig_data[starter_cols]
    starter_ind_df = starter_ind_df.groupby(['game_pk', 'inning_topbot'])[['game_pk', 'pitcher']].first()
    starter_ind_df['pitcher_pos'] = 'starter'

    pit_cols = ['pitcher', 'game_date','game_pk', 'inning_topbot', 'home_team', 'away_team', 'p_throws']
    pitchers = orig_data[pit_cols].sort_values('game_date', ascending=True).drop_duplicates()
    pitchers['cur_team'] = ""
    pitchers.loc[pitchers['inning_topbot']=='Bot', 'cur_team'] = pitchers.loc[pitchers['inning_topbot']=='Bot', 'away_team']
    pitchers.loc[pitchers['inning_topbot']=='Top', 'cur_team'] = pitchers.loc[pitchers['inning_topbot']=='Top', 'home_team']
    keep_cols = ['pitcher', 'cur_team', 'p_throws', 'game_date', 'game_pk']
    pitchers = pitchers[keep_cols]
    pitchers.groupby(['pitcher', 'cur_team', 'p_throws'])['game_date'].first().reset_index()
    pitchers['pos'] = 'pitcher'
    #pitchers = pitchers.rename(columns={'pitcher': 'player'})
    starter_ind_df = starter_ind_df.reset_index(level='game_pk', drop=True).reset_index()
    pitchers = pd.merge(pitchers, starter_ind_df, how='left', on=['game_pk', 'pitcher'])
    pitchers['pitcher_pos'] = pitchers['pitcher_pos'].fillna('bullpen') 

    players = pd.concat([batters, pitchers], axis=0, ignore_index=True)
    #if type(orig_data) == type(None):
    write_csv_to_gcs(players, 'bts-mlb', 'player_meta.csv')
    #else:
    #    return players
    return None
