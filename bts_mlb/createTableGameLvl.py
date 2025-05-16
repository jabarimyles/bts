
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


with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path


# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

def get_game_lvl(prod=False, table_dict={}):

    if prod == False:
        orig_data = read_csv_from_gcs('bts-mlb','statcast.csv')
    else:
        orig_data = read_csv_from_gcs('bts-mlb','statcast.csv')
    orig_data = orig_data.loc[orig_data['game_type']=='R']


    keep_cols = ['index', 'game_date', 'player_name', 'batter',
            'events', 'inning_topbot',
            'description',  'des',
            'game_type', 'stand','home_team', 'away_team', 'type',
            'game_year',  'game_pk', 'events_grouped']


    sub = ['pitcher', 'game_pk', 'inning', 'inning_topbot']
    keep_cols_p = sub + ['pitch_number']
    sp_info = orig_data[keep_cols_p]
    sp_info = sp_info.loc[sp_info['inning']==1.0].drop_duplicates()
    sp_info = sp_info.sort_values(by=['game_pk', 'pitch_number'], ascending=True)
    sp_info = sp_info.drop_duplicates(subset=['game_pk', 'inning_topbot'], keep='first')
    piv_cols= ['game_pk', 'pitcher', 'inning_topbot']
    sp_info = sp_info[piv_cols].rename(columns={'pitcher':'starting_pitcher'})

    data = orig_data[keep_cols]
    data = pd.get_dummies(data, columns=['events_grouped'], prefix ='', prefix_sep = '')
    print('--- data, game_date ---')
    print(data['game_date'].max())
    #data['at_bats'] = data.groupby(['batter','game_pk'])['at_bat_number'].transform(max)
    group_cols = ['game_year', 'batter','inning_topbot', 'game_pk','game_date','stand','home_team','away_team']
    game_data = data.groupby(group_cols).sum().reset_index()
    print('---- game_data ----')
    print(game_data['game_date'].max())

    game_data = game_data.rename(columns={'hit':'hits', 'non_ab':'non_abs', 'non_play':'non_plays', 'out':'outs'})
    game_data = pd.merge(game_data, sp_info, how='left', on=['game_pk', 'inning_topbot'])
    
    print('---- game_data ----')
    print(game_data['game_date'].max())
    if prod==True:
        write_csv_to_gcs(game_data, 'bts-mlb', 'GameLvl.csv')
        return game_data
    else:
        write_csv_to_gcs(game_data, 'bts-mlb', 'GameLvl.csv')
        return None


'''
def get_season_bat(start_date, end_date){

    data = batting_stats_range(start_date, end_date)
}
'''
