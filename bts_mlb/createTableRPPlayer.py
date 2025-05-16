#-- Base packages
import os
from tqdm import tqdm
import copy
import time

#-- Pypi packages
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 100)
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

#-- Custom packages


def get_rpplayer(rp_period, file_name_out, pitch_or_bat, prod=False, table_dict={}):
    if prod == False:
        data = read_csv_from_gcs('bts-mlb','statcast.csv')
    else:
        data = read_csv_from_gcs('bts-mlb','statcast.csv')

    game_date_df = data[['game_date', 'game_pk', pitch_or_bat, 'events_grouped']] # , 'md = pd.read_csv()'
    events_grouped_dummy = pd.get_dummies(game_date_df['events_grouped'])
    game_date_df = pd.concat([game_date_df, events_grouped_dummy], axis=1)

    rm_cols = game_date_df.drop(['events_grouped'], axis=1)
    game_lvl=pd.pivot_table(rm_cols, index=['game_date', pitch_or_bat, 'game_pk'], values=['hit', 'non_ab', 'out'],aggfunc=np.sum).reset_index()


    '''
        fill in missing dates for each player
        dummy variables for events_grouped
        rolling sum for each
    '''
    in_season_dates = game_date_df['game_date'].drop_duplicates()
    batter_rp = pd.DataFrame(columns=['game_date', pitch_or_bat])
    for bat in tqdm(game_date_df[pitch_or_bat].unique()):
        _ = pd.DataFrame(columns=['game_date', pitch_or_bat])
        _['game_date'] = in_season_dates
        _[pitch_or_bat] = bat
        batter_rp = pd.concat([batter_rp, _], ignore_index=True)
    full_dates = pd.merge(batter_rp, game_lvl, how='left', on=['game_date', pitch_or_bat])
    full_dates = full_dates.sort_values(by=[pitch_or_bat, 'game_date'], ascending=True)
    #full_dates['hit'] = full_dates['hit'].fillna(0)
    #full_dates['out'] = full_dates['out'].fillna(0)
    #full_dates['non_ab'] = full_dates['non_ab'].fillna(0)
    full_dates['games_played'] = np.where(full_dates['game_pk'].isna(), 0, 1)
    full_dates['year'] = full_dates['game_date'].str[0:4]
    #full_dates['cur_rp_hits'] = full_dates.groupby([pitch_or_bat, 'year'])['hit'].transform(lambda s: s.rolling(rp_period, min_periods=1).sum())
    #full_dates['cur_rp_outs'] = full_dates.groupby([pitch_or_bat, 'year'])['out'].transform(lambda s: s.rolling(rp_period, min_periods=1).sum())
    #full_dates['cur_rp_non_abs'] = full_dates.groupby([pitch_or_bat, 'year'])['non_ab'].transform(lambda s: s.rolling(rp_period, min_periods=1).sum())
    #full_dates['cur_rp_games_played'] = full_dates.groupby([pitch_or_bat, 'year'])['games_played'].transform(lambda s: s.rolling(rp_period, min_periods=1).sum())
    #full_dates['rp_hits'] = full_dates['cur_rp_hits'] - full_dates['hit']
    #full_dates['rp_outs'] = full_dates['cur_rp_outs'] - full_dates['out']
    #full_dates['rp_non_abs'] = full_dates['cur_rp_non_abs'] - full_dates['non_ab']
    #full_dates['rp_games_played'] = full_dates['cur_rp_games_played'] - full_dates['games_played']
    full_dates['rp_outs'] = full_dates.groupby([pitch_or_bat, 'year'])['out'].transform(lambda s: s.shift(1).rolling(rp_period, min_periods=1).sum())
    full_dates['rp_non_abs'] = full_dates.groupby([pitch_or_bat, 'year'])['non_ab'].transform(lambda s: s.shift(1).rolling(rp_period, min_periods=1).sum())
    full_dates['rp_games_played'] = full_dates.groupby([pitch_or_bat, 'year'])['games_played'].transform(lambda s: s.shift(1).rolling(rp_period, min_periods=1).sum())
    full_dates['rp_hits'] = full_dates.groupby([pitch_or_bat, 'year'])['hit'].transform(lambda s: s.shift(1).rolling(rp_period, min_periods=1).sum())
    full_dates['rp_hits_var'] = full_dates.groupby([pitch_or_bat, 'year'])['hit'].transform(lambda s: s.shift(1).rolling(rp_period, min_periods=1).var())

    if prod==False:
        write_csv_to_gcs(full_dates, 'bts-mlb', file_name_out)
    else:
        write_csv_to_gcs(full_dates, 'bts-mlb', file_name_out)
    return None
''' -- To delete
    batter_rp = game_date_df[['game_date', 'batter', 'rp_start_date']].drop_duplicates()
    print('dropping duplicates...')
    batter_rp['hits'] = 0
    batter_rp['outs'] = 0
    batter_rp['non_abs'] = 0
    batter_rp['games_played'] = 0
    for i, r in tqdm(batter_rp.iterrows(), total=batter_rp.shape[0]):
        d = game_date_df.loc[(game_date_df['game_date']<=r['game_date']) & (game_date_df['game_date']>=r['rp_start_date']) & (game_date_df['batter'] == r['batter']) ]
        counts = d['events_grouped'].value_counts()
        batter_rp.loc[i, 'hits'] = counts['hit'] if 'hit' in counts.keys() else 0
        batter_rp.loc[i, 'outs'] = counts['out'] if 'out' in counts.keys() else 0
        batter_rp.loc[i, 'non_abs'] = counts['non_ab'] if 'non_ab' in counts.keys() else 0
        batter_rp.loc[i, 'games_played'] = len(d['game_pk'].unique())
    batter_rp.to_csv('./data/RPBatter.csv', index=False)
    return None
'''
