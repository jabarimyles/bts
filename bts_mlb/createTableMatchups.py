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


with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path


# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
#-- Custom packages


def get_matchups(prod=False, table_dict={}):
    if table_dict == {}:
        data = read_csv_from_gcs('bts-mlb','statcast.csv')
    else:
        data = table_dict['statcast']

    game_date_df = data[['game_date', 'game_pk', 'batter', 'pitcher', 'events_grouped']]
    events_grouped_dummy = pd.get_dummies(game_date_df['events_grouped'])
    game_date_df = pd.concat([game_date_df, events_grouped_dummy], axis=1)

    rm_cols = game_date_df.drop(['events_grouped'], axis=1)
    game_lvl=pd.pivot_table(rm_cols, index=['game_date','batter', 'pitcher', 'game_pk'], values=['hit', 'non_ab', 'out'],aggfunc=np.sum).reset_index()
    game_lvl = game_lvl.sort_values(by=['batter', 'pitcher', 'game_date'], ascending=True)
    game_lvl['year'] = game_lvl['game_date'].str[0:4]
    game_lvl['game_ind'] = 1
    game_lvl['year_hits'] = game_lvl.groupby(['batter', 'pitcher', 'year'])['hit'].transform(lambda s: s.shift(1).rolling(300, min_periods=1).sum())
    game_lvl['year_outs'] = game_lvl.groupby(['batter', 'pitcher', 'year'])['out'].transform(lambda s: s.shift(1).rolling(300, min_periods=1).sum())
    game_lvl['year_non_abs'] = game_lvl.groupby(['batter', 'pitcher', 'year'])['non_ab'].transform(lambda s: s.shift(1).rolling(300, min_periods=1).sum())
    game_lvl['year_games_played'] = game_lvl.groupby(['batter', 'pitcher', 'year'])['game_ind'].transform(lambda s: s.shift(1).rolling(300, min_periods=1).sum())

    game_lvl['career_hits'] = game_lvl.groupby(['batter', 'pitcher'])['hit'].transform(lambda s: s.shift(1).rolling(365*20, min_periods=1).sum())
    game_lvl['career_outs'] = game_lvl.groupby(['batter', 'pitcher'])['out'].transform(lambda s: s.shift(1).rolling(365*20, min_periods=1).sum())
    game_lvl['career_non_abs'] = game_lvl.groupby(['batter', 'pitcher'])['non_ab'].transform(lambda s: s.shift(1).rolling(365*20, min_periods=1).sum())
    game_lvl['career_games_played'] = game_lvl.groupby(['batter', 'pitcher'])['game_ind'].transform(lambda s: s.shift(1).rolling(365*20, min_periods=1).sum())

    if prod==True:
        write_csv_to_gcs(game_lvl, 'bts-mlb', 'matchups.csv')
        return game_lvl
    
    return None
