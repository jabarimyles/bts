#-- Base packages
import os
import sys
from datetime import date
import pickle
import math

#-- Pypi packages
import pandas as pd
pd.set_option('display.max_columns', 100)
from statcast import *

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
from get_data import get_statcast
from createTableRPPlayer import get_rpplayer
from createTableGameLvl import get_game_lvl
from createModelingData import get_modeling_data
from createTablePlayerMeta import get_player_meta
from createTableMatchups import get_matchups
from createTodaysMatchups import get_todays_matchups
from train_model import logistic
from enterDailyPreds import enter
from .gcs_helpers import *

from train_model import logistic



if __name__ == '__main__':

    try:
        train_or_prod = 'prod' #sys.argv[1]
    except IndexError:
        train_or_prod = 'train'
    if train_or_prod == 'train':
        start_date = '2014-01-01'
        end_date   = '2025-05-10'

        print('querying statcast data....')
        #get_statcast(start_date, end_date, append=False)
        print('creating rp_player data...')
        get_rpplayer(16, 'RPBatter.csv', 'batter', prod=False)
        get_rpplayer(300, 'YTDBatter.csv', 'batter', prod=False)
        get_rpplayer(20, 'RPPitcher.csv', 'pitcher', prod=False)
        get_rpplayer(300, 'YTDPitcher.csv', 'pitcher', prod=False)
        print('Creating player meta data...')
        get_player_meta()
        print('creating game level data...')
        get_game_lvl()
        print('get matchups...')
        get_matchups()

    elif train_or_prod == 'prod':
        start_date = '2024-03-01'
        #start_date needs to query current db to get max date + 1
        today = date.today()
        end_date   = '{}-{}-{}'.format(today.year, today.month, today.day)
        end_date = '2025-06-30'
        table_dict = {}

        print('getting todays matchups... from {} to {}'.format(start_date, end_date))
        table_dict['todays_players'] = get_todays_matchups()
        print('querying statcast data....')
        # TODO: try to read from gcs first, if not found, query statcast
        table_dict['statcast'] = get_statcast(sd=start_date, ed=end_date, prod=True, table_dict=table_dict, append=False)
        ##append todays records to statcast...

        print('creating rp_player data...')
        table_dict['rpbatter'] = get_rpplayer(16, 'RPBatter.csv', 'batter', prod=True, table_dict=table_dict)
        table_dict['ytdbatter'] = get_rpplayer(300, 'YTDBatter.csv', 'batter', prod=True, table_dict=table_dict)
        table_dict['rppitcher'] = get_rpplayer(20, 'RPPitcher.csv', 'pitcher', prod=True, table_dict=table_dict)
        table_dict['ytdpitcher'] = get_rpplayer(300, 'YTDPitcher.csv', 'pitcher', prod=True, table_dict=table_dict)
        print('Creating player meta data...')
        table_dict['player_meta'] = get_player_meta(table_dict=table_dict)
        print('creating game level data...')
        table_dict['gamelvl'] = get_game_lvl(prod=True, table_dict=table_dict)
        print('get matchups...')
        table_dict['matchups'] = get_matchups(prod=True, table_dict=table_dict)

        print('get prod data...')
        table_dict['prod_data'] = get_modeling_data(prod=True, table_dict=table_dict)
        # TODO: insert main_train.py code here
        x_train = read_csv_from_gcs('bts-mlb','x_train.csv')
        y_train = read_csv_from_gcs('bts-mlb','y_train.csv')
        id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits', 'hit_ind']
        model = logistic(x_train.drop(id_vars, axis=1), y_train)
        model_name_file = 'model.pickle'
        upload_pickle_to_gcs('bts-mlb', model_name_file, model)
        #model = download_pickle_from_gcs('bts-mlb', 'model.pickle')

        #table_dict['todays_preds'] = table_dict['prod_data'].loc[table_dict['prod_data']['game_date']==table_dict['prod_data']['game_date'].max()]
        table_dict['todays_preds'] = table_dict['prod_data']
        id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits', 'hit_ind']
        print('--- checking cols ---')
        print(table_dict['todays_preds'].drop(id_vars, axis=1).columns)
        preds = model.predict_proba(table_dict['todays_preds'].drop(id_vars, axis=1))
        print(preds[0:10])
        proba_hit = [i[1] for i in preds]
        #print(proba_hit.head())
        #print(proba_hit.shape)
        #print(table_dict['todays_preds'].shape)
        table_dict['todays_preds']['proba'] = proba_hit
        upload_pickle_to_gcs('bts-mlb', 'table_dict.pickle', table_dict)
        coef_df = pd.DataFrame({'coef_': model.coef_[0].tolist()})
        #coef_df['input'] = list(table_dict['todays_preds'].drop(id_vars, axis=1).columns)
        #coef_df = coef_df.append({'coef_':model.intercept_[0], 'input': 'intercept'})
        #coef_df['prob_'] = coef_df['coef_'].apply(lambda x: math.exp(x) / (1+math.exp(x)))
        #



        keep_cols = ['MLBID', 'MLBNAME']
        meta = read_csv_from_gcs('bts-mlb','player_id_mapping.csv')
        meta = meta[keep_cols]
        table_dict['todays_preds'] = pd.merge(table_dict['todays_preds'], meta.rename(columns={'MLBNAME':'pitcher_name'}), how='left', left_on='starting_pitcher', right_on='MLBID')
        table_dict['todays_preds'] = pd.merge(table_dict['todays_preds'], meta.rename(columns={'MLBNAME':'batter_name'}), how='left', left_on='batter', right_on='MLBID')

        print(table_dict['todays_preds']['proba'].head())
        name_file = './data/table_dict' + '.pickle'
        upload_pickle_to_gcs('bts-mlb', name_file, table_dict)
        '''
        print(table_dict['todays_preds']['game_date'].value_counts())
        table_dict['todays_preds'] = table_dict['todays_preds'].loc[~table_dict['todays_preds']['game_date'].isna()]
        max_date = table_dict['todays_preds']['game_date'].max()
        print(max_date)
        table_dict['todays_preds'] = table_dict['todays_preds'].loc[table_dict['todays_preds']['game_date']==max_date]
        print(table_dict['todays_preds'].head())
        print(table_dict['todays_preds'].shape)
        table_dict['todays_preds'] = table_dict['todays_preds'].sort_values('proba', ascending=False)
        print(table_dict['todays_preds'].shape)
        print(table_dict['todays_preds'].shape)
        print(table_dict['todays_preds'][['batter', 'proba']].head())
        table_dict['todays_preds']['batter'] = table_dict['todays_preds']['batter'].astype(int).astype(str).head()
        pick1 = table_dict['todays_preds']['batter'].iloc[0]
        pick2 = table_dict['todays_preds']['batter'].iloc[1]
        print(pick1)
        print(pick2)
        enter(pick1, pick2)
        '''


        #preditc_data_comb.to_sql()
