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
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from gcs_helpers import *
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
        start_date = '2025-03-01'
        #start_date needs to query current db to get max date + 1
        today = date.today()
        end_date   = '{}-{}-{}'.format(today.year, today.month, today.day)
        table_dict = {}

        print('getting todays matchups... from {} to {}'.format(start_date, end_date))
        table_dict['todays_players'] = get_todays_matchups()
        print('querying statcast data....')
        # TODO: try to read from gcs first, if not found, query statcast
        table_dict['statcast'] = get_statcast(sd=start_date, ed=end_date, prod=True, table_dict=table_dict, append=False, today=today)
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
        table_dict['prod_data'], _ ,_ , _, _ = get_modeling_data(prod=True, table_dict=table_dict)
        # TODO: insert main_train.py code here
        x_train = read_csv_from_gcs('bts-mlb','x_train.csv')
        y_train = read_csv_from_gcs('bts-mlb','y_train.csv')
        id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits', 'hit_ind']
        try:
            model = download_pickle_from_gcs('bts-mlb', 'model.pickle')
        except:
            model = logistic(x_train.drop(id_vars, axis=1), y_train)
            model_name_file = 'model.pkl'
            upload_pickle_to_gcs('bts-mlb', model_name_file, model)

        table_dict['todays_preds'] = table_dict['prod_data']
        id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits', 'hit_ind']
        print('--- checking cols ---')
        preds = model.predict_proba(table_dict['todays_preds'].drop(id_vars, axis=1))
        print(preds[0:10])
        proba_hit = [i[1] for i in preds]
        table_dict['todays_preds']['proba'] = proba_hit
        upload_pickle_to_gcs('bts-mlb', 'table_dict.pickle', table_dict)
        coef_df = pd.DataFrame({'coef_': model.coef_[0].tolist()})



        keep_cols = ['MLBID', 'MLBNAME']
        meta = read_csv_from_gcs('bts-mlb','player_id_mapping.csv')
        meta = meta[keep_cols]
        table_dict['todays_preds'] = pd.merge(table_dict['todays_preds'], meta.rename(columns={'MLBNAME':'pitcher_name'}), how='left', left_on='starting_pitcher', right_on='MLBID')
        table_dict['todays_preds'] = pd.merge(table_dict['todays_preds'], meta.rename(columns={'MLBNAME':'batter_name'}), how='left', left_on='batter', right_on='MLBID')

        print(table_dict['todays_preds']['proba'].head())
        name_file = 'table_dict.pickle'
        upload_pickle_to_gcs('bts-mlb', name_file, table_dict)



        #preditc_data_comb.to_sql()
