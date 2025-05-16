
#-- Base packages
import os

#-- Pypi packages
import pandas as pd
pd.set_option('display.max_columns', 100)
from statcast import *
from .gcs_helpers import *

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
