
#-- Base packages
import os

#-- Pypi packages
import pandas as pd
pd.set_option('display.max_columns', 100)
from statcast import *
from .gcs_helpers import *

import tempfile
import json

cred_path = "/tmp/service_account.json"
with open(cred_path, "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS_JSON"])

# Set the environment variable for Google auth
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

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
