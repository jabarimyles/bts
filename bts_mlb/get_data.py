#-- Base packages
import os
import sys
from datetime import date
from gcs_helpers import *

#-- Pypi packages
import pandas as pd
pd.set_option('display.max_columns', 100)
from statcast import *

#-- Custom packages
from createTablePlayerMeta import get_player_meta


def get_statcast(sd, ed, table_dict={}, prod=True, append=True):
    try:
        old_df = read_csv_from_gcs('bts-mlb','statcast.csv')
        return old_df
    except:
        tries = 0
        while True:
            try:
                data = statcast(start_dt=sd, end_dt=ed)
                break
            except  pd.errors.ParserError:
                tries += 1
                if tries > 10:
                    raise ValueError('Tried exceeded 10')
                continue

        keep_cols = [
                'index', 'game_date', 'player_name', 'batter', 'pitcher',
            'events',
            'description',  'des',
            'game_type', 'stand', 'p_throws', 'home_team', 'away_team', 'type',
            'hit_location',  'game_year',  'game_pk', 'pitcher', 'pitch_number', 'pitch_name',
            'home_score', 'away_score', 'inning', 'inning_topbot',
            'launch_speed_angle', 'release_speed', 'pitch_type',
            'zone', 'release_spin_rate', 'launch_speed', 'launch_angle',
            'balls', 'strikes'
        ]
        data = data[keep_cols]

        if len(table_dict.keys()) != 0:
            todays_matchups = table_dict['todays_players']
            #todays_matchups['p_throws'] ='R'
            #todays_matchups['stand'] ='R'
            #-- read in player_id mapping
            keep_cols = ['MLBID', 'BATS', 'THROWS']
            rename_cols = {'BATS': 'stand', 'THROWS':'p_throws'}
            meta = read_csv_from_gcs('bts-mlb','player_id_mapping.csv')
            meta = meta[keep_cols].rename(columns=rename_cols)
            meta['stand'] = meta['stand'].replace({'B': 'S'})
            todays_matchups = pd.merge(todays_matchups, meta[['MLBID', 'p_throws']], how='left', left_on='pitcher', right_on='MLBID')
            todays_matchups = pd.merge(todays_matchups, meta[['MLBID', 'stand']], how='left', left_on='batter', right_on='MLBID')

            #meta = get_player_meta(table_dict={'statcast': data})
            todays_matchups.loc[(todays_matchups['stand']=='S')&(todays_matchups['p_throws']=='L'), 'stand'] = 'R'
            todays_matchups.loc[(todays_matchups['stand']=='S')&(todays_matchups['p_throws']=='R'), 'stand'] = 'L'

            today = date.today()
            todays_matchups['game_date'] ='{:02d}-{:02d}-{:02d} 00:00:00'.format(today.year, today.month, today.day)
            todays_matchups['game_year'] = float(today.year)
            todays_matchups['game_type'] = 'R'
            todays_matchups['inning']=1.0
            todays_matchups['pitch_number'] = 1
            todays_matchups['inning_topbot'] = 'Top'
            todays_matchups['home_team'] = 'BOS'
            todays_matchups['away_team'] = 'NYY'
            data = data.loc[:, ~data.columns.duplicated()]
            todays_matchups = todays_matchups.loc[:, ~todays_matchups.columns.duplicated()]
            data = data._append(todays_matchups, ignore_index=True)

        hit_inds = ['single', 'double', 'triple', 'home_run']
        out_ind = ['field_out', 'strikeout',
            'grounded_into_double_play',
            'force_out', 'fielders_choice_out',
            'field_error', 'fielders_choice',
            'double_play']
        noab_ind = [ 'walk', 'hit_by_pitch',
                'sac_bunt', 'sac_fly',
                'intent_walk']
        place_holder = ['_placeholder_']

        #-- Events Grouped
        data['events_grouped'] = ""
        data.loc[data['events'].isin(hit_inds), 'events_grouped'] = 'hit'
        data.loc[data['events'].isin(out_ind), 'events_grouped'] = 'out'
        data.loc[data['events'].isin(noab_ind), 'events_grouped'] = 'non_ab'
        data.loc[data['events'].isin(place_holder), 'events_grouped'] = 'out'

        #-- Pitches Grouped
        fastballs = ['FA', 'FF', 'FT', 'FC', 'FS', 'SI', 'SF']
        offspeed = ['SL', 'CH', 'CB', 'CU', 'KC', 'KN', 'EP', 'SC', 'CS']
        other = ['UN' 'XX', 'PO', 'FO', 'IN']
        data['pitch_type_group'] = ''
        data.loc[data['pitch_type'].isin(fastballs), 'pitch_type_group'] = 'FASTBALL'
        data.loc[data['pitch_type'].isin(offspeed), 'pitch_type_group'] = 'OFFSPEED'
        data.loc[data['pitch_type'].isin(other), 'pitch_type_group'] = 'OTHER'


        #data.loc[data['events_grouped'] == "", 'events_grouped'] = 'non_play'
        data = data.loc[data['events_grouped'] != 'non_play']
        recent_performance_range = 15

        data['game_date'] = data['game_date'].astype(str)
        if append == True:
            old_df = read_csv_from_gcs('bts-mlb','statcast.csv')
            pd.concat([old_df, data], axis=0, ignore_index=True)
        write_csv_to_gcs(data, 'bts-mlb', 'statcast.csv')
        if prod==True:
            return data
        return None


if __name__ == '__main__':
    sd = '2022-04-07'
    ed = '2025-05-09'
    get_statcast(sd, ed, table_dict={}, prod=False, append=False)
