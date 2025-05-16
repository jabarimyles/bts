#-- Base packages
import os
import sys
from datetime import date
from .gcs_helpers import *


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
