#-- Base packages
import os
import sys
import copy

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

#-- Functions
def get_modeling_data(prod=False, table_dict={}):

    if prod == False:
        game_lvl = read_csv_from_gcs('bts-mlb','GameLvl.csv')
    else:
        game_lvl = read_csv_from_gcs('bts-mlb','GameLvl.csv')
    game_lvl['PAs'] = game_lvl['hits'] + game_lvl['outs'] + game_lvl['non_abs']
    game_lvl['ABs'] = game_lvl['hits'] + game_lvl['outs']
    game_lvl['hit_ind'] = np.where( game_lvl['hits'] > 0, 1,0)


    if prod == False:
        rp_batter = read_csv_from_gcs('bts-mlb','RPBatter.csv')
        ytd_batter = read_csv_from_gcs('bts-mlb', 'YTDBatter.csv')
    else:
        rp_batter = read_csv_from_gcs('bts-mlb','RPBatter.csv')
        ytd_batter = read_csv_from_gcs('bts-mlb', 'YTDBatter.csv')

    rename_cols = {
        'rp_hits': 'ytd_hits',
        'rp_outs': 'ytd_outs',
        'rp_non_abs': 'ytd_non_abs',
        'rp_hits_var': 'ytd_hits_var',
        'rp_games_played': 'ytd_games_played',
    }
    keep_cols = ['game_pk', 'batter'] + list(rename_cols.values())
    ytd_batter = ytd_batter.rename(columns=rename_cols)
    ytd_batter = ytd_batter[keep_cols]
    game_lvl = pd.merge(game_lvl, ytd_batter, how='left', on=['game_pk', 'batter'])
    game_lvl['ytd_PAs'] = game_lvl['ytd_hits'] + game_lvl['ytd_non_abs'] + game_lvl['ytd_outs']
    game_lvl['ytd_ABs'] = game_lvl['ytd_hits'] + game_lvl['ytd_outs']
    game_lvl['ytd_AB_div_PA'] = (game_lvl['ytd_ABs'] / game_lvl['ytd_PAs']).round(3)
    game_lvl['ytd_BA'] = (game_lvl['ytd_hits'] / game_lvl['ytd_ABs']).round(3)


    if prod == False:
        rp_sp = read_csv_from_gcs('bts-mlb','RPPitcher.csv')
        ytd_sp = read_csv_from_gcs('bts-mlb','YTDPitcher.csv')
    else:
        rp_sp = read_csv_from_gcs('bts-mlb','RPPitcher.csv')
        ytd_sp = read_csv_from_gcs('bts-mlb','YTDPitcher.csv')

    rename_cols = {
        'rp_hits': 'rp_hits_sp',
        'rp_outs': 'rp_outs_sp',
        'rp_non_abs': 'rp_non_abs_sp',
        'rp_games_played': 'rp_games_played_sp',
        'rp_hits_var': 'rp_hits_var_sp',
        'pitcher': 'starting_pitcher'
    }
    keep_cols = ['game_pk'] + list(rename_cols.values())
    rp_sp = rp_sp.rename(columns=rename_cols)
    rp_sp = rp_sp[keep_cols]
    game_lvl = pd.merge(game_lvl, rp_sp, how='left', on=['game_pk', 'starting_pitcher'])
    game_lvl['rp_PAs_sp'] = game_lvl['rp_hits_sp'] + game_lvl['rp_outs_sp'] + game_lvl['rp_non_abs_sp']
    game_lvl['rp_ABs_sp'] = game_lvl['rp_hits_sp'] + game_lvl['rp_outs_sp']
    game_lvl['rp_AB_div_PA_sp'] = (game_lvl['rp_ABs_sp'] / game_lvl['rp_PAs_sp']).round(3)
    game_lvl['rp_BA_sp'] = (game_lvl['rp_hits_sp'] / game_lvl['rp_ABs_sp']).round(3)

    rename_cols = {
        'rp_hits': 'ytd_hits_sp',
        'rp_outs': 'ytd_outs_sp',
        'rp_non_abs': 'ytd_non_abs_sp',
        'rp_games_played': 'ytd_games_played_sp',
        'rp_hits_var': 'ytd_hits_var_sp',
        'pitcher': 'starting_pitcher'
    }
    keep_cols = ['game_pk'] + list(rename_cols.values())
    ytd_sp = ytd_sp.rename(columns=rename_cols)
    ytd_sp = ytd_sp[keep_cols]
    game_lvl = pd.merge(game_lvl, ytd_sp, how='left', on=['game_pk', 'starting_pitcher'])
    game_lvl['ytd_PAs_sp'] = game_lvl['ytd_hits_sp'] + game_lvl['ytd_outs_sp'] + game_lvl['ytd_non_abs_sp']
    game_lvl['ytd_ABs_sp'] = game_lvl['ytd_hits_sp'] + game_lvl['ytd_outs_sp']
    game_lvl['ytd_AB_div_PA_sp'] = (game_lvl['ytd_ABs_sp'] / game_lvl['ytd_PAs_sp']).round(3)
    game_lvl['ytd_BA_sp'] = (game_lvl['ytd_hits_sp'] / game_lvl['ytd_ABs_sp']).round(3)




    #-- deprecated
    #game_lvl['rp_end_date'] = game_lvl.apply(lambda x: rp_batter.loc[(rp_batter['batter']== x['batter']) & (rp_batter['game_date']<x['game_date']), 'game_date'].max(), axis=1)



    game_lvl = pd.merge(game_lvl, rp_batter, how='left', on=['game_pk', 'batter'])
    game_lvl['rp_PAs'] = game_lvl['rp_hits'] + game_lvl['rp_non_abs'] + game_lvl['rp_outs']
    game_lvl['rp_ABs'] = game_lvl['rp_hits'] + game_lvl['rp_outs']
    game_lvl['rp_AB_div_PA'] = (game_lvl['rp_ABs'] / game_lvl['rp_PAs']).round(3)
    game_lvl['rp_BA'] = (game_lvl['rp_hits'] / game_lvl['rp_ABs']).round(3)

    if prod == False:
        matchups = read_csv_from_gcs('bts-mlb','matchups.csv')
    else:
        matchups = read_csv_from_gcs('bts-mlb','matchups.csv')

    matchups['match_year_PAs'] = matchups['year_hits']+matchups['year_outs']+matchups['year_non_abs']
    matchups['match_year_ABs'] = matchups['year_hits']+matchups['year_outs']
    matchups['match_year_BA'] =  (matchups['year_hits'] / matchups['match_year_ABs']).round(3)
    matchups['match_year_AB_div_PA'] = (matchups['match_year_ABs'] / matchups['match_year_PAs']).round(3)
    matchups['match_career_PAs'] = matchups['career_hits']+matchups['career_outs']+matchups['career_non_abs']
    matchups['match_career_ABs'] = matchups['career_hits']+matchups['career_outs']
    matchups['match_career_BA'] =  (matchups['career_hits'] / matchups['match_career_ABs']).round(3)
    matchups['match_career_AB_div_PA'] = (matchups['match_career_ABs'] / matchups['match_career_PAs']).round(3)
    keep_cols = ['match_year_PAs', 'match_year_BA', 'match_year_AB_div_PA',
                'match_career_PAs', 'match_career_BA', 'match_career_AB_div_PA',
                'batter', 'pitcher', 'game_pk'
    ]
    matchups = matchups[keep_cols]
    left_on = ['batter', 'starting_pitcher', 'game_pk']
    right_on = ['batter', 'pitcher', 'game_pk']
    matchups = matchups.fillna(0)
    game_lvl_hold = copy.deepcopy(game_lvl)
    game_lvl = pd.merge(game_lvl, matchups, how='left', left_on=left_on, right_on=right_on)


    if prod == False:
        player_meta = read_csv_from_gcs('bts-mlb','player_meta.csv')
    else:
        player_meta = read_csv_from_gcs('bts-mlb','player_meta.csv')
    player_meta = player_meta.loc[player_meta['pos']=='pitcher', ['pitcher', 'p_throws']].drop_duplicates()
    #common_values = set(game_lvl['starting_pitcher']).intersection(set(player_meta['player']))
    #print(f"Number of values in common: {len(common_values)}")
    game_lvl = pd.merge(game_lvl, player_meta, left_on='starting_pitcher', right_on='pitcher')
    game_lvl.loc[(game_lvl['stand']=='S')&(game_lvl['p_throws']=='L'), 'stand'] = 'R'
    game_lvl.loc[(game_lvl['stand']=='S')&(game_lvl['p_throws']=='R'), 'stand'] = 'L'
    game_lvl['handedness_matchup'] = game_lvl['stand'] +'-'+ game_lvl['p_throws']
    game_lvl = game_lvl.loc[game_lvl['rp_PAs'] > 20]
    game_lvl = game_lvl.rename(columns={'game_date_x':'game_date'})
    modeling_vars = [
        'rp_BA', 'rp_AB_div_PA', 'ytd_BA', 'ytd_AB_div_PA', 'rp_hits_var', 'ytd_hits_var',
        'inning_topbot', 'handedness_matchup', 'hit_ind',
        'rp_BA_sp', 'rp_AB_div_PA_sp', 'ytd_BA_sp', 'ytd_AB_div_PA_sp',
        'match_year_PAs', 'match_year_BA', 'match_year_AB_div_PA',
        'match_career_PAs', 'match_career_BA', 'match_career_AB_div_PA'
    ]
    id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits']
    dummy_vars = ['inning_topbot', 'handedness_matchup']
    game_lvl = game_lvl[modeling_vars + id_vars]
    game_lvl = pd.get_dummies(game_lvl, columns=dummy_vars, prefix ='', prefix_sep = '')

    #game_lvl.to_csv('./data/modeling_data.csv', index=False)

    #modeling_data = read_csv_from_gcs('bts-mlb',modeling_data.csv').dropna()
    #print("modeling_data shape: ", str(modeling_data.shape))
    inputs = [
        'rp_BA', 'rp_AB_div_PA', 'ytd_BA', 'ytd_AB_div_PA', 'rp_BA_sp',
        'rp_AB_div_PA_sp', 'ytd_BA_sp', 'ytd_AB_div_PA_sp', 'Bot',
        'L-L', 'L-R', 'R-L',
        'rp_hits_var', 'ytd_hits_var',
        'match_year_PAs', 'match_year_BA', 'match_year_AB_div_PA',
        'match_career_PAs', 'match_career_BA', 'match_career_AB_div_PA'
    ]
    print('game_lvl shape: {}'.format(str(game_lvl.shape)))
    game_lvl = game_lvl.dropna()

    print('game_lvl shape: {}'.format(str(game_lvl.shape)))
        # Ensure game_date is datetime
    game_lvl['game_date'] = pd.to_datetime(game_lvl['game_date'])

    # Calculate date range and midpoint
    min_date = game_lvl['game_date'].min()
    max_date = game_lvl['game_date'].max()
    midpoint = min_date + (max_date - min_date) / 2

    # Split
    train = game_lvl[game_lvl['game_date'] < midpoint]
    test = game_lvl[game_lvl['game_date'] >= midpoint]

    write_csv_to_gcs(train, 'bts-mlb', 'x_train.csv')
    write_csv_to_gcs(train['hit_ind'], 'bts-mlb', 'y_train.csv')
    write_csv_to_gcs(test, 'bts-mlb', 'x_test.csv')
    write_csv_to_gcs(test['hit_ind'], 'bts-mlb', 'y_test.csv')

    if prod:
        # Optional: export prod data
        pass  # e.g., game_lvl.to_csv('./data/prod_data.csv', index=False)

    return game_lvl #train, train['hit_ind'], test, test['hit_ind']
    # if prod == False:
    #     train = game_lvl[game_lvl['game_date'] < '2019-01-01']
    #     test = game_lvl[(game_lvl['game_date'] >= '2019-01-01') & (game_lvl['game_date'] < '2020-01-01')]
    #     return train, train['hit_ind'], test, test['hit_ind']
    # else:
    #     #game_lvl.to_csv('./data/prod_data.csv', index=False)
    #     train = game_lvl[game_lvl['game_date'] < '2019-01-01']
    #     test = game_lvl[(game_lvl['game_date'] >= '2019-01-01') & (game_lvl['game_date'] < '2020-01-01')]
    #     return train, train['hit_ind'], test, test['hit_ind']
