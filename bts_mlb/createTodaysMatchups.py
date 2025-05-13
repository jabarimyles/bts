
#-- Base packages
import os
import sys
from datetime import date
import requests
import re
import json

#-- Pypi packages
import pandas as pd
from bs4 import BeautifulSoup

#-- Custom packages

def get_todays_matchups(target_date=None):
    today = date.today().strftime('%Y-%m-%d')
    schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher"
    schedule_resp = requests.get(schedule_url).json()

    all_matchups = []

    for date_info in schedule_resp.get('dates', []):
        for game in date_info.get('games', []):
            game_id = game['gamePk']
            home_team = game['teams']['home']['team']['name']
            away_team = game['teams']['away']['team']['name']

            home_pitcher = game['teams']['home'].get('probablePitcher', {}).get('id')
            away_pitcher = game['teams']['away'].get('probablePitcher', {}).get('id')

            if not home_pitcher or not away_pitcher:
                continue

            # Get boxscore to extract lineups
            boxscore_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
            box = requests.get(boxscore_url).json()

            try:
                home_players = box['teams']['home']['players']
                away_players = box['teams']['away']['players']
            except KeyError:
                continue  # skip if not available

            # Build batter vs opposing pitcher
            for p_id, player in away_players.items():
                if player.get('position', {}).get('code') == '1':  # skip pitchers
                    continue
                batter_id = player['person']['id']
                all_matchups.append({
                    'batter_id': batter_id,
                    'pitcher_id': home_pitcher,
                    'batter_team': away_team,
                    'pitcher_team': home_team
                })

            for p_id, player in home_players.items():
                if player.get('position', {}).get('code') == '1':
                    continue
                batter_id = player['person']['id']
                all_matchups.append({
                    'player_id': batter_id,
                    'pitcher_id': away_pitcher,
                    'batter_team': home_team,
                    'pitcher_team': away_team
                })

    df = pd.DataFrame(all_matchups)
    keep_cols = ['player_id', 'pitcher_id', 'batter_team', 'pitcher_team']
    matchups_df = df[keep_cols]
    today = date.today()
    matchups_df['game_date'] = '{:04d}-{:02d}-{:02d} 00:00:00'.format(today.year, today.month, today.day)

    matchups_df['game'] = matchups_df.apply(lambda x: ", ".join(sorted([x['batter_team'], x['pitcher_team']])), axis=1)
    start_id = -111111.0
    game_pk_ph = pd.DataFrame(columns=['game', 'game_pk'])
    for game in matchups_df['game'].unique():
        game_pk_ph = game_pk_ph._append({'game':game, 'game_pk': start_id}, ignore_index=True)
        start_id -= 1
    matchups_df = pd.merge(matchups_df, game_pk_ph, how='left', on='game')
    matchups_df['events'] = '_placeholder_'
    matchups_df['inning_topbot'] = 'Bot' # temporary
    rename_cols = {
        'player_id': 'batter',
        'pitcher_id': 'pitcher',
    }
    matchups_df = matchups_df.rename(columns=rename_cols)

    return matchups_df
