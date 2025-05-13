import requests
import pandas as pd
from datetime import date

def get_daily_matchups(target_date=None):
    if target_date is None:
        target_date = date.today().strftime('%Y-%m-%d')

    url = 'https://baseballsavant.mlb.com/statcast_search/csv'
    
    params = {
        'all': 'true',
        'hfPT': '',      # pitch type
        'hfAB': '',      # batter handedness
        'hfGT': '',      # game type
        'hfPR': '',      # pitcher handedness
        'hfZ': '',       # zone
        'stadium': '',
        'hfBBL': '',
        'hfNewZones': '',
        'hfPull': '',
        'metric_1': 'exit_velocity',
        'metric_1_gt': '',
        'metric_1_lt': '',
        'metric_2': '',
        'metric_2_gt': '',
        'metric_2_lt': '',
        'team': '',
        'position': '',
        'hfRO': '',
        'home_road': '',
        'date_from': target_date,
        'date_to': target_date,
        'player_type': 'batter',
        'type': 'details',
        'min_pas': 0,
        'sort_col': 'game_date',
        'sort_order': 'desc'
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
    from io import StringIO
    df = pd.read_csv(StringIO(response.text))
    df = df[df['game_date'] == target_date]

    
    # Rename and filter relevant columns
    df = df.rename(columns={
        'batter': 'player_id',
        'pitcher': 'pitcher_id',
        'home_team': 'home_team',
        'away_team': 'away_team'
    })
    
    # Determine batter_team and pitcher_team
    df['batter_team'] = df.apply(
        lambda row: row['home_team'] if row['inning_topbot'] == 'Bot' else row['away_team'], axis=1)
    df['pitcher_team'] = df.apply(
        lambda row: row['away_team'] if row['inning_topbot'] == 'Bot' else row['home_team'], axis=1)

    return df[['player_id', 'pitcher_id', 'batter_team', 'pitcher_team', 'game_date']]

# Example usage:
if __name__ == '__main__':
    matchup_df = get_daily_matchups('2025-05-10')  # or leave blank for today
    print(matchup_df.head())
