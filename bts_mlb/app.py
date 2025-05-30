#-- Base packages
import os
import sys
import pickle
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from gcs_helpers import *
import tempfile
import json


#-- Pypi paackages
from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
from datetime import datetime as dt
from bs4 import BeautifulSoup
#import urllib.request
#import pandas.io.sql as psql
#import sqlalchemy
#from sqlalchemy.types import INTEGER, TEXT

#-- Custom packages

# Your service account JSON string from an env var or secret manager
service_account_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])

# Write it to a temporary file
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path



app = Flask(__name__)

#app.config['img_dir'] = 'static/images'

#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///coffee.db'
#app.secret_key = 'java'
#db = SQLAlchemy(app)


@app.route('/')
@app.route('/bts2')
def main():
    model_fp = 'table_dict.pickle'
    table_dict = download_pickle_from_gcs('bts-mlb', model_fp)
    preds = table_dict['todays_preds'].sort_values('proba', ascending=False)
    preds = preds.dropna(subset=['batter', 'starting_pitcher', 'batter_name','pitcher_name' ]) #
    # Make sure proba is numeric for sorting (if you've formatted it before)
    preds['proba'] = preds['proba'].astype(float)
    # Sort by game_date and proba descending
    preds = preds.sort_values(by=['game_date', 'proba'], ascending=[True, False])
    # Keep only top 20 for each game_date
    preds = preds.head(25) #preds.groupby('game_date', group_keys=False).head(5)

    preds['proba'] = preds['proba'].apply(lambda x: "{0:.1f}%".format(x*100))

    preds['batter_img'] = preds['batter'].apply(
    lambda x: f'<img class="food-img" src="/static/images/{int(x)}.jpg" onError="this.onerror=null;this.src=\'/static/images/placeholder.jpg\';">')

    preds['pitcher_img'] = preds['starting_pitcher'].apply(
        lambda x: f'<img class="food-img" src="/static/images/{int(x)}.jpg" onError="this.onerror=null;this.src=\'/static/images/placeholder.jpg\';">'
    )

    
    preds['batter_disp'] = "<a class='player-name'>"+preds['batter_name']+"</a>"+preds['batter_img']
    preds['pitcher_disp'] = "<a class='player-name'>"+preds['pitcher_name']+"</a>"+preds['pitcher_img']
    preds['hit_outcomes'] = preds['hits'].astype(str) +' / '+ preds['ABs'].astype(str)


    dates_arr = table_dict['todays_preds'][['game_date']].drop_duplicates().sort_values(by='game_date', ascending=True).reset_index()
    dates_arr['month'] = pd.to_datetime(dates_arr['game_date']).dt.month_name()
    dates_arr['day'] = pd.to_datetime(dates_arr['game_date']).dt.day.astype(str)
    dates_arr['month_num'] = pd.to_datetime(dates_arr['game_date']).dt.month.astype(str)
    dates_json = dates_arr[['month', 'day', 'month_num', 'game_date']].to_json(orient="records")

    current_date = preds['game_date'].max()
    print('--- printing requests ---')
    print(current_date)
    #preds = preds.loc[preds['game_date']==current_date]
    print(preds['game_date'].unique())
    print(preds.shape)
    month = str(dates_arr.loc[dates_arr['game_date']==current_date, 'month'].unique()[0])
    day = str(dates_arr.loc[dates_arr['game_date']==current_date, 'day'].unique()[0])
    index = dates_arr.loc[dates_arr['game_date']==current_date, 'month'].index[0]
    current_date = preds['game_date'].max()
    keep_cols = ['game_date', 'batter_disp', 'pitcher_disp', 'proba', 'hit_outcomes', 'hit_ind']
    rename_cols = {
        'proba': 'Estimated Hit Probability',
        'batter_disp': 'Player',
        'pitcher_disp': 'Starting Pitcher'
    }
    preds = preds[keep_cols]
    preds = preds.sort_values(by=['game_date', 'proba'], ascending=(False, False)).rename(columns=rename_cols)
    table = preds.to_html(index=False, escape=False)
    soup = BeautifulSoup(table, "html.parser")
    rows = soup.find_all('tr')
    rows[0]['class'] = 'header'
    for i, gd in enumerate(preds['game_date']):
        rows[i+1]['class'] = preds['game_date'].iloc[i]
    print(preds.shape)

    return render_template("index.html", table=soup, dates_arr=dates_json, month=month, day=day,index=index)


if __name__ == "__main__":
    app.run(debug=True)