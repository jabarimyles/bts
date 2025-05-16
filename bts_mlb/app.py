
#-- Base packages
import os
import sys
import pickle
from google.cloud import storage
import io
from io import BytesIO
from .gcs_helpers import *


#-- Pypi paackages
from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
from datetime import datetime as dt
from bs4 import BeautifulSoup

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


with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
    json.dump(service_account_info, temp_file)
    temp_file_path = temp_file.name

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path



# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
#import urllib.request
#import pandas.io.sql as psql
#import sqlalchemy
#from sqlalchemy.types import INTEGER, TEXT

#-- Custom packages



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
    preds = preds.dropna(subset=['batter', 'starting_pitcher']) #, 'batter_name','pitcher_name' 

    preds['proba'] = preds['proba'].apply(lambda x: "{0:.1f}%".format(x*100))
    #preds['batter_img'] = preds['batter'].apply(lambda x: '<img class="food-img" src=./static/images/{}.jpg  onError="this.onerror=null;this.src=./static/images/placeholder.jpg;"></img>'.format(int(x)))
    #preds['pitcher_img'] = preds['starting_pitcher'].apply(lambda x: '<img class="food-img" src=./static/images/{}.jpg onError="this.onerror=null;this.src=./static/images/placeholder.jpg;"></img>'.format(int(x)))
    preds['batter_img'] = preds['batter'].apply(
    lambda x: '<img class="food-img" src="/static/images/{}.jpg" onError="this.onerror=null;this.src=\'/static/images/placeholder.jpg\';">'.format(int(x)))


    preds['pitcher_img'] = preds['starting_pitcher'].apply(
    lambda x: '<img class="food-img" src="/static/images/{}.jpg" onError="this.onerror=null;this.src=\'/static/images/placeholder.jpg\';">'.format(int(x)))


    preds['batter_disp'] = "<a class='player-name'>" + preds['batter'].astype(str) + "</a>" + preds['batter_img']
    preds['pitcher_disp'] = "<a class='player-name'>" + preds['starting_pitcher'].astype(str) + "</a>" + preds['pitcher_img']

    #preds['batter_disp'] = "<a class='player-name'>"+preds['batter']+"</a>"+preds['batter_img']
    #preds['pitcher_disp'] = "<a class='player-name'>"+preds['starting_pitcher']+"</a>"+preds['pitcher_img']
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
