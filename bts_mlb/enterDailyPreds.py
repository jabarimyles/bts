
#-- import base packages
import time
import re
#-- import pypi packages
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
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
#-- import local python functions


def enter(pick1, pick2=None):
    #---- define variables
    print('loading chrome driver....')
    driver = webdriver.Chrome('./chromedriver')
    url = 'https://bts-web.mlb.com/mlb/fantasy/bts/y2021/index.jsp#t=picks'
    email = 'ned.hulseman@gmail.com'
    pw = 'dustPed15'
    todays_pick_reactid = "'.0.1.1.1.0.0'"
    todays_pick_reactid2 = todays_pick_reactid[:12] + '1' +todays_pick_reactid[13:]
    select_team_reactid = "'.0.1.1.1.0.1.0.0.0.4'"
    select_team_reactid2 = select_team_reactid[:12] + '2' +select_team_reactid[13:]
    tbody_reactid = "'.0.1.1.1.0.1.1.0.1'"
    select_suffix = ".3.0"
    player_id_search_pattern = "people/(.*?)/headshot"
    team_dict = {}

    ids = [
        "'.0.1.1.1.0.1.0.2.0.0'", "'.0.1.1.1.0.1.0.2.0.1'",
        "'.0.1.1.1.0.1.0.2.0.2'", "'.0.1.1.1.0.1.0.2.0.3'",
        "'.0.1.1.1.0.1.0.2.0.4'", "'.0.1.1.1.0.1.0.2.0.5'",
        "'.0.1.1.1.0.1.0.2.0.6'", "'.0.1.1.1.0.1.0.2.0.7'",
        "'.0.1.1.1.0.1.0.2.0.8'", "'.0.1.1.1.0.1.0.2.0.a'",
        "'.0.1.1.1.0.1.0.2.0.b'", "'.0.1.1.1.0.1.0.2.0.c'",
        "'.0.1.1.1.0.1.0.2.0.d'", "'.0.1.1.1.0.1.0.2.0.e'",
        "'.0.1.1.1.0.1.0.2.0.f'", "'.0.1.1.1.0.1.0.2.0.g'",
        "'.0.1.1.1.0.1.0.2.0.h'", "'.0.1.1.1.0.1.0.2.0.i'",
        "'.0.1.1.1.0.1.0.2.0.j'", "'.0.1.1.1.0.1.0.2.0.k'",
        "'.0.1.1.1.0.1.0.2.0.l'", "'.0.1.1.1.0.1.0.2.0.m'",
        "'.0.1.1.1.0.1.0.2.0.n'", "'.0.1.1.1.0.1.0.2.0.o'",
        "'.0.1.1.1.0.1.0.2.0.p'", "'.0.1.1.1.0.1.0.2.0.q'",
        "'.0.1.1.1.0.1.0.2.0.r'", "'.0.1.1.1.0.1.0.2.0.s'",
        "'.0.1.1.1.0.1.0.2.0.t'"
    ]


    #-- Run Selenium Processes
    print('loading url....')
    driver.get(url)
    time.sleep(15)
    print('selecting log in....')
    driver.find_element_by_class_name('ui-dialog-buttonset').click()
    print('logging in....')
    driver.find_element_by_id('okta-signin-username').send_keys(email)
    driver.find_element_by_id('okta-signin-password').send_keys(pw)
    driver.find_element_by_id("okta-signin-submit").click()
    time.sleep(15)
    #---- Pick #1
    #-- todays pick #1
    print('opening team selection and collecting react_ids....')
    driver.find_element_by_xpath("(//div[@data-reactid={}])".format(todays_pick_reactid)).click()
    driver.find_element_by_xpath("(//li[@data-reactid={}])".format(select_team_reactid)).click()
    time.sleep(3)

    #-- pick by team
    for id in ids:
        try:
            id_fixed = id#.replace("'.0.1.1", "'.0.1.2") # temporary for dev
            el = driver.find_element_by_xpath("(//li[@data-reactid={}])".format(id_fixed))
            team = el.get_attribute("class").split(' ')[0].upper()
            team_dict[team] = {}
            team_dict[team]['team_reactid'] = id_fixed

        except Exception as e:
            print('error on id: {}'.format(id_fixed))
            pass
    cols = ['team', 'team_reactid', 'player_name', 'player_reactid', 'player_id']
    player_df = pd.DataFrame(columns=cols)

    driver.find_element_by_xpath("(//li[@data-reactid={}])".format(select_team_reactid)).click()
    #-- create player_df
    print('iterating through teams to collect player react_ids....')
    for team in team_dict.keys():
        time.sleep(2)
        driver.find_element_by_xpath("(//li[@data-reactid={}])".format(select_team_reactid)).click()
        time.sleep(3)
        driver.find_element_by_xpath("(//li[@data-reactid={}])".format(team_dict[team]['team_reactid'])).click()
        time.sleep(3)
        tbody = driver.find_element_by_xpath("(//tbody[@data-reactid={}])".format(tbody_reactid))
        rows = tbody.find_elements_by_tag_name("tr")
        players = []
        for r in rows:
            im = r.find_element_by_tag_name("td").find_element_by_tag_name("img")
            src = im.get_attribute("src")
            reactid = im.get_attribute("data-reactid")
            #players.append((re.search(player_id_search_pattern, reactid).group(1), reactid))
            #team_dict[team]['players'] = players
            df_row = {
                'team': team,
                'team_reactid': team_dict[team]['team_reactid'],
                'player_name': '',
                'player_reactid': reactid[:-4],
                'player_id': re.search(player_id_search_pattern, src).group(1)
            }
            player_df = player_df.append(df_row, ignore_index=True)
    write_csv_to_gcs(player_df, 'bts-mlb', 'bts_player_df.csv')
    #--- submit picks
    print('submitting picks....')

    player_row = player_df.loc[player_df['player_id']==pick1]
    player_team_reactid = player_row['team_reactid'].iloc[0]
    player_reactid = player_row['player_reactid'].iloc[0]
    driver.find_element_by_xpath("(//li[@data-reactid={}])".format(select_team_reactid)).click()
    time.sleep(3)
    driver.find_element_by_xpath("(//li[@data-reactid={}])".format(player_team_reactid)).click()
    time.sleep(3)
    driver.find_element_by_xpath("(//button[@data-reactid='{}{}'])".format(player_reactid, select_suffix)).click()
    if pick2 != None:
        print('submitting pick #2...')
        time.sleep(3)
        driver.find_element_by_xpath("(//div[@data-reactid={}])".format(todays_pick_reactid2)).click()
        player_row = player_df.loc[player_df['player_id']==pick2]
        player_team_reactid = player_row['team_reactid'].iloc[0]
        print(player_team_reactid)
        player_team_reactid = player_team_reactid[:12] + '2' +player_team_reactid[13:]
        #player_team_reactid = select_team_reactid2[0:-1]+player_team_reactid[len(select_team_reactid2)-1:]
        print(player_team_reactid)
        player_reactid = "'" + player_row['player_reactid'].iloc[0]
        player_reactid = player_reactid[0:12]+'2'+player_reactid[13:]
        print(player_reactid)
        driver.find_element_by_xpath("(//li[@data-reactid={}])".format(select_team_reactid2)).click()
        time.sleep(3)
        driver.find_element_by_xpath("(//li[@data-reactid={}])".format(player_team_reactid)).click()
        time.sleep(3)
        driver.find_element_by_xpath("(//button[@data-reactid={}{}])".format(player_reactid, select_suffix+"'")).click()
    driver.quit()
    return player_df


if __name__ == '__main__':
    #-- test with Rafael Devers,  Mookie
    enter("646240", "605141")
