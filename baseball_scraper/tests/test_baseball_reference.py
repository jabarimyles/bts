#!/bin/python

import datetime as dt
import pytest
import numpy as np


def test_scrape_2015(bref_team):
    bref_team.set_season(2015)
    df = bref_team.scrape('NYY')
    print(df)
    assert(len(df.index) == 162)
    assert(df[df['W/L'].isin(['W', 'W-wo'])].count()[0] == 87)


def test_scrape_2019(bref_team):
    bref_team.set_season(2019)
    df = bref_team.scrape('NYY')
    print(df)
    assert(len(df.index) == 162)
    assert(df[df['W/L'].isin(['W', 'W-wo'])].count()[0] == 54)
    assert(len(df[df['W/L'].notnull()]) == 82)


def test_2015_avg_attenance(bref_team):
    bref_team.set_season(2015)
    df = bref_team.scrape('NYY')
    home_df = df[df['Home_Away'] == 'Home']
    print(home_df)
    assert(len(home_df.index) == 81)
    assert(int(df[df['Home_Away'] == 'Home']['Attendance'].mean()) == 39922)


def test_2019_attendance_for_games_not_player(bref_team):
    bref_team.set_season(2019)
    df = bref_team.scrape('NYY')
    assert(np.isnan(df['Attendance'][162]))


def test_page_not_found(bref_team):
    bref_team.set_season(1741)
    with pytest.raises(ValueError):
        bref_team.scrape('NYY')


def test_scrape_may(bref_team):
    bref_team.set_date_range(dt.date(2019, 5, 1), dt.date(2019, 5, 31))
    df = bref_team.scrape('NYY')
    print(df)
    assert(len(df.index) == 27)
    assert(df[df['W/L'].isin(['W', 'W-wo'])].count()[0] == 20)


def test_scrape_team_list(bref_team_list):
    df = bref_team_list.scrape()
    assert(len(df.index) == 30)
    assert(df[df.Franchise.str.endswith("Rays")].abbrev.iloc(0)[0] == "TBR")
    assert(df[df.Franchise.str.endswith("Dodgers")].abbrev.iloc(0)[0] == "LAD")
