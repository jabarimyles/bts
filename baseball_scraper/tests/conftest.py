#!/bin/python

import pytest
from baseball_scraper import fangraphs, baseball_reference, espn
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


@pytest.fixture()
def fg():
    fg = fangraphs.Scraper("Steamer (RoS)")
    fg.load_fake_cache()
    yield fg


@pytest.fixture(scope="module")
def bref_team():
    br = baseball_reference.TeamScraper()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.baseball_reference.team.nyy.2015.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(2015)
        br.set_source('NYY', src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.2019.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(2019)
        br.set_source('NYY', src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.1927.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(1927)
        br.set_source('NYY', src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.1741.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(1741)
        br.set_source('NYY', src)

    yield br


@pytest.fixture(scope="module")
def espn_probable_starters():
    start_date = datetime(2019, 8, 7)
    end_date = start_date + timedelta(1)
    es = espn.ProbableStartersScraper(start_date, end_date)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    for day in [start_date, end_date]:
        day_s = day.strftime("%b%d")
        fn = dir_path + "/sample.espn.probable_starters.{}.xml".format(day_s)
        with open(fn, "r") as f:
            src = BeautifulSoup(f, "lxml")
            es.set_source(day, src)
    yield es
