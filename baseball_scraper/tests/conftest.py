#!/bin/python

import pytest
from baseball_scraper import fangraphs, baseball_reference
import os
from bs4 import BeautifulSoup


@pytest.fixture()
def fangraph_khris_davis():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.fangraphs.khris_davis.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
    proj = fangraphs.Scraper(player_id="9112")
    proj.set_source(src)
    yield proj


@pytest.fixture()
def fangraph_jesse_winker():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.fangraphs.jesse_winker.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
    proj = fangraphs.Scraper(player_id="13590")
    proj.set_source(src)
    yield proj


@pytest.fixture()
def fangraph_jesse_winker_and_khris_davis():
    fg = fangraphs.Scraper()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.fangraphs.jesse_winker.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        fg.set_player_id("13590")
        fg.set_source(src)

    fn = dir_path + "/sample.fangraphs.khris_davis.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        fg.set_player_id("9112")
        fg.set_source(src)

    yield fg


@pytest.fixture()
def bref_team():
    br = baseball_reference.TeamScraper('NYY')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.baseball_reference.team.nyy.2015.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(2015)
        br.set_source(src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.2019.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(2019)
        br.set_source(src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.1927.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(1927)
        br.set_source(src)
    fn = dir_path + "/sample.baseball_reference.team.nyy.1741.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        br.set_season(1741)
        br.set_source(src)

    yield br
