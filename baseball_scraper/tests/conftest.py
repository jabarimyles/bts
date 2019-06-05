#!/bin/python

import pytest
from baseball_scraper import fangraphs
import os
from bs4 import BeautifulSoup


@pytest.fixture()
def fangraph_khris_davis():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.khris_davis.fangraphs.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
    proj = fangraphs.Scraper(player_id="9112")
    proj.set_source(src)
    yield proj


@pytest.fixture()
def fangraph_jesse_winker():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.jesse_winker.fangraphs.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
    proj = fangraphs.Scraper(player_id="13590")
    proj.set_source(src)
    yield proj


@pytest.fixture()
def fangraph_jesse_winker_and_khris_davis():
    fg = fangraphs.Scraper()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.jesse_winker.fangraphs.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        fg.set_player_id("13590")
        fg.set_source(src)

    fn = dir_path + "/sample.khris_davis.fangraphs.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
        fg.set_player_id("9112")
        fg.set_source(src)

    yield fg
