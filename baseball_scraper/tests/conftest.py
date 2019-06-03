#!/bin/python

import pytest
from baseball_scraper import batting_projection
import os
from bs4 import BeautifulSoup


@pytest.fixture()
def fangraph_hitting_projection():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    fn = dir_path + "/sample.hitting_projection.fangraphs.xml"
    with open(fn, "r") as f:
        src = BeautifulSoup(f, "lxml")
    proj = batting_projection.FanGraphs("9112")
    proj.set_source(src)
    yield proj
