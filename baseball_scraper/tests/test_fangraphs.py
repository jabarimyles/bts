#!/bin/python

from baseball_scraper import fangraphs
import math


def test_instances():
    avail = fangraphs.Scraper.instances()
    print(avail)
    assert(len(avail) == 6)
    assert('THE BAT (RoS)' in avail)
    assert('Steamer (RoS)' in avail)


def test_scrape_hitter(fg):
    df = fg.scrape_hitter(13611)
    print(df)
    assert(df['HR'][0] == 11)
    assert(df['RBI'][0] == 35)
    assert(df['SB'][0] == 8)
    assert(math.isclose(df['AVG'][0], 0.298))


def test_scrape_pitcher(fg):
    df = fg.scrape_pitcher(3137)
    print(df)
    assert(df['W'][0] == 6)
    assert(df['SO'][0] == 103)
    assert(math.isclose(df['ERA'][0], 3.04))

def test_scrape_multi(fg):
    df = fg.scrape_hitter("13590")
    assert(df.Name[0] == "Jesse Winker")
    df = fg.scrape_hitter("13611")
    assert(df.Name[0] == "Mookie Betts")
