#!/bin/python

import numpy as np


def test_instances(fangraph_hitting_projection):
    avail = fangraph_hitting_projection.instances()
    print(avail)
    assert(len(avail) == 4)
    assert('THE BAT (R)' in avail)
    assert('Steamer (R)' in avail)


def test_scrape_zips(fangraph_hitting_projection):
    df = fangraph_hitting_projection.scrape('ZiPS (R)')
    print(df)
    assert(df['HR'][0] == 24)
    assert(df['RBI'][0] == 64)
    assert(df['AVG'][0] == 0.246)
    assert(np.isnan(df['GDP'][0]))


def test_scrape_the_bat(fangraph_hitting_projection):
    df = fangraph_hitting_projection.scrape('THE BAT (R)')
    print(df)
    assert(df['HR'][0] == 30)
    assert(df['RBI'][0] == 72)
    assert(df['AVG'][0] == 0.253)
    assert(df['GDP'][0] == 6)
