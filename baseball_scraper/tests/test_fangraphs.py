#!/bin/python

from baseball_scraper import fangraphs
import numpy as np
import math
import pytest


def test_instances(fangraph_khris_davis):
    avail = fangraph_khris_davis.instances()
    print(avail)
    assert(len(avail) == 4)
    assert('THE BAT (R)' in avail)
    assert('Steamer (R)' in avail)


def test_scrape_zips(fangraph_khris_davis):
    df = fangraph_khris_davis.scrape(instance='ZiPS (R)')
    print(df)
    assert(df['HR'][0] == 24)
    assert(df['RBI'][0] == 64)
    assert(math.isclose(df['AVG'][0], 0.246))
    assert(np.isnan(df['GDP'][0]))


def test_scrape_the_bat(fangraph_khris_davis):
    df = fangraph_khris_davis.scrape(instance='THE BAT (R)')
    print(df)
    assert(df['HR'][0] == 30)
    assert(df['RBI'][0] == 72)
    assert(math.isclose(df['AVG'][0], 0.253))
    assert(df['GDP'][0] == 6)


def test_scrape_steamer(fangraph_jesse_winker):
    df = fangraph_jesse_winker.scrape(instance='Steamer (R)')
    print(df)
    assert(df['HR'][0] == 10)
    assert(math.isclose(df['BABIP'][0], 0.302))
    assert(math.isclose(df['UBR'][0], -0.6))


def test_scrape_multi(fangraph_jesse_winker_and_khris_davis):
    fg = fangraph_jesse_winker_and_khris_davis
    fg.set_player_id("13590")
    kd_df = fg.scrape('Steamer (R)')
    print(kd_df)
    fg.set_player_id("9112")
    jw_df = fg.scrape('Steamer (R)')
    print(jw_df)
    assert(kd_df['HR'][0] != jw_df['HR'][0])
    assert(kd_df['RBI'][0] != jw_df['RBI'][0])


def test_scape_no_player_id():
    fg = fangraphs.Scraper()
    with pytest.raises(RuntimeError):
        fg.instances()
    with pytest.raises(RuntimeError):
        fg.scrape('Steamer (R)')
