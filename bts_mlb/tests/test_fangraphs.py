#!/bin/python

from bts_mlb import fangraphs
import math


def test_instances():
    avail = fangraphs.Scraper.instances()
    print(avail)
    assert(len(avail) == 6)
    assert('THE BAT (RoS)' in avail)
    assert('Steamer (RoS)' in avail)


def test_scrape_hitter(fg):
    df = fg.scrape(13611, scrape_as=fangraphs.ScrapeType.HITTER)
    print(df)
    assert(df['HR'][0] == 11)
    assert(df['RBI'][0] == 35)
    assert(df['SB'][0] == 8)
    assert(math.isclose(df['AVG'][0], 0.298))


def test_scrape_pitcher(fg):
    df = fg.scrape(3137, scrape_as=fangraphs.ScrapeType.PITCHER)
    print(df)
    assert(df['W'][0] == 6)
    assert(df['SO'][0] == 103)
    assert(math.isclose(df['ERA'][0], 3.04))


def test_scrape_multi(fg):
    df = fg.scrape(["13590", "13611"], scrape_as=fangraphs.ScrapeType.HITTER)
    assert(len(df.index) == 2)
    assert(df.Name[0] == "Mookie Betts")
    assert(df.Name[1] == "Jesse Winker")


def test_scrape_hitters_by_player_name(fg):
    df = fg.scrape("Jesse Winker", id_name="Name")
    print(len(df.index))
    assert(df.Name[0] == "Jesse Winker")
    assert(df.playerid[0] == "13590")


def test_scrape_hitters_by_team(fg):
    df = fg.scrape("Blue Jays", id_name="Team")
    print(len(df.index))
    for name in ["Vladimir Guerrero Jr.", "Justin Smoak",
                 "Lourdes Gurriel Jr."]:
        plyr = df[df.Name == name]
        assert(len(plyr.index) == 1)


def test_scrape_pitchers_by_player_name(fg):
    df = fg.scrape("Steve Cishek", id_name="Name",
                   scrape_as=fangraphs.ScrapeType.PITCHER)
    print(len(df.index))
    print(df)
    assert(df.Name[0] == "Steve Cishek")
    assert(df.playerid[0] == "6483")


def test_scrape_pitchers_by_team(fg):
    df = fg.scrape("Dodgers", id_name="Team",
                   scrape_as=fangraphs.ScrapeType.PITCHER)
    print(len(df.index))
    for name in ["Hyun-Jin Ryu", "Clayton Kershaw", "Walker Buehler"]:
        plyr = df[df.Name == name]
        assert(len(plyr.index) == 1)
