from datetime import datetime, timedelta
from baseball_scraper import espn
import pytest


def test_scrape_probable_starters(espn_probable_starters):
    df = espn_probable_starters.scrape()
    print(df)
    assert(len(df.index) == 42)
    assert(df.iloc[20]['Name'] == 'James Paxton')
    assert(df.iloc[20]['espn_id'] == 31980)
    assert(df.iloc[23]['Name'] == 'Eduardo Rodriguez')
    assert(df.iloc[23]['espn_id'] == 32675)


def test_scrape_prob_start_bad_date():
    start_date = datetime(2019, 8, 7)
    end_date = start_date - timedelta(1)
    with pytest.raises(ValueError):
        espn.ProbableStartersScraper(start_date, end_date)
