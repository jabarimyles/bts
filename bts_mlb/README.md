# ![The Logo](https://github.com/nedhulseman/bts2/blob/main/bts%20logo.png) Beat The Streak
This project is a part of the [Beat The Streak Competition](https://www.mlb.com/apps/beat-the-streak) hosted by [Major League Baseball](http://www.mlb.com). 

#### -- Project Status: [Active]

## Project Intro/Objective
The purpose of this project is create sound and accurate predictions for batters getting at least one hit on any given day. In addition, we want to learn more about what factors influence a batter's probability of getting a hit.



### Methods Used
* Logistic Regression
* Random Forest
* LightGBM


### Technologies
* Python
* Selenium 
* Flask
* HTML
* CSS


## Project Description
We pulled our data using the package baseball_scraper which provides play level information on historical baseball games going back to 2013.

## Needs of this project

- frontend developer
- data exploration
- data processing/cleaning
- statistical modeling
- automation of pick submission

## Getting Started

1. Clone this repo (for help see this [tutorial](https://help.github.com/articles/cloning-a-repository/)).
2. Raw Data is obtained from the get_data.py script    
3. Data processing/transformation scripts are being kept [here](Repo folder containing data processing scripts/notebooks)
4. Downlaod the appropriate Google Chrome Driver [here](https://chromedriver.chromium.org/downloads)
5. Install required packages from [requirement.txt](https://github.com/nedhulseman/bts2/blob/main/requirements.txt)

## File Descriptions
1. Main.py completes the gathering and manipulation of data for both training and deployment for time period of interest.
2. Main_train.py trains the models models using given a start date and end date provided. 
3. App.py spins up application to see current and previous results and predicted probabilities.

## Data Dictionary
The data is obtained using the package [baseball-scraper] (https://pypi.org/project/baseball-scraper/). The data dictionary for the raw variables can be found [here](https://baseballsavant.mlb.com/csv-docs).


## Contributing Members

**Team Leads (Contacts) : [Jabari Myles](https://github.com/mylesj8259) [Ned Hulseman](https://github.com/nedhulseman)**

## Overview of Scripts

**main.py**
- Purpose: Uses the get_data, createTable, and other local modules, to create
**main_train.py**
- Purpose:
**train_model.py**
- Purpose:
**get_data.py**
- Purpose: Collects data from statcast. This is used in prod to get recent data, as well as the source of data to train models

**createTableGameLvl.py**
- Purpose: Calculate game level summaries per hitter for hits, PAs, hometeam, starting_pitcher

|   game_year |   batter | inning_topbot   |   game_pk | game_date   | stand   | home_team   | away_team   |   index |   hits |   non_abs |   outs |   starting_pitcher |
|------------:|---------:|:----------------|----------:|:------------|:--------|:------------|:------------|--------:|-------:|----------:|-------:|-------------------:|
|        2013 |   111072 | Bot             |    346831 | 2013-04-07  | R       | TOR         | BOS         |   18570 |      1 |         0 |      3 |452657 |
|        2013 |   136660 | Bot             |    346831 | 2013-04-07  | R       | TOR         | BOS         |   19862 |      0 |         1 |      3 |452657 |
|        2013 |   408314 | Bot             |    346831 | 2013-04-07  | L       | TOR         | BOS         |    7336 |      1 |         0 |      0 |452657 |

**createTableRPPlayer.py**
- Purpose:
**createModelingByBatter.py**
- Purpose:
**createTableMatchups**
- Purpose: Created batter/SP matchup level data, at the time of a given game date



| game_date   |   batter |   pitcher |   game_pk |   hit |   non_ab |   out |   year |   game_ind |   year_hits |   year_outs |   year_non_abs |   year_games_played |   career_hits |   career_outs |   career_non_abs |   career_games_played |
|:------------|---------:|----------:|----------:|------:|---------:|------:|-------:|-----------:|------------:|------------:|---------------:|--------------------:|--------------:|--------------:|-----------------:|----------------------:|
| 2020-08-10  |   453568 |    605177 |    631546 |     1 |        0 |     0 |   2020 |          1 |         nan |         nan |            nan |                 nan |             9 |            22 |                3 |                    33 |
| 2019-08-14  |   453568 |    605177 |    565472 |     0 |        0 |     1 |   2019 |          1 |           1 |           4 |              0 |                   5 |             9 |            21 |                3 |                    32 |
| 2019-08-12  |   453568 |    605177 |    565470 |     0 |        0 |     1 |   2019 |          1 |           1 |           3 |              0 |                   4 |             9 |            20 |                3 |                    31 |



**createTablePlayerMeta.py**
- Purpose:
**createTodaysMatchups.py**
- Purpose:
**enterDailyPreds.py**
- Purpose:



## Data Dictionary:

|Field                 | Field Name         | Script | Implemented |
|------------------------|--------------------|-----------------|-------------|
| rp_BA                  | Recent Play Batting AVG   | createTableRPPlayer.get_rpplayer | Yes |
| rp_AB_div_PA           | Recent Play Batting AB/PA |  createTableRPPlayer.get_rpplayer | Yes |
| ytd_BA                 | YTD Batting AVG.          | .  | Yes |
| ytd_AB_div_PA          | YTD AB/PA                 | .  | Yes |
| rp_hits_var            | Recent Play Hit Variance  | .  | Yes |
| ytd_hits_var           | YTD Hit Variance          |.   | Yes |
| hit_ind                | Did player get hit? (target)  |.   | Yes |
| rp_BA_sp               | Recent Play Batting AVG for Starting Pitcher |.  |.  | Yes |
| rp_AB_div_PA_sp        | Recent Play YTD Batting AVG for SP  |  |.   | Yes |
| ytd_BA_sp              | YTD Batting AVG for SP  |.  | Yes |
| ytd_AB_div_PA_sp       | YTD AB/PA for SP  |.  | Yes |
| match_year_PAs         | Batter/SP YTD # of PAs  |.  | Yes |
| match_year_BA          | Batter/SP YTD Batting AVG    |.  | Yes |
| match_year_AB_div_PA   | Batter/SP YTD AB/PA    |.  | Yes |
| match_career_PAs       | Batter/SP Career PAs  |.  | Yes |
| match_career_BA        | Batter/SP Career BAs  |.  | Yes |
| match_career_AB_div_PA | Batter/SP Career AB/PA  |.  | Yes |
| game_date              | Game Date  |.  | Yes |
| game_pk                | Game ID  |.  | Yes |
| batter                 | Batter ID  |.  | Yes |
| starting_pitcher       | Starting Pitcher ID  |.  | Yes |
| ABs                    | Batter Career ABs   |.  | Yes |
| hits                   | Batter Career Hits  |.  | Yes |
| Bot               | Batting in Bottom of Inning (Home Team)  |.  | Yes |
| Top                | Batting in Top of Inning (Away Team)  |.  | Yes |
| L-L               | Batter is Lefty; SP is Lefty  |.  | Yes |
| L-R               | Batter is Lefty; SP is Righty |.  | Yes |
| R-L                | Batter is Righty; SP is Lefty  |.  | Yes |
| R-R                | Batter is Righty; SP is Righty  |.  | Yes |
| rp_BA_bp               | Recent Play Batting AVG for Bullpen |.  |.  | No |
| rp_AB_div_PA_bp        | Recent Play YTD Batting AVG for Bullpen  |  |.   | No |
| ytd_BA_bp              | YTD Batting AVG for Bullpen  |.  | No |
| ytd_AB_div_PA_bp       | YTD AB/PA for Bullpen  |.  | No |



# Using BTS2!
1. Get Statcast data
2. 
