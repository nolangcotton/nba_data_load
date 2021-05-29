#--------------------------------------------------------------------------------
#   Program: NBA Stat Scraper
#   Purpose: Scrape NBA team stats reformat data & load into PostgreSQL
#   Author : Nolan Cotton
#--------------------------------------------------------------------------------
import pandas as pd
import pgpasslib
import re
import psycopg2
from sqlalchemy import create_engine
from selenium import webdriver

#---------------------------------------------
#   Checks for schema prior to data load
#---------------------------------------------
def check_for_schema(password):
    engine = create_engine('postgresql://data_load:' + password + '@localhost:5432/reporting', client_encoding='utf8')
    conn = engine.connect()
    try:
        res = conn.execute('select count(1) from information_schema.schemata where schema_name = \'nba\'')
        if res.fetchone()[0] != 1:
            print('NBA schema not yet created, creating now')
            conn.execute('create schema nba')
        else:
            print('NBA schema present, skipping creation')
    except psycopg2.OperationalError as e:
        print("Unable to query reporting db ", e)

#---------------------------------------------
#   Retrieves player data for provided team
#---------------------------------------------
def get_player_data(team, password):

    #-----------------------------------------
    #   Get Player Data from URL
    #-----------------------------------------
    url = f'https://www.basketball-reference.com/teams/{team}/2021.html'
    df = pd.read_html(url)[1]
    df['Team Name'] = team

    #-----------------------------------------
    #   Fix column names & Drop un-needed
    #-----------------------------------------
    df.rename(columns={'Unnamed: 1': 'name', 'PTS/G': 'PPG', 'Team Name': 'teamname'}, inplace=True)
    df.drop(['Rk'], axis=1, inplace=True)
    pctg_pattern = re.compile('%')
    for col in df:
        if re.search(pctg_pattern, col):
            df.rename(columns={col: col.replace('%', 'pct')}, inplace=True)
        df.rename(columns={col: col.lower()}, inplace=True)

    #-----------------------------------------
    #   Connect to DB and load data
    #-----------------------------------------
    engine = create_engine(f'postgresql://data_load:{password}@localhost:5432/reporting',client_encoding='utf8')
    df.to_sql(schema='nba', name='player_stats', con=engine, index=False, if_exists='replace')

#---------------------------------------------
#   Drops team data table
#---------------------------------------------
def drop_team_data(password):
    engine = create_engine(f'postgresql://data_load:{password}@localhost:5432/reporting', client_encoding='utf8')
    conn = engine.connect()
    try:
        conn.execute('drop table if exists nba.team_stats')
    except psycopg2.OperationalError as e:
        print("Unable to drop reporting db ", e)

#---------------------------------------------
#   Retrieves team data and loads to tbl
#---------------------------------------------
def get_team_data(team, password):

    #--------------------------------------------------------------
    #   Get Team Data from URL, render the site before pulling html
    #--------------------------------------------------------------
    url = f'https://www.basketball-reference.com/teams/{team}/2021.html'
    driver = webdriver.Safari()
    driver.get(url)
    html = driver.page_source

    #--------------------------
    #   Reformat dataframe
    #--------------------------
    df = pd.read_html(html)[3]
    df['Team Name'] = team

    #--------------------------
    #   Append team_stats table
    #--------------------------
    engine = create_engine(f'postgresql://data_load:{password}@localhost:5432/reporting',client_encoding='utf8')
    df.to_sql(schema='nba', name='team_stats', con=engine, index=False, if_exists='append')


#-----------------------------
# Main Routine & Exec
#-----------------------------
def main():

    team_names = [
        'BOS', 'ATL', 'BRK', 'CHO', 'CHI',
        'CLE', 'DAL', 'DEN', 'DET', 'GSW',
        'HOU', 'IND', 'LAC', 'LAL', 'MEM',
        'MIA', 'MIL', 'MIN', 'NOP', 'NYK',
        'OKC', 'ORL', 'PHI', 'PHO', 'POR',
        'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
    ]

    password = pgpasslib.getpass('localhost', 5432, 'reporting', 'data_load')
    check_for_schema(password)
    drop_team_data(password)

    for team in team_names:
        get_player_data(team, password)
        get_team_data(team, password)

if __name__ == '__main__':
    main()
