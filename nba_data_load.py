#--------------------------------------------------------------------------------
#   Program: NBA Stat Scraper
#   Purpose: Scrape NBA team stats reformat data & load into PostgreSQL
#   Author : Nolan Cotton
#--------------------------------------------------------------------------------
import pandas as pd
import getpass as gp
import re
import psycopg2
from sqlalchemy import create_engine

#---------------------------------------------
#   Checks for schema prior to data load
#---------------------------------------------
def check_for_schema(password):
    engine = create_engine('postgresql://ncotton:' + password + '@dev-pgsql01.postgres.database.azure.com:5432/nba_stat',client_encoding='utf8')
    conn = engine.connect()
    try:
        res = conn.execute('select count(1) from information_schema.schemata where schema_name = \'nba\'')
        if res.fetchone()[0] != 1:
            print('NBA schema not yet created, creating now')
            conn.execute('create schema nba')
        else:
            print('NBA schema present, skipping creation')
    except psycopg2.OperationalError as e:
        print("Unable to query dev-pgsql01", e)

#---------------------------------------------
#   Checks for table prior to data load
#---------------------------------------------
def check_for_tbl(password):
    engine = create_engine('postgresql://ncotton:' + password + '@dev-pgsql01.postgres.database.azure.com:5432/nba_stat',client_encoding='utf8')
    conn = engine.connect()
    try:
        res = conn.execute('select count(1) from information_schema.tables where table_name = \'player_per_game\' and table_schema = \'nba\'')
        if res.fetchone()[0] == 1:
            pg_truncate(password)
        else:
            print('nba.player_per_game not present, skipping truncation')
    except psycopg2.OperationalError as e:
        print("Unable to query dev-pgsql01", e)

#---------------------------------------------
#   Truncates table prior to data load
#---------------------------------------------
def pg_truncate(password):
    engine = create_engine('postgresql://ncotton:' + password + '@dev-pgsql01.postgres.database.azure.com:5432/nba_stat',client_encoding='utf8')
    conn = engine.connect()
    try:
        conn.execute('TRUNCATE TABLE nba.player_per_game')
    except psycopg2.OperationalError as e:
        print("Unable to Truncate...", e)

#---------------------------------------------
#   Retrieves player data for provided team
#---------------------------------------------
def get_player_data(team, password):

    #-----------------------------------------
    #   Get Player Data from URL
    #-----------------------------------------
    url = 'https://www.basketball-reference.com/teams/' + team + '/2021.html'
    df = pd.read_html(url)[1]
    df['Team Name'] = team

    #-----------------------------------------
    #   Fix column names
    #-----------------------------------------
    pattern = re.compile('%')
    for col in df:
        if re.search(pattern, col):
            df.rename(columns={col: col.replace('%', 'PCT')}, inplace=True)

    #-----------------------------------------
    #   Connect to Azure DB and load data
    #-----------------------------------------
    engine = create_engine('postgresql://ncotton:' + password + '@dev-pgsql01.postgres.database.azure.com:5432/nba_stat',client_encoding='utf8')
    df.to_sql(
        schema='nba',
        name='player_per_game',
        con=engine,
        index=False,
        if_exists='append'
    )

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

    password = gp.getpass('Enter admin password ncotton@dev-pgsql01: ')

    check_for_schema(password)
    check_for_tbl(password)

    for team in team_names:
        get_player_data(team, password)

if __name__ == '__main__':
    main()
