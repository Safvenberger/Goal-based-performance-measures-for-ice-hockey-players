from bs4 import BeautifulSoup
import requests
import pandas as pd
from db import create_db_engine

# Script 0
def parse_data(season="2014"):
    """
    Extract dates for games played in the given season. The results are stored
    in a table named "match_date"
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2008 - 2015.
        Note: This number indicates the _end year_ of the season.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Request season games
    page = requests.get(f"https://www.hockey-reference.com/leagues/NHL_{season}_games.html")
    
    # Parse HTML
    soup = BeautifulSoup(page.text, "html.parser")
    
    # Date of the game
    dates = soup.find_all("th", attrs={"data-stat": "date_game"})
    dates = [i.get_text() for i in dates]
    
    # Delete dates that have the value "Date"
    drop_dates = [i for i, value in enumerate(dates) if value == "Date"]
    for date in sorted(drop_dates, reverse=True):
        del dates[date]
    
    # Home team stats
    home_team_name = soup.find_all("td", attrs={"data-stat": "home_team_name"})
    home_team_goals = soup.find_all("td", attrs={"data-stat": "home_goals"})
    
    # Away team stats
    away_team_name = soup.find_all("td", attrs={"data-stat": "visitor_team_name"})
    away_team_goals = soup.find_all("td", attrs={"data-stat": "visitor_goals"})
    
    # Team names
    home_team_name = [i.get_text() for i in home_team_name]
    away_team_name = [i.get_text() for i in away_team_name]
    
    # Get the goals scored
    home_team_goals = [i.get_text() for i in home_team_goals]
    away_team_goals = [i.get_text() for i in away_team_goals]
    
    # Find matches that were postponed
    drop_values = [i for i, value in enumerate(home_team_goals) if value == ""]
    if len(drop_values) == 1:
        drop_values = drop_values[0]
    
    # Delete the unwanted values
    del dates[drop_values]
    del home_team_name[drop_values]
    del away_team_name[drop_values]
    del home_team_goals[drop_values]
    del away_team_goals[drop_values]
    
    # Create data frame
    df = pd.DataFrame(list(zip(dates, home_team_name, away_team_name, 
                               home_team_goals, away_team_goals)),
                      columns=["Date", "HomeTeam", "AwayTeam", 
                               "HomeTeamScore", "AwayTeamScore"])
    
    # Add to SQL
    engine = create_db_engine("hockey")
    
    df.to_sql("match_date", engine, if_exists='replace', chunksize=25000, index=False)


def fix_match_date(connection):
    """
    Fix the date column in match_date table, created from parse_data.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    cursor = connection.cursor(buffered=True)
    # Convert date to int
    query = """UPDATE match_date as d
    SET d.Date = convert(date_format(str_to_date(Date, "%Y-%m-%d"), "%Y%m%d"), unsigned);
    """
    cursor.execute(query)
    
    # Add hometeam and awayteamID columns
    query = """ALTER TABLE match_date ADD COLUMN HomeTeamId int;"""
    cursor.execute(query)
    query = """ALTER TABLE match_date ADD COLUMN AwayTeamId int;"""
    cursor.execute(query)

    # Get teamId
    query = """UPDATE match_date AS d
    INNER JOIN team AS t ON d.HomeTeam = t.TeamName
    SET d.HomeTeamId = t.TeamId;"""
    cursor.execute(query)

    query = """UPDATE match_date AS d
    INNER JOIN team AS t ON d.AwayTeam = t.TeamName
    SET d.AwayTeamId = t.TeamId;"""
    cursor.execute(query)

    # Drop the missing value
    query = """DELETE FROM match_date 
    WHERE Date = 20140316 And HomeTeam = 'Chicago Blackhawks';"""
    cursor.execute(query)
    
    connection.commit()


def add_date_to_game(connection):
    """
    Add a date column to the game table.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    cursor = connection.cursor(buffered=True)

    # Add date and rownumber column
    query = """ALTER TABLE game ADD COLUMN Date INT;"""
    cursor.execute(query)
    
    query = """ALTER TABLE game ADD rowNumber int DEFAULT '0' NOT NULL;"""
    cursor.execute(query)
    
    # Add grouped by rownumber
    query = """UPDATE game as g
    INNER JOIN
    (SELECT @row_no := IF(@prev_val = g.Season, @row_no + 1, 1) AS rowNumber,
       @prev_val := g.Season AS Season,
       g.GameId
    FROM game g,
      (SELECT @row_no := 0) x,
      (SELECT @prev_val := '') y) as rowTable
    ON g.GameId = rowTable.GameId
    SET g.rowNumber = rowTable.rowNumber;"""
    cursor.execute(query)

    # Set RowNumber for match_date
    query = """ALTER TABLE match_date ADD rowNumber int DEFAULT '0' NOT NULL;"""
    cursor.execute(query)

    query = """SELECT @n:=0;"""
    cursor.execute(query)

    query = """UPDATE match_date SET rowNumber = @n := @n + 1;"""
    cursor.execute(query)

    # Join dates
    query = """UPDATE game  
    INNER JOIN match_date
    ON game.rowNumber = match_date.rowNumber    
    SET game.Date = match_date.Date
    WHERE game.GameId LIKE '2013%';"""
    cursor.execute(query)
    connection.commit()

    
def add_date_to_pbp(connection):   
    """
    Add date column to the play_by_play_events table.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    cursor = connection.cursor(buffered=True)

     # Add column
    query = """ALTER TABLE play_by_play_events ADD COLUMN Date int;"""
    cursor.execute(query)

    # Fill table with dates
    query = """UPDATE play_by_play_events as pbp
    LEFT JOIN game as g
    ON g.GameId = pbp.GameId
    SET pbp.Date = g.Date;"""
    cursor.execute(query)
    connection.commit()

if __name__ == "__main__":
    "Placeholder"