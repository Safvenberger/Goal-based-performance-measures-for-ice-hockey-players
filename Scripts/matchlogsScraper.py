#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import numpy as np
from db import connect_to_db, create_db_engine


def extract_game_information(game):
    """
    Extract the game information of interest available in the gamelogs
    at hockey-reference.com.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    game : string
        A string representation of the html tags returned by soup.find_all().

    Returns
    -------
    game_data : dictionary
        Key-value pairs with attribute: value, e.g. pen_min: 10.

    """
    # Extract the data columns
    game_data = [i for i in game.split() if re.findall("href|data-stat", i)]
    
    # Remove html tags not wanted
    game_data = [re.sub("\"|td|tr|/a|<a|/|<|boxscores|\.html|teams|", "", 
            re.sub("[>]", " ", i.split("=")[1])) for 
         i in game_data][2:]
    
    # Remove some unwanted list elements and combine into a new list
    game_data = ["date " + game_data[0][:8], game_data[2] + game_data[3][:3]] + game_data[4:]
    
    # Convert to a dict of key value pairs with nan for missing values
    game_data = {i.split()[0]: i.split()[1] if len(i.split()) != 1 else np.nan for i in game_data}
    
    return game_data


def scrape_team_gamelogs(team, season):
    """
    Scrape the gamelogs for a team during a given season from 
    hockey-reference.com

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    team : string
        Team acronym of length 3.
    season : integer
        Numeric value of length 4 indicating season.

    Returns
    -------
    df : pandas.DataFrame
        Data frame with all the games from a team during a season.

    """
    # Get the gamelogs for a given team during a given season
    page = requests.get(f"https://www.hockey-reference.com/teams/{team}/{season}_gamelog.html")
    
    # Parse HTML
    soup = BeautifulSoup(page.text, "html.parser")
    
    # Find all gamelogs
    gamelogs = soup.find_all("tr", id=re.compile("^tm_gamelog"))
    
    # Convert bs4 tags to string representation
    gamelogs = [str(i) for i in gamelogs]

    # Empty dictionary
    game_list = {}
    # Extract the needed information from each game
    for game in gamelogs:
        # Extract the game identifier, e.g. "rs.1" or "po.12"
        game_id = re.sub('\"', "", game[19:24])
        # Store the game information
        game_list[game_id] = extract_game_information(game)
    
    # Convert to a data frame
    df = pd.DataFrame.from_dict(game_list).T
    
    # Add team as the 2nd column (after date)
    df.insert(loc=1, column="team_name", value=team)
    
    # Add the season of the gamelogs
    df = df.assign(season=season)
    
    return df


def check_table_exists(connection, tablename="gamelogs"):
    """
    Check if a table exists in the database.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    tablename : string
        Name of the table to check existence of. The default is "gamelogs".

    Returns
    -------
    table_exists : bool
        True if the table exists, False otherwise.

    """
    cursor = connection.cursor(buffered=True)

    # Query to examine the existence of the table "gamelogs"
    query = f"""SELECT COUNT(*) 
    FROM information_schema.tables
    WHERE table_name = '{tablename}'
    """
    cursor.execute(query)

    # Check if table exists in the database
    table_exists = False
    if cursor.fetchone()[0] == 1: # i.e. exists
        table_exists = True
    
    return table_exists


def add_gamelogs_to_db(connection, engine):
    """
    Add a new table, gamelogs, to the database with information over
    GameId and corresponding dates.
    
    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """

    # List of all teams
    team_list = ['ANA', 'ARI', 'ATL', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 
                 'COL', 'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NJD', 
                 'NSH', 'NYI', 'NYR', 'OTT', 'PHI', 'PHX', 'PIT', 'SJS', 'STL', 
                 'TBL', 'TOR', 'VAN', 'WPG', 'WSH']
    
    # Team names in the SQL database
    team_names = ['ANAHEIM DUCKS', 'ARIZONA COYOTES', 'ATLANTA THRASHERS', 
                  'BOSTON BRUINS', 'BUFFALO SABRES', 'CAROLINA HURRICANES',
                  'COLUMBUS BLUE JACKETS', 'CALGARY FLAMES', 'CHICAGO BLACKHAWKS',
                  'COLORADO AVALANCHE', 'DALLAS STARS', 'DETROIT RED WINGS', 
                  'EDMONTON OILERS', 'FLORIDA PANTHERS', 'LOS ANGELES KINGS',
                  'MINNESOTA WILD', 'MONTREAL CANADIENS', 'NEW JERSEY DEVILS',
                  'NASHVILLE PREDATORS', 'NEW YORK ISLANDERS', 'NEW YORK RANGERS',
                  'OTTAWA SENATORS', 'PHILADELPHIA FLYERS', 'PHOENIX COYOTES',
                  'PITTSBURGH PENGUINS', 'SAN JOSE SHARKS', 'ST. LOUIS BLUES',
                  'TAMPA BAY LIGHTNING', 'TORONTO MAPLE LEAFS', 'VANCOUVER CANUCKS',
                  'WINNIPEG JETS', 'WASHINGTON CAPITALS']
    
    # List of all seasons
    season_list = list(range(2008, 2015))
    
    season_gamelog_dict = {}
    # Loop over all seasons to cover all seasons of interest
    for season in season_list:
        # Placeholder value
        season_gamelog_dict[season] = {}
        
        # Loop over all teams to get their gamelogs
        for team in team_list:
            # Skip teams that do not exist in a given season
            if (season <= 2014 and team == "ARI") or \
               (season >= 2015 and team == "PHX") or \
               (season <= 2011 and team == "WPG") or \
               (season >= 2012 and team == "ATL"):
                continue
            
            # Get gamelogs for specific team
            df = scrape_team_gamelogs(team, season)
            # Save the gamelog
            season_gamelog_dict[season][team] = df
    
    # All seasons in one data frame
    season_gamelog = pd.DataFrame()
    
    for season in season_list:
        # Combine the nested dataframes for each team
        season_gamelog = season_gamelog.append(
            pd.concat(season_gamelog_dict[season]).\
                reset_index(level=0, drop=True).reset_index()                    
            )
    
    #season_gamelog.to_csv("gamelogs.csv", index=False)
    
    # Team acronym to team name mapping 
    team_name_mapping = dict(zip(team_list, team_names))
    
    # Re-map the team names to match that of the database
    season_gamelog["team_name"] = season_gamelog["team_name"].map(team_name_mapping)
    season_gamelog["opp_name"] = season_gamelog["opp_name"].map(team_name_mapping)
    
    # Read the "team" table from the database
    team_query = "SELECT TeamId, TeamName FROM team"
    teams = pd.read_sql(team_query, con=connection)
    
    # Mapping for team names to team id
    team_id_mapping = dict(zip(teams["TeamName"], teams["TeamId"]))
    
    # Map the team names to a new column with team id
    season_gamelog["team_id"] = season_gamelog["team_name"].map(team_id_mapping)
    season_gamelog["opp_id"] = season_gamelog["opp_name"].map(team_id_mapping)

    # Add to SQL if it does not exist
    season_gamelog.to_sql("gamelogs", engine, if_exists='replace', 
                          chunksize=25000, index=False)
   

def merge_id_and_date(connection, engine):
    """
    Merge the GameId (from the database) and date (from webscraping)
    into a new table called 'gamedate'.

    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    query = """
        SELECT pl.GameId, min(gl.date) as date 
        FROM hockey.gamelogs as gl
        RIGHT JOIN
        	(SELECT even.GameId, team_id, opp_id, gd, opp_gd, shots, shots_against, 
                    pen_min, pen_min_opp, season
        	 FROM
        		(SELECT TeamId as team_id, GameId, GoalDifference as gd, 
                        TotalShots as shots, PenaltyMinutes as pen_min, 
                        SUBSTR(GameId, 1, 4) as season
        		 FROM
        			(SELECT *, ROW_NUMBER() OVER (ORDER BY GameId) as row_num # Create row numbering
        			FROM plays_in) as p
         	 	 WHERE p.row_num % 2 != 0 # Uneven rows, i.e. "team"
        		 ) as even
        	INNER JOIN
        		(SELECT GameId, TeamId as opp_id, GoalDifference as opp_gd, 
                        TotalShots as shots_against, PenaltyMinutes as pen_min_opp
        		 FROM
        			(SELECT *, ROW_NUMBER() OVER (ORDER BY GameId) as row_num # Create row numbering
        			 FROM plays_in) as p
        		 WHERE p.row_num % 2 = 0 # Even rows, i.e. "opponent team"
        		 ) as uneven
        	ON even.GameId = uneven.GameId	
            WHERE even.season >= 2007 AND even.season < 2014  # AND SUBSTR(even.GameId, 5, 2) = "02"
        ) as pl 
        ON gl.team_id = pl.team_id 
        AND gl.opp_id = pl.opp_id 
        AND gl.season-1 = pl.season
        AND (gl.goals - gl.opp_goals = pl.gd)
        AND gl.shots = pl.shots 
        AND gl.shots_against = pl.shots_against
        GROUP BY GameId;
        """
        
    # Get all the GameIds and dates
    game_id_dates = pd.read_sql(query, connection).sort_values("GameId", 
                                                               ascending=False)

    # Dates not added automatically => Add manually
    date_fix_dict = {2013020539: 20131221,
                     2013020438: 20131207,
                     2013020183: 20131029,
                     2013020120: 20131019,
                     2012020446: 20130321,
                     2011021036: 20120312,
                     2011020191: 20111119,
                     2011020120: 20111025,
                     2010020600: 20110106,
                     2010020457: 20101215,
                     2009020410: 20091204,
                     2009020117: 20091021,
                     2008020697: 20090121,
                     2008020380: 20081206,
                     2008020157: 20081101,
                     2008020151: 20081101,
                     2007020788: 20080202,
                     2007020641: 20080110,
                     2007020596: 20080103,
                     2007020425: 20071208,
                     2007020386: 20071203,
                     2007020373: 20071201,
                     2007020315: 20071123
                     }

    # Replace the NA values
    game_id_dates.loc[game_id_dates.date.isna(), "date"] = list(date_fix_dict.values())
    
    # Save it in the database
    game_id_dates.to_sql("gamedate", engine, if_exists='replace',
                         chunksize=25000, index=False)


def create_pbp_view(connection):   
    """
    Create a view of the considered play by play data.
    
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
    
    # Drop the view if it already exists
    drop_query = "DROP VIEW IF EXISTS pbp_view"
    cursor.execute(drop_query)
    
    # Create a view of play by play data with the date added
    query = """CREATE VIEW pbp_view AS
               SELECT pbp.*, g.date FROM play_by_play_events as pbp
               INNER JOIN gamedate as g
               ON g.GameId = pbp.GameId
               WHERE pbp.GameId > 2007010000;"""
    
    # Commit changes
    cursor.execute(query)
    connection.commit()


if __name__ == "__main__":
    # Database connections
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    
    #Check if the table 'gamelogs' already exists
    table_exists = check_table_exists(connection, tablename="gamelogs")
    
    if not table_exists:
        # Get all the gamelogs and add them to the database 
        add_gamelogs_to_db(connection, engine)
    
        # Close the connection
        connection.close()
        
        # Re-open connection
        connection = connect_to_db("hockey")

        # Merge the GameId and dates
        merge_id_and_date(connection, engine)
    
    #Check if the view 'pbp_view' already exists
    view_exists = check_table_exists(connection, tablename="pbp_view")
    
    if not view_exists:    
        # Creat a view for the play by play data 
        create_pbp_view(connection)