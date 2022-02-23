#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from db import connect_to_db, create_db_engine


def cut_play_by_play(connection, season=None, start_date=None, end_date=None, 
                     multiple_parts=False, playoffs=False,
                     evaluation_season=None, start_date_evaluation=None,
                     pbp_table="mpbp"):
    """
    Limit the play by play data to fewer season(s)/smaller parts.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    start_date : integer value of format yyyymmdd
        The start date we are interested in examining.
        The default is None.
    end_date : integer value of format yyyymmdd
        The end date we are interested in examining.
        The default is None.
    multiple_parts : boolean
        Whether to consider multiple parts (seasons/partitions) worth of data.
        The default is False.
    playoffs : boolean
        To consider only playoffs or regular season.
        The default is False.
    evaluation_season : integer value of 4 characters (e.g. 2013)
        The season to evaluate the data specified in season on.
        The default is None.
    start_date_evaluation : integer value of format yyyymmdd
        The date to start the valiation set at. Will be between
        start_date_evaluation and end_date.
    pbp_table : string.
        Name of the play by play SQL table to be altered. The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # Ensure that the table is recreated each time
    drop_query = f"""DROP TABLE IF EXISTS {pbp_table}"""
    cursor.execute(drop_query)

    # Initialize a new play by play table in order to keep the original 
    # play by play table unchanged.
    query = f"""CREATE TABLE {pbp_table}(
        GameId INT(11),
        AwayTeamId INT(11), HomeTeamId INT(11),
        ActionSequence INT(11), EventNumber INT(11),
        PeriodNumber INT(11),
        EventTime TIME, EventType TEXT,
        ExternalEventId INT(11),
        AwayPlayer1 INT(11), AwayPlayer2 INT(11), AwayPlayer3 INT(11),
        AwayPlayer4 INT(11), AwayPlayer5 INT(11), AwayPlayer6 INT(11),
        AwayPlayer7 INT(11), AwayPlayer8 INT(11), AwayPlayer9 INT(11),
        HomePlayer1 INT(11), HomePlayer2 INT(11), HomePlayer3 INT(11),
        HomePlayer4 INT(11), HomePlayer5 INT(11), HomePlayer6 INT(11),
        HomePlayer7 INT(11), HomePlayer8 INT(11), HomePlayer9 INT(11),
        Date INT(11),
        PRIMARY KEY(GameId, EventNumber)
    ) """
    cursor.execute(query)
    
    # Copy data from the pre-existing play by play table for the given season
    # and dates
    query = f"""INSERT INTO {pbp_table}
                SELECT *
                FROM pbp_view WHERE """
    
    # If playoffs are to be examined
    if playoffs: 
        playoffs_query = " AND GameId LIKE '20__03%'"
    else:
        playoffs_query = " AND GameId LIKE '20__02%'"
    
    # Matches between two dates
    if start_date is not None and end_date is not None:
        if multiple_parts:
            if start_date_evaluation is not None:
                # start_date to start_date_evaluation
                query += f"Date BETWEEN '{start_date}' AND '{int(start_date_evaluation)-1}'"
            else: 
                # start_date_evaluation to end_date
                query += f"Date BETWEEN '{start_date}' AND '{end_date}'"
        else:
            query += f"Date BETWEEN '{start_date}' AND '{end_date}'"
    else:
        # Multiple parts/seasons
        if multiple_parts:
            if evaluation_season is not None:
                # all seasons until evaluation season
                query += f"""GameId > {season}020000 AND 
                             GameId < {evaluation_season}020000""" + playoffs_query
            else:
                # full evaluation_season
                query += f"""GameId > {season}020000 AND 
                             GameId < {season+1}020000""" + playoffs_query 
        elif not multiple_parts and season is not None: 
            query += f"GameId LIKE '{season}%'" + playoffs_query
        # Get all the data
        else: 
            query += "1=1"    
    cursor.execute(query)

    # Add new columns to be filled later
    column_query = f"""ALTER TABLE {pbp_table} 
    ADD TimeBin TEXT,
    ADD GoalsAgainst INT(11), ADD GoalsFor INT(11), 
    ADD GoalDiff INT(11), ADD GoalDiffAway INT(11),
    ADD ManpowerAway INT(11), ADD ManpowerHome INT(11), 
    ADD ManpowerDiff INT(11), ADD ManpowerDiffAway INT(11), 
    ADD Outcome TEXT AFTER ExternalEventId,
    ADD OutcomeAway TEXT AFTER ExternalEventId"""
    cursor.execute(column_query)
    
    # Commit all changes
    connection.commit()


def add_manpower(connection, pbp_table="mpbp"):
    """
    Counts the number of players on the ice and sets manpower fields in the 
    SQL table.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    pbp_table : string.
        Name of the play by play SQL table to be altered. The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # The possible sides for the teams.
    sides = ["Home", "Away"]
    
    for side in sides:
        # Count number of players on the ice at any given time.
        mp_query = f"""
        UPDATE {pbp_table} SET ManPower{side} = 
        (
        CASE WHEN {side}Player1 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player2 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player3 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player4 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player5 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player6 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player7 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player8 IS NOT NULL THEN 1 ELSE 0 END + 
        CASE WHEN {side}Player9 IS NOT NULL THEN 1 ELSE 0 END
        )
        """
        
        # Execute the query
        cursor.execute(mp_query)
        #print("Manpower", side, "done!")
    
    # Commit all changes
    connection.commit()


def add_gf_and_ga_fast(connection, pbp_table="mpbp"):
    """
    Add goals for and against from the home team perspective for each game.
    Note: As per the MySQL documentation, this method of solving it will be 
    deprecated in the future (Version 9.0 and later). 
        
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    drop_column : boolean
        Whether to drop the scoring team id column from the SQL table. 
        The default is False.
    pbp_table : string.
        Name of the play by play SQL table to be altered. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)

    # Ensure we recreate the table if it already exists
    drop_query = """DROP TABLE IF EXISTS mPBP_temp"""
    cursor.execute(drop_query)

    # Create a temporary table MySQL doesn't update with local variables.
    query = f"""CREATE TABLE mpbp_temp
            SELECT *
            FROM
            (SELECT rowTable.GameId, rowTable.AwayTeamId, rowTable.HomeTeamId,
            rowTable.ActionSequence, rowTable.EventNumber, rowTable.PeriodNumber,
            rowTable.EventTime, rowTable.EventType, rowTable.ScoringTeamId, 
            rowTable.ExternalEventId, rowTable.Outcome, rowTable.OutcomeAway, 
            rowTable.AwayPlayer1, rowTable.AwayPlayer2, rowTable.AwayPlayer3, 
            rowTable.AwayPlayer4, rowTable.AwayPlayer5, rowTable.AwayPlayer6,
            rowTable.AwayPlayer7, rowTable.AwayPlayer8, rowTable.AwayPlayer9,
            rowTable.HomePlayer1, rowTable.HomePlayer2, rowTable.HomePlayer3, 
            rowTable.HomePlayer4, rowTable.HomePlayer5, rowTable.HomePlayer6,
            rowTable.HomePlayer7, rowTable.HomePlayer8, rowTable.HomePlayer9,
            rowTable.Date, rowTable.TimeBin, 
            rowTable.AwayTeamScore as GoalsAgainst,
            rowTable.HomeTeamScore as GoalsFor,
            rowTable.GoalDiff, rowTable.GoalDiffAway, 
            rowTable.ManpowerAway, rowTable.ManpowerHome,
            rowTable.ManpowerDiff, rowTable.ManpowerDiffAway
            FROM
            (SELECT 
                g.*,
                @curr_game := g.GameId AS CurrGameId,
                @curr_goal := g.ScoringTeamId AS CurrScoringTeamId,
                @curr_home := g.HomeTeamId AS HomeTeamIdCurr,
                @curr_away := g.AwayTeamId AS AwayTeamIdCurr,#
                @home_team_score := IF(@curr_game != @prev_game, 
                      IF(@curr_home = @curr_goal AND @prev_game = g.GameId, 
                            @home_team_score + 1, IF(@curr_home = @curr_goal, 1, 0)),
                      IF(@prev_home = @curr_goal, @home_team_score + 1, @home_team_score)
                                ) AS HomeTeamScore,
                @away_team_score := IF(@curr_game != @prev_game, 
                      IF(@curr_away = @curr_goal AND @prev_game = g.GameId, 
                            @away_team_score + 1, IF(@curr_away = @curr_goal, 1, 0)),
                      IF(@prev_away = @curr_goal, @away_team_score + 1, @away_team_score)
                                ) AS AwayTeamScore,
                @prev_home := g.HomeTeamId AS PrevHomeTeamId,
                @prev_away := g.AwayTeamId AS PrevAwayTeamId,
                @prev_game := g.GameId AS PrevGameId
            FROM {pbp_table} g,
            (SELECT @home_team_score := 0) hscore,
            (SELECT @away_team_score := 0) ascore,
            (SELECT @curr_goal := '') cgoal,
            (SELECT @prev_game := 0) pgame,
            (SELECT @curr_game := 0) cgame,
            (SELECT @prev_home := '') phome,
            (SELECT @prev_away = '') paway,
            (SELECT @curr_home = '') chome,
            (SELECT @curr_away = '') caway) as rowTable) as results;"""

    cursor.execute(query)

    # Remove original table data
    query = f"DELETE FROM {pbp_table};"
    cursor.execute(query)

    # Recreate original table with new column added
    query = f"""
    INSERT INTO {pbp_table}
    SELECT *
    FROM mpbp_temp;"""
    cursor.execute(query)

    # Drop temporary table
    drop_query = "DROP TABLE mpbp_temp;"
    cursor.execute(drop_query)

    # Remove goals at the actual goal event as they count after the face-off.
    query = f"""UPDATE {pbp_table}
    SET GoalsFor = GoalsFor - 1
    WHERE EventType = "GOAL" AND ScoringTeamId = HomeTeamId;"""
    cursor.execute(query)

    query = f"""UPDATE {pbp_table}
    SET GoalsAgainst = GoalsAgainst - 1
    WHERE EventType = "GOAL" AND ScoringTeamId = AwayTeamId;"""
    cursor.execute(query)

    connection.commit()
    
    
def remove_shootout_goals(connection, pbp_table="mpbp"):
    """
    Remove goals scored in a shoot-out, i.e. shots & goals during period 5.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    pbp_table : string.
        Name of the play by play SQL table to be altered. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # Delete shootout goals and shots from the data
    query = f"""DELETE FROM {pbp_table} 
    WHERE PeriodNumber = 5 AND 
    (EventType = 'GOAL' OR EventType = 'SHOT')"""
    
    # Execute query and commit changes.
    cursor.execute(query)
    connection.commit()
    

def add_outcome_to_pbp_fast(connection, engine, pbp_table="mpbp"):
    """
    Adds a column to the play by play table with the outcome of each game from
    both the perspective of the home and away team by calculating the goals
    scored by each team.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.
    pbp_table : string.
        Name of the play by play SQL table to be altered. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Get all play by play data
    df_pbp = pd.read_sql(f"SELECT * FROM {pbp_table}", connection)
   
    # Get the final event of the game
    game_end = df_pbp[df_pbp["EventType"].isin(["PERIOD END", "GAME END"])].\
        drop_duplicates("GameId", keep="last").copy()
    #df_pbp[df_pbp["EventType"] == "GAME END"].copy()
    # Above is uncommented since some games have GAME END before PERIOD END
    
    # Specify the outcome for home team at the end of the game
    game_end["Outcome"] = np.select(
        [
            (game_end["GoalsFor"] > game_end["GoalsAgainst"]) & (game_end["PeriodNumber"] <= 3), 
            (game_end["GoalsFor"] < game_end["GoalsAgainst"]) & (game_end["PeriodNumber"] <= 3), 
            (game_end["GoalsFor"] > game_end["GoalsAgainst"]) & (game_end["PeriodNumber"] > 3), 
            (game_end["GoalsFor"] < game_end["GoalsAgainst"]) & (game_end["PeriodNumber"] > 3), 
        ], 
        [
            "win", 
            "loss",
            "tie-win",
            "tie-loss"
        ], 
        default='Unknown'
    )

    # Reverse the outcome for the away team
    game_end["OutcomeAway"] = game_end["Outcome"].replace({'^win': 'loss', 
                                                           '^loss': 'win',
                                                           'tie-win': 'tie-loss', 
                                                           'tie-loss': 'tie-win'}, 
                                regex=True)

    # Combine the correct two dataframes
    df_pbp = df_pbp.merge(game_end[["GameId", "Outcome", "OutcomeAway"]], 
                          on = "GameId")

    # Replace NULL columns with actual values
    df_pbp["Outcome_x"] = df_pbp["Outcome_y"]
    df_pbp["OutcomeAway_x"] = df_pbp["OutcomeAway_y"]
    
    # Drop redundant columns
    df_pbp.drop(["Outcome_y", "OutcomeAway_y"], inplace=True, axis=1)
    
    # Rename columns to match rest of code
    df_pbp.rename(columns={"Outcome_x": "Outcome", 
                           "OutcomeAway_x": "OutcomeAway"}, 
                  inplace=True)
    
    # Convert from timedelta
    df_pbp["EventTime"] = df_pbp["EventTime"].astype(str)
    
    # Keep only unique values
    df_pbp.drop_duplicates("GameId", inplace=True)
    
    # Create temporary table     
    df_pbp[["GameId", "Outcome", "OutcomeAway"]].to_sql("temp_table", engine, 
                                                        if_exists='replace', 
                                                        chunksize=10000,
                                                        index=False)
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    connection.commit()
    
    # Join the original and temporary table
    query = f"""
    UPDATE {pbp_table} m, temp_table t 
    SET m.Outcome = t.Outcome, m.OutcomeAway = t.OutcomeAway
    WHERE m.GameId = t.GameId;
    """
    
    # Execute query
    cursor.execute(query)
    
    # Drop temporary table
    drop_query = "DROP TABLE temp_table;"
    cursor.execute(drop_query)
    
    # Commit all changes.
    connection.commit()


def add_goal_mp_differential(connection, pbp_table="mpbp"):
    """
    Add new columns to the play by play table with Goal & Manpower difference 
    from both the home and away team perspective.     
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    pbp_table : string.
        Name of the play by play SQL table to be altered. The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # Calculate the differences for goals and manpower for each team.
    diff_query = f"""UPDATE {pbp_table} 
    SET ManpowerDiff = (ManpowerHome - ManpowerAway),  
    GoalDiff = (GoalsFor - GoalsAgainst),
    ManpowerDiffAway = (ManpowerAway - ManpowerHome),  
    GoalDiffAway = (GoalsAgainst - GoalsFor)"""
    
    # Execute the query and commit changes
    cursor.execute(diff_query)
    connection.commit()


def add_scoring_team_id(connection, drop_column=False, pbp_table="mpbp"):
    """
    Add team id of the scoring team to the play by play table in order to 
    easier to identify scoring team and whether home or away team scored.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    drop_column : boolean
        Whether to drop the scoring team id column from the SQL table. 
        The default is False.
    pbp_table : string.
        Name of the play by play SQL table to be altered. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # If the column already exists we should drop it
    if drop_column:
        query = f"ALTER TABLE {pbp_table} DROP COLUMN ScoringTeamId"
        cursor.execute(query)

    # Add the column scoring team id to the table
    query = f"ALTER TABLE {pbp_table} ADD COLUMN ScoringTeamId INT AFTER EventType"
    cursor.execute(query)

    # Update the SQL table with ScoringTeamId column from event_goal table.
    query = f"""
        UPDATE {pbp_table} p
        INNER JOIN event_goal g
        ON p.GameId = g.GameId AND p.EventNumber = g.EventNumber
        SET p.ScoringTeamId = g.ScoringTeamId
        """
    
    # Execute the query and commit changes
    cursor.execute(query)
    connection.commit()


def add_total_elapsed_time(connection, drop_column=False, pbp_table="mpbp"):
    """
    Add total elapsed time during the match as a column to the SQL table.
    Total elapsed time is 0-1200 for period 1, 1201-2400 for period 2, 
    2401-3600 for period 3 and so on.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    drop_column : boolean
        Whether to drop the scoring team id column from the SQL table. 
        The default is False.
    pbp_table : string.
        Name of the play by play SQL table to be altered. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)

    # If the column already exists we should drop it
    if drop_column:
        query = f"ALTER TABLE {pbp_table} DROP COLUMN TotalElapsedTime"
        cursor.execute(query)

    # If using seconds
    query = f"ALTER TABLE {pbp_table} ADD COLUMN TotalElapsedTime int AFTER EventTime"
    cursor.execute(query)

    # Calculate total elapsed time during the match
    query = f"""
       UPDATE {pbp_table} 
       SET TotalElapsedTime = Time_to_sec(EventTime) + (1200 * (PeriodNumber - 1))
       """
       
    # Execute the query and commit changes
    cursor.execute(query)
    connection.commit()


def extract_season(connection, engine, season=None, 
                   start_date=None, end_date=None, 
                   multiple_parts=False, playoffs=False, 
                   evaluation_season=None, start_date_evaluation=None, 
                   pbp_table="mpbp"):
    """
    Extract the season and all required data for the given season.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    start_date : integer value of format yyyymmdd
        The start date we are interested in examining.
    end_date : integer value of format yyyymmdd
        The end date we are interested in examining.
    multiple_parts : boolean
        Whether to consider multiple parts (seasons/partitions) worth of data.
        The default is False.
    playoffs : boolean
        To consider only playoffs or regular season.
        The default is False.
    evaluation_season : integer value of 4 characters (e.g. 2013)
        The season to evaluate the data specified in season on.
        The default is None.
    start_date_evaluation : integer value of format yyyymmdd
        The date to start the valiation set at. Will be between
        start_date_evaluation and end_date.
    pbp_table : string.
        Name of the play by play SQL table to be used. 
        The default is "mpbp".
        
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    print("Extracting season and popuplating fields.")
    #print("Creating game table...")
    # create_game(connection, season)
        
    # Evaluate data from multiple seasons
    if multiple_parts:    
        n_tables = 2
    else:
        n_tables = 1
        
    # If we have multiple parts
    multiple_season = False
    # If we have multiple full seasons
    if evaluation_season is not None:
        multiple_season = True
        
    # "Loop" over the amount of tables to create; 2 if to be evaluated
    for table_count in range(n_tables):
        if table_count == 1:
            # Full seasons
            if multiple_season:
                if evaluation_season is not None:
                    season = evaluation_season
                    pbp_table += "_eval"
                    evaluation_season = None
                else: 
                    evaluation_season = season
            # Partitions
            else:
                if start_date_evaluation is not None:
                    start_date = start_date_evaluation
                    pbp_table += "_eval"
                    start_date_evaluation = None
                else: 
                    start_date_evaluation = start_date
                
        # Cut all data into smaller parts
        cut_play_by_play(connection, season, start_date, end_date, multiple_parts,
                         playoffs, evaluation_season, start_date_evaluation,
                         pbp_table=pbp_table)
        
        # Add the id of the scoring team
        add_scoring_team_id(connection, pbp_table=pbp_table)
        
        # Add manpower values
        add_manpower(connection, pbp_table=pbp_table)
        
        # Add goals for and against
        add_gf_and_ga_fast(connection, pbp_table=pbp_table)
        
        # Remove shootout goals
        remove_shootout_goals(connection, pbp_table=pbp_table)
        
        # Add outcomes to the play-by-play table
        add_outcome_to_pbp_fast(connection, engine, pbp_table=pbp_table)
        
        # Add goal and manpower differences
        add_goal_mp_differential(connection, pbp_table=pbp_table)
        
        # Add the total elapsed time in-game
        add_total_elapsed_time(connection, pbp_table=pbp_table)
        print("Fields have been populated!")


if __name__ == "__main__":
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    
    # # Check one full season
    season = 2012
    # extract_season(connection, engine, season, 
    #                multiple_parts=False, playoffs=False)

    # # One season between dates
    # extract_season(connection, engine, 
    #                start_date="20131001", end_date="20140713", 
    #                multiple_parts=False, playoffs=False)
    
    # Multiple seasons
    # extract_season(connection, engine, season, 
    #                multiple_parts=True, playoffs=True,
    #                evaluation_season=2013)
    
    # Multiple parts within a season
    extract_season(connection, engine, 
                   start_date="20140201", end_date="20140713", 
                   multiple_parts=True, playoffs=False,
                   start_date_evaluation="20140418")
