#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from db import connect_to_db, create_db_engine
import numpy as np


def get_data(connection, season=None, multiple_parts=False, pbp_table="mpbp"):
    """
    Extract the needed data from the pbp, event_goal and reward tables.

    Author: Jon Vik
    Updates by: Rasmus Säfvenberg
   
    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    multiple_parts : boolean
        Whether to consider multiple seasons worth of data.
        The default is False.
    pbp_table : string.
        Name of the play by play SQL table to be used. 
        The default is "mpbp".
        
    Returns
    -------
    df : pandas.DataFrame
        Data frame containing all goals with metadata regarding the game
        and players on the ice during the goal.

    """
        
    # Specify the amount of players in database for each team
    HOMEPLAYERS = ['HomePlayer{}'.format(n) for n in range(1, 10)]
    AWAYPLAYERS = ['AwayPlayer{}'.format(n) for n in range(1, 10)]
    
    # Specify strings to make the query more readable
    home_string = ', '.join(HOMEPLAYERS)
    away_string = ', '.join(AWAYPLAYERS)
    
    # Retrieve the needed data to group rewards
    query = f"""
    SELECT pbp.GameId, pbp.AwayTeamId, pbp.HomeTeamId, pbp.EventNumber, pbp.PeriodNumber, 
           r.TotalElapsedTime, r.GD, r.MD, 
           g.ScoringTeam, g.ScoringTeamId, g.Disposition, g.GoalScorerId, 
           r.reward, g.FirstAssistId, g.SecondAssistId, {away_string}, {home_string}
    FROM {pbp_table} pbp, event_goal g, reward r
    WHERE pbp.GameId = g.GameId AND pbp.GameId = r.GameId
          AND pbp.PeriodNumber >= 1 AND pbp.PeriodNumber <= 3 
          AND pbp.ExternalEventId = g.GoalId
          AND pbp.TotalElapsedTime = r.TotalElapsedTime"""
    
    # Read data as data frame
    df = pd.read_sql(query, con=connection)
    
    # Convert columns to desired format
    df[HOMEPLAYERS] = df[HOMEPLAYERS].astype('Int64')
    df[AWAYPLAYERS] = df[AWAYPLAYERS].astype('Int64')
    
    return df


def calc_plus_minus(data):
    """
    Calculate the total traditional and weighted plus-minus statistic for all
    players, grouped by player id.

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame as retrieved by weighted.get_data()

    Returns
    -------
    weighted_plus_minus : pandas.DataFrame
        A data frame with total plus-minus and weighted plus-minus per player.

    """

    df = data.copy()
    # Specify the amount of players in database for each team
    HOMEPLAYERS = ['HomePlayer{}'.format(n) for n in range(1, 10)]
    AWAYPLAYERS = ['AwayPlayer{}'.format(n) for n in range(1, 10)]
    
    # Calculate plus-minus from the away team perspective
    df["PlusMinusAway"] = np.select(
        [
             # Traditional
            (df["ScoringTeamId"] != df["HomeTeamId"]) & (df["MD"] >= 0), 
            (df["ScoringTeamId"] != df["HomeTeamId"]) & (df["MD"] < 0), 
            (df["ScoringTeamId"] == df["HomeTeamId"]) & (df["MD"] > 0), 
            (df["ScoringTeamId"] == df["HomeTeamId"]) & (df["MD"] <= 0), 
        ], 
        [
            1, 
            0, 
            0, 
            -1,
        ], 
        default=999
    )

    # Calculate plus-minus from the home team perspective
    df["PlusMinusHome"] = np.select(
        [     
             # Traditional
            (df["ScoringTeamId"] == df["HomeTeamId"]) & (df["MD"] <= 0), 
            (df["ScoringTeamId"] == df["HomeTeamId"]) & (df["MD"] > 0), 
            (df["ScoringTeamId"] != df["HomeTeamId"]) & (df["MD"] < 0), 
            (df["ScoringTeamId"] != df["HomeTeamId"]) & (df["MD"] >= 0), 
        ], 
        [
            1, 
            0, 
            0, 
            -1,
        ], 
        default=999
    )

    
    # Indicator variable for whether the home team scored
    df["ScoringTeam"] = np.where(df["ScoringTeamId"] == df["HomeTeamId"], "Home", "Away")

    # Get players for both team and plusminus values
    plusminus = pd.concat([df[HOMEPLAYERS+AWAYPLAYERS], 
                           df[["GameId", "TotalElapsedTime", 
                               "PlusMinusHome", "PlusMinusAway", "reward",
                               "ScoringTeam"]]], 
                          axis=1)

    # Rename the columns and convert from wide to long
    plusminus = plusminus.rename(columns={"HomePlayer1": "Home", "HomePlayer2": "Home",
                                          "HomePlayer3": "Home", "HomePlayer4": "Home",
                                          "HomePlayer5": "Home", "HomePlayer6": "Home",
                                          "HomePlayer7": "Home", "HomePlayer8": "Home",
                                          "HomePlayer9": "Home", 
                                          "AwayPlayer1": "Away", "AwayPlayer2": "Away",
                                          "AwayPlayer3": "Away", "AwayPlayer4": "Away",
                                          "AwayPlayer5": "Away", "AwayPlayer6": "Away",
                                          "AwayPlayer7": "Away", "AwayPlayer8": "Away",
                                          "AwayPlayer9": "Away"
                                          }).melt(id_vars=("GameId", "TotalElapsedTime", 
                                                           "PlusMinusHome", "PlusMinusAway",
                                                           "reward", "ScoringTeam"),
                                                  var_name="Side",
                                                  value_name="PlayerId")
    
    # Drop NA values for PlayerId
    plusminus = plusminus.loc[~plusminus["PlayerId"].isna()]
                              
    # Set negative reward if the opposition scored
    plusminus.loc[plusminus["Side"] != plusminus["ScoringTeam"], "reward"] *= -1 

    # Set reward to 0 if no plus-minus was awarded and since if one team has 0
    # plus-minus the other team also has 0 it suffices to check one side.
    plusminus.loc[plusminus["PlusMinusHome"] == 0, "reward"] = 0 

    # Calculate plus-minus while playing home and away respectively                            
    home_pm = plusminus[plusminus["Side"] == "Home"].groupby(["PlayerId"])\
        [["PlusMinusHome", "reward"]].sum().reset_index()\
            .rename(columns={"PlusMinusHome": "PlusMinus"})
            
    away_pm = plusminus[plusminus["Side"] == "Away"].groupby(["PlayerId"])\
        [["PlusMinusAway", "reward"]].sum().reset_index()\
            .rename(columns={"PlusMinusAway": "PlusMinus"})

    # Calculate total plus-minus
    weighted_plus_minus = home_pm.append(away_pm).groupby("PlayerId").sum().\
        reset_index().rename(columns={"reward": "WeightedPlusMinus"}).\
            sort_values("WeightedPlusMinus", ascending=False)
    
    return weighted_plus_minus


def calc_goals(data):
    """
    Calculate the total traditional and weighted goals for all
    players, grouped by player id.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame as retrieved by weighted.get_data()

    Returns
    -------
    weighted_goals : pandas.DataFrame
        A data frame with total goals and weighted goals per player.

    """  
    # Get required columns
    goals = data[["GoalScorerId", "reward"]].copy()
    
    # Intialize new columns that means 1 goal per event.
    goals["GoalsScored"] = 1
    
    # Calculate number of goals and weighted goals per player
    weighted_goals = goals.groupby("GoalScorerId")[["GoalsScored", "reward"]].sum().reset_index().\
                          rename(columns={"GoalsScored": "Goals", 
                                          "reward": "WeightedGoals", 
                                          "GoalScorerId": "PlayerId"}).\
                              sort_values("WeightedGoals", ascending=False)
                              
    return weighted_goals


def calc_assists(data):
    """
    Calculate the total traditional and weighted assists for all
    players, grouped by player id.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame as retrieved by weighted.get_data()

    Returns
    -------
    weighted_assists : pandas.DataFrame
        A data frame with total assists and weighted assists per player.

    """
    
    # Get required columns
    assists = data[["FirstAssistId", "SecondAssistId", "reward"]].copy()
    
    # Convert from wide to long and have each assist (first/second) as a row
    assists = assists.rename(columns={"FirstAssistId": "AssistId", 
                            "SecondAssistId": "AssistId"}).melt(id_vars="reward").\
        rename(columns={"value": "PlayerId"}).drop("variable", axis=1)
    
    # Intialize new columns that means 1 assist per event.
    assists["AssistedGoals"] = 1
    
    # Calculate number of assists and weighted assists per player
    weighted_assists = assists.groupby("PlayerId")[["AssistedGoals", "reward"]].sum().reset_index().\
                          rename(columns={"AssistedGoals": "Assists", 
                                          "reward": "WeightedAssists"}).\
                              sort_values("WeightedAssists", ascending=False)
    return weighted_assists
  
    
def calc_first_assists(data):
    """
    Calculate the total traditional and weighted first assist for all
    players, grouped by player id.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    data : pandas.DataFrame
        A data frame as retrieved by weighted.get_data()

    Returns
    -------
    weighted_first_assists : pandas.DataFrame
        A data frame with total and weighted first assists per player.

    """
    
    # Get required columns
    first_assists = data[["FirstAssistId", "reward"]].copy()
    
    # Convert from wide to long and have each assist (first only) as a row
    first_assists = first_assists.rename(columns={"FirstAssistId": "AssistId"}).\
        melt(id_vars="reward").\
        rename(columns={"value": "PlayerId"}).drop("variable", axis=1)
    
    # Intialize new columns that means 1 assist per event.
    first_assists["AssistedGoals"] = 1
    
    # Calculate number of assists and weighted assists per player
    weighted_first_assists = first_assists.groupby("PlayerId")[["AssistedGoals", "reward"]].\
        sum().reset_index().rename(columns={"AssistedGoals": "First_Assists", 
                                            "reward": "WeightedFirst_Assists"}).\
                              sort_values("WeightedFirst_Assists", ascending=False)
                              
    return weighted_first_assists


def calc_points(goals, assists):
    """
    Calculate the total traditional and weighted points for all
    players, grouped by player id.
    
    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    goals : pandas.DataFrame
        A data frame with total goals and weighted assists per player.
    assists : pandas.DataFrame
        A data frame with total assists and weighted assists per player.

    Returns
    -------
    points : pandas.DataFrame
        A data frame with total points and weighted points per player.

    """
       
    # Specify columns to keep for merging
    goals = goals[["PlayerId", "PlayerName", "Position", "Goals", "WeightedGoals"]]
    assists = assists[["PlayerId", "PlayerName", "Position", "Assists", "WeightedAssists"]]
    
    # Combine goals and assists
    points = goals.merge(assists, on=["PlayerId", "PlayerName", "Position"], 
                         how="outer")
    
    # Fill missing values with 0 (some players only score goals etc.)
    points.fillna(0, inplace=True)
    
    # Calculate points = goals + assists
    points["Points"] = points["Goals"] + points["Assists"]
    
    # Calculate weighted points = weighted goals + weighted assists
    points["WeightedPoints"] = points["WeightedGoals"] + points["WeightedAssists"]

    # Sort by weighted points
    points.sort_values("WeightedPoints", ascending=False, inplace=True)
    
    return points


def add_names(weighted_metric, connection):
    """
    Add player names and group the specified metric by player name.

    Author: Rasmus Säfvenberg

    Parameters
    ----------
    weighted_metric : pandas.DataFrame
        A data frame containing PlayerId and a specified metric, both the 
        traditional and weighted version.
        connection : MySQLconnection as created by db.connect_to_db
            a connection to the SQL database we are working with.
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    Returns
    -------
    weighted_metric_named : pandas.DataFrame
        A data frame based on the weighted metric data frame, extended
        with player names.

    """

    # Query to obtain player metadata (e.g. name and position)
    query = "SELECT * FROM player"
    
    # Read metadata about all players
    players = pd.read_sql(query, connection)    

    # Keep only outfield players
    skaters = players[players["Position"] != "G"]
    
    # Add player names
    weighted_metric_named = weighted_metric.merge(skaters[["PlayerId", "PlayerName",
                                                           "Position"]], on="PlayerId")
    
    # Specify colums to be at the front of the data frame
    cols_to_move = ["PlayerId", "PlayerName", "Position"]
    
    # Reorder columns
    weighted_metric_named = weighted_metric_named[cols_to_move + [col for col in 
                                                                  weighted_metric_named.columns if
                                                                  col not in cols_to_move]]
    
    return weighted_metric_named


def add_ranks(weighted_metric, traditional_metric):
    """
    Add rankings (1 being the best) for both the traditional and weighted metrics.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    weighted_metric : pandas.DataFrame
        A data frame containing PlayerId and a specified metric, both the 
        traditional and weighted version.
    traditional_metric : string
        Name of the traditional metric.
        Supported values: Assists, Goals, Points, PlusMinus.
    
    Returns
    -------
    weighted_metric_rank : pandas.DataFrame
        A data frame based on the weighted metric data frame, extended
        with rankings for traditional and weighted mtetrics.

    """
    
    weighted_metric_rank = weighted_metric.copy().reset_index(drop=True)
            
    # Create a ranking according to the weighted metric
    weighted_metric_rank["Rank_w"] = range(1, len(weighted_metric_rank)+1)
    
    # Create a ranking according to the traditional metric (reset twice for correct value)
    weighted_metric_rank = weighted_metric_rank.sort_values(f"{traditional_metric}", 
                                                            ascending=False).\
        reset_index(drop=True).reset_index().rename(columns={"index": "Rank_trad"})
      
    # Add 1 to index to create rank
    weighted_metric_rank["Rank_trad"] += 1
    
    # Calculate rank difference between traditional and weighted
    weighted_metric_rank["Rank_diff"] = weighted_metric_rank["Rank_trad"] -  weighted_metric_rank["Rank_w"]
    
    # Specify colums to be at the front of the data frame
    cols_to_move = ["Rank_trad", "Rank_w", "Rank_diff"]
    
    # Reorder columns
    weighted_metric_rank = weighted_metric_rank[cols_to_move + [col for col in weighted_metric.columns if
                                                      col not in cols_to_move]]
    
    # Sort values by weighted rank
    weighted_metric_rank.sort_values("Rank_w", ascending=True, inplace=True)
  
    return weighted_metric_rank  


def add_to_sql(df, table_name, engine):
    """
    Add the data frames to SQL by their specified table names.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    df : pandas.DataFrame
        Data frame to be created as an SQL table.
    table_name : string
        Name of the table to be commited to SQL.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.
        
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    
    # Convert playerId to an integer instead of float
    df["PlayerId"] = df["PlayerId"].astype('Int64')
     
    # Round the weighted metrics to three decimal places
    df.iloc[:, -1] = df.iloc[:, -1].round(3)    
    
    # Add the table to SQL
    df.to_sql(f"{table_name}", engine, if_exists='replace', 
              chunksize=25000, index=False)


def create_weighted_metrics(connection, engine, season=None, suffix="",
                            multiple_parts=False, pbp_table="mpbp"):
    """
    Create the weighted metrics for the given season and add them to the 
    SQL database.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() & pd.read_sql().
    suffix : string
        What to append at the end of the table name in the SQL database.

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # If the data is to be evaluated on another season/data set
    if multiple_parts:
        pbp_table += "_eval"
        
    # Get the data with weighted reward
    df = get_data(connection, season, multiple_parts, pbp_table)
    
    # Calculate goals and add player names + ranks
    goals = add_ranks(add_names(calc_goals(df), connection), "Goals")
    
    # Calculate assists and add player names + ranks
    assists = add_ranks(add_names(calc_assists(df), connection), "Assists")
    
    # Calculate first assists and add player names + ranks
    first_assists = add_ranks(add_names(calc_first_assists(df), connection), "First_Assists")
    
    # Calculate points and add player names + ranks
    points = add_ranks(calc_points(goals, assists), "Points")

    # Calculate plus-minus and add player names + ranks
    plusminus = add_ranks(add_names(calc_plus_minus(df), connection), "PlusMinus")

    # Add tables to SQL    
    add_to_sql(goals,     f"weighted_goals_ranked{suffix}",     engine)
    add_to_sql(assists,   f"weighted_assists_ranked{suffix}",   engine)
    add_to_sql(first_assists, f"weighted_first_assists_ranked{suffix}",   engine)
    add_to_sql(points,    f"weighted_points_ranked{suffix}",    engine)
    add_to_sql(plusminus, f"weighted_plusminus_ranked{suffix}", engine)


if __name__ == '__main__':
    season = 2013
    suffix = ""
    
    # Create a connection and a database engine
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
        
    # Get the data with weighted reward
    df = get_data(connection, season, multiple_parts=False)
    
    # Calculate goals and add player names + ranks
    goals = add_ranks(add_names(calc_goals(df), connection), "Goals")
    
    # Calculate assists and add player names + ranks
    assists = add_ranks(add_names(calc_assists(df), connection), "Assists")
    
    # Calculate points and add player names + ranks
    points = add_ranks(calc_points(goals, connection), "Points")

    # Calculate plus-minus and add player names + ranks
    plusminus = add_ranks(add_names(calc_plus_minus(df), connection), "PlusMinus")

    # Add tables to SQL    
    add_to_sql(goals, f"weighted_goals_ranked{suffix}", engine)
    add_to_sql(assists, f"weighted_assists_ranked{suffix}", engine)
    add_to_sql(points, f"weighted_points_ranked{suffix}", engine)
    add_to_sql(plusminus, f"weighted_plusminus_ranked{suffix}", engine)
    

