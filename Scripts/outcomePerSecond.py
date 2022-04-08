#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from db import connect_to_db, create_db_engine
from tqdm import tqdm

# Register tqdm to work with pandas
tqdm.pandas()

def occ_before(df, TotalElapsedTime, GD, MD, PeriodNumber):
    """
    Count all occurrences of the respective outcomes:
        [Win/Loss (Regulation) & Tie-win/Tie-loss (Overtime)]
    prior to the goal being scored.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    df : pandas.DataFrame
        The play by play data frame with the necessary columns.
    TotalElapsedTime : integer
        The total elapsed time in-game when the goal was scored.
    GD : integer
        The goal difference for the scoring team when the goal was scored.
    MD : integer
        The manpower difference for the scoring team when the goal was scored.
    PeriodNumber : integer
        The period number in which the goal occurred.

    Returns
    -------
    before : pandas.DataFrame
        A data frame with count of occurence for each combination of total 
        elapsed time, goal difference and manpower difference before the goal
        was scored.

    """
    
    # Get prior-goal events closest to time of the goal and keep only the last per game
    before_general = df[(df["TotalElapsedTime"] < TotalElapsedTime) & 
                        (df["TotalElapsedTime"] > 0)].\
        drop_duplicates("GameId", keep="last")
    	
    # Get the required goal and manpower difference for home team perspective
    before_home = before_general[(before_general["GD"] == GD) & 
                                 (before_general["MD"] == MD)]
    	
    # Count number of occurences for home team
    before_home = before_home.Outcome.value_counts()
    					
    # Get the required goal and manpower difference for away team perspective
    before_away = before_general[(before_general["GDaway"] == GD) &
                                 (before_general["MDaway"] == MD)]
    	
    # Count number of occurences for away team
    before_away = before_away.OutcomeAway.value_counts()
    
    # Combine home and away perspective											 
    before = before_home.append(before_away)
    
    # Sum up the values
    before = before.groupby(before.index).sum()
    	
    return before


def occ_after(df, TotalElapsedTime, GD, MD, PeriodNumber):
    """
    Count all occurrences of the respective outcomes:
        [Win/Loss (Regulation) & Tie-win/Tie-loss (Overtime)]
    after the goal was scored.

    Author: Rasmus Säfvenberg
    
    Parameters
    ----------
    df : pandas.DataFrame
        The play by play data frame with the necessary columns.
    TotalElapsedTime : integer
        The total elapsed time in-game when the goal was scored.
    GD : integer
        The goal difference for the scoring team when the goal was scored.
    MD : integer
        The manpower difference for the scoring team when the goal was scored.
    PeriodNumber : integer
        The period number in which the goal occurred.

    Returns
    -------
    after : pandas.DataFrame
        A data frame with count of occurence for each combination of total 
        elapsed time, goal difference and manpower difference after the goal
        was scored.

    """
    # Get post-goal events closest to time of the goal and keep only the first per game
    after_general = df[(df["TotalElapsedTime"] >= TotalElapsedTime) &
                       (df["EventType"] != "GOAL")].\
        drop_duplicates("GameId", keep="first")
    
    # Get the required goal and manpower difference for home team perspective
    after_home = after_general[(after_general["GD"] == GD) & 
                               (after_general["MD"] == MD)]
    	
    # Count number of occurences for home team
    after_home = after_home.Outcome.value_counts()
    
    # Get the required goal and manpower difference for away team perspective
    after_away = after_general[(after_general["GDaway"] == GD) & 
                               (after_general["MDaway"] == MD)]
    	
    # Count number of occurences for away team
    after_away = after_away.OutcomeAway.value_counts()
    	
    # Combine home and away perspective											 
    after = after_home.append(after_away)
    	
    # Sum up the values
    after = after.groupby(after.index).sum()
    	
    return after


def count_occurrences(connection, engine, multiple_parts=False,
                      pbp_table="mpbp"):
    """
    Count total occurrences for each outcome before and after tha goal was 
    scored and store in a data frame before committing to the SQL database.
    The occurrences are counted from the perspective of the scoring team.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() function.
    multiple_parts : boolean
        Whether to consider multiple seasons worth of data.
        The default is False.
    pbp_table : string.
        Name of the play by play SQL table to be used. 
        The default is "mpbp".

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # If the data is to be evaluated on another season/data set
    if multiple_parts:
        original_pbp_table = pbp_table
        pbp_table += "_eval"
        
    # Query to retrieve all the needed data from the play by play table
    query = f"""SELECT Outcome, OutcomeAway, GameId, HomeTeamId, AwayTeamId, 
                ScoringTeamId, EventNumber, EventType, PeriodNumber, TotalElapsedTime, 
                GoalDiff AS GD, GoalDiffAway AS GDaway,
                ManpowerDiff AS MD, ManpowerDiffAway AS MDaway
                FROM {pbp_table}
                WHERE TotalElapsedTime >= 0"""
	
    # All play by play events as a data frame
    df = pd.read_sql(query, con=connection)
    	
    # Subset only the goals
    df_goals = df[df["EventType"] == "GOAL"].copy()

    # If the data is to be evaluated on another season/data set
    if multiple_parts:
        query = query.replace(f"{pbp_table}", f"{original_pbp_table}")
        df_eval = df.copy(deep=True)
        df = pd.read_sql(query, con=connection)
    
    # Occurences prior to goal
    before = df_goals.progress_apply(lambda row: occ_before(df, row.TotalElapsedTime, 
                                                            row.GD, row.MD, 
                                                            row.PeriodNumber), axis=1)
    print("Finished before occurrences!")

    # Get state after goal (i.e. the face-off usually)
    state_after = pd.DataFrame()
    for row in df_goals.itertuples(index=False):
        if multiple_parts:
            state_after = pd.concat([state_after, 
                                     df_eval[(df_eval["GameId"] == row.GameId) & 
                                             (df_eval["EventNumber"] == row.EventNumber+1)]])
    
        else:
            state_after = pd.concat([state_after, 
                                     df[(df["GameId"] == row.GameId) & 
                                        (df["EventNumber"] == row.EventNumber+1)]])
    
    # Occurences after goal
    after = state_after.progress_apply(lambda row: occ_after(df, row.TotalElapsedTime, 
                                                             row.GD, row.MD, 
                                                             row.PeriodNumber), axis=1)
    
    print("Finished after occurrences!")

    # Boolean series of whether the home team scored
    homeTeamScored = df_goals["ScoringTeamId"] == df_goals["HomeTeamId"]
    
    # Goal difference from the perspective of the scoring team
    df_goals.loc[homeTeamScored, "GD_scoring_team"] = df_goals.loc[homeTeamScored, "GD"]
    df_goals.loc[~homeTeamScored, "GD_scoring_team"] = df_goals.loc[~homeTeamScored, "GDaway"]

    # Manpower difference from the perspective of the scoring team
    df_goals.loc[homeTeamScored, "MD_scoring_team"] = df_goals.loc[homeTeamScored, "MD"]
    df_goals.loc[~homeTeamScored, "MD_scoring_team"] = df_goals.loc[~homeTeamScored, "MDaway"]
    
    # Create occurence data frame with game id and total elapsed time
    occ_df = df_goals.loc[:, ["GameId", "TotalElapsedTime", 
                              "GD_scoring_team", "MD_scoring_team"]].copy().\
        reset_index(drop=True)

    # Reset index to align with before and after
    homeTeamScored.reset_index(drop=True, inplace=True)
    
    # Remove NA and reorder columns
    before = before.fillna(0)[["win", "loss", "tie-win", "tie-loss"]].reset_index(drop=True)
    after = after.fillna(0)[["win", "loss", "tie-win", "tie-loss"]].reset_index(drop=True)
    
    # Change place of win and loss if the away team scored
    before.loc[~homeTeamScored, ["win", "loss", "tie-win", "tie-loss"]] = \
        before.loc[~homeTeamScored, ["loss", "win", "tie-loss", "tie-win"]].values

    after.loc[~homeTeamScored, ["win", "loss", "tie-win", "tie-loss"]] = \
        after.loc[~homeTeamScored, ["loss", "win", "tie-loss", "tie-win"]].values    
        
    # Add before goal occurences to the data frame
    occ_df[["win_before", "loss_before", 
            "tie-win_before", "tie-loss_before"]] = before.\
        rename(columns={"loss": "loss_before", "tie-loss": "tie-loss_before", 
                        "tie-win": "tie-win_before", "win": "win_before"})	
    	
    # Add after goal occurences to the data frame
    occ_df[["win_after", "loss_after", 
            "tie-win_after", "tie-loss_after"]] = after.\
        rename(columns={"loss": "loss_after", "tie-loss": "tie-loss_after", 
                        "tie-win": "tie-win_after", "win": "win_after"})	
    
    # Rename columns
    occ_df = occ_df.rename(columns={"GD_scoring_team": "GD",
                                    "MD_scoring_team": "MD"})
    
    # Commit to SQL
    occ_df.to_sql("occurrences", engine, if_exists='replace', 
    			  chunksize=25000, index=False)
    	

if __name__ == "__main__":
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    count_occurrences(connection, engine, multiple_parts=True)




