#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from db import connect_to_db, create_db_engine


def reward_func(win_before, loss_before, tie_win_before, tie_loss_before,
                win_after, loss_after, tie_win_after, tie_loss_after):
    """
    Returns the reward according to: 
    Reward = 2 [ P(win | state after goal) - P(win| state before goal)] 
            + 1 [P(tie | state after goal) - P(tie | state before goal)]]

    Author: Jon Vik
    Updates by: Rasmus Säfvenberg
    
    Parameters
    ----------
    win_before : integer
        Amount of occurrences of winning in regulation before the goal.
    loss_before : integer
        Amount of occurrences of losing in regulation before the goal.
    tie_win_before : integer
        Amount of occurrences of winning in overtime before the goal.
    tie_loss_before : integer
        Amount of occurrences of losing in overtime before the goal.
    win_after : integer
        Amount of occurrences of winning in regulation after the goal.
    loss_after : integer
        Amount of occurrences of losing in regulation after the goal.
    tie_win_after : integer
        Amount of occurrences of winning in overtime after the goal.
    tie_loss_after : integer
        Amount of occurrences of losing in overtime after the goal.

    Returns
    -------
    reward : float
        The reward calculated by the aforementioned formula.

    """
    # Calculate the total number of occurences prior to the goal
    all_before = win_before + loss_before + tie_win_before + tie_loss_before
    
    # Calculate the total number of occurences after the goal
    all_after = win_after + loss_after + tie_win_after + tie_loss_after

    if all_before == 0:
        p_win_before = 0
        p_tie_loss_before = 0
    else:
        # Probability of winning before the goal is scored
        p_win_before = (win_before + tie_win_before)/all_before
        # Probability of losing in overtime before the goal is scored
        p_tie_loss_before = tie_loss_before/all_before
    
    if all_after == 0:
        p_win_after = 0
        p_tie_loss_after = 0
    else:
        # Probability of winning after the goal is scored
        p_win_after = (win_after + tie_win_after)/all_after
        # Probability of losing in overtime after the goal is scored
        p_tie_loss_after = tie_loss_after/all_after    
    
    # Calculate the reward
    reward = 2 * (p_win_after - p_win_before) + 1 * (p_tie_loss_after - p_tie_loss_before)
    return reward


def apply_reward(connection, engine, position=[]):
    """
    Calculate the reward and assign it to a new SQL table called 'reward'.
    
    Author: Jon Vik
    Updates by: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    engine : sqlalchemy enginge as creadted by db.create_db_engine
        an engine object such that we can use pd.to_sql() & pd.read_sql().
    position : list
        A list of position(s) to calculate reward for.
        Default is an empty list (i.e. all positions).
        
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """

    # Select all occurrences from SQL table (Group by to remove dupes)
    #query = """SELECT * FROM occurrences"""
    query = """
    SELECT og.* FROM 
    (SELECT o.*, g.GoalScorerId FROM 
    occurrences o
    INNER JOIN event_goal g
    ON o.GameId = g.GameId 
    AND o.TotalElapsedTime = ((g.PeriodNumber - 1) * 20 * 60 + TIME_TO_SEC(g.EventTime)) 
    ) og
    INNER JOIN player p
    ON og.GoalScorerId = p.PlayerId"""
    #GROUP BY TotalElapsedTime, GD, MD"""
    
    # If we want to investigate per position
    if len(position) == 1:
        query += f" WHERE p.Position = '{position[0]}'"            
    elif len(position) == 2:
        query += f" WHERE p.Position = '{position[0]}' OR p.Position = '{position[1]}'"            
        
    # Read as a pandas data frame
    df = pd.read_sql(query, con=engine)
    
    # Calculate the reward for each row
    df["reward"] = df.apply(lambda row: reward_func(row["win_before"], 
                                                    row["loss_before"], 
                                                    row["tie-win_before"], 
                                                    row["tie-loss_before"],
                                                    row["win_after"], 
                                                    row["loss_after"],
                                                    row["tie-win_after"],
                                                    row["tie-loss_after"]), axis=1)
    # Commit new table to SQL
    df.to_sql("reward", engine, if_exists='replace', chunksize=25000, index=False)
    connection.commit()


def create_table_copy(connection, suffix):
    """
    Create copies of the table to store for future reference.
    
    Author: Rasmus Säfvenberg

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    suffix : string
        What to add as a suffix to the new SQL tables..

    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create a cursor to executy queries
    cursor = connection.cursor(buffered=True)
    
    # Delete tables if they already extist
    drop_query = f"DROP TABLE IF EXISTS mpbp{suffix}"
    cursor.execute(drop_query)
    
    drop_query = f"DROP TABLE IF EXISTS occurrences{suffix}"
    cursor.execute(drop_query)
    
    drop_query = f"DROP TABLE IF EXISTS reward{suffix}"
    cursor.execute(drop_query)
    
    # Create copies for the relevant tables
    query = f"CREATE TABLE mpbp{suffix} SELECT * FROM mpbp;"
    cursor.execute(query)
    
    query = f"CREATE TABLE occurrences{suffix} SELECT * FROM occurrences;"
    cursor.execute(query)
    
    query = f"CREATE TABLE reward{suffix} SELECT * FROM reward;"
    cursor.execute(query)
    
    connection.commit()


if __name__ == "__main__":
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    apply_reward(connection, engine)
    suffix = ""
    create_table_copy(connection, suffix)
