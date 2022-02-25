#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Rasmus Säfvenberg

import pandas as pd
import numpy as np
from time import perf_counter
from db import connect_to_db, create_db_engine
from populateFields import extract_season
from outcomePerSecond import count_occurrences
from reward import apply_reward, create_table_copy
from weighted import create_weighted_metrics
from matchlogsScraper import check_table_exists, add_gamelogs_to_db, \
    merge_id_and_date, create_pbp_view


def apply_weighted_reward(season=None, suffix="", 
                          start_date=None, end_date=None,
                          create_copy=True, 
                          calc_occur=True,
                          position_list=[],
                          multiple_parts=False, playoffs=False,
                          evaluation_season=None, start_date_evaluation=None
                          ):
    """
    Extract the season, calculate occurrences, apply the reward function and 
    finally, calculate the weighted metrics and assign to the SQL database.

    Parameters
    ----------
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2013.
    suffix : string
        What to append at the end of the table name in the SQL database.
    start_date : integer value of format yyyymmdd
        The start date we are interested in examining.
    end_date : integer value of format yyyymmdd
        The end date we are interested in examining.
    create_copy : boolean
        If copies of the table, ending with suffix, should be added to the database. 
        The default is True.
    calc_occur : boolean
        Whether to create the mpbp table and calculate occurrences or simply
        apply the reward based on an already defined occurrences table.
    position_list : list
        A list of position(s) to calculate reward for.
        Default is an empty list (i.e. all positions).
    multiple_parts : boolean
        Whether to consider multiple seasons worth of data.
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
    
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Time to see how long the execution takes
    time_start = perf_counter()
    
    # Create database connection and engine
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    
    # Check if the table gamelogs exists
    table_exists = check_table_exists(connection, "gamelogs")
    
    if not table_exists:
        print("Creating table gamelogs")
        # Get all the gamelogs and add them to the database 
        add_gamelogs_to_db(connection, engine)
    
        # Merge the GameId and dates
        merge_id_and_date(connection, engine)
        
    #Check if the view 'pbp_view' already exists
    view_exists = check_table_exists(connection, tablename="pbp_view")
    
    if not view_exists:    
        print("Creating view pbp_view")
        # Creat a view for the play by play data 
        create_pbp_view(connection)
    
    if calc_occur:
        # Extract the season
        extract_season(connection, engine, season, 
                       start_date, end_date, 
                       multiple_parts, playoffs,
                       evaluation_season, start_date_evaluation)
    
        print("Counting occurrences...")
        # Count occurrences
        count_occurrences(connection, engine, multiple_parts)
        
    print("Applying reward...")
    # Apply the reward function
    apply_reward(connection, engine, position_list)

    # Create copies of the desired tables
    if create_copy and suffix != "": 
        create_table_copy(connection, suffix)
    
    print("Creating weighted metrics...")
    # Calculate and create the weighted metrics and push to SQL
    create_weighted_metrics(connection, engine, season, suffix,
                            multiple_parts)
    
    # End of execution
    time_end = perf_counter()
    
    print(f"Finished! Execution time: {time_end-time_start:.2f} seconds.")
    

def partitioned_season(season, n_partitions):
    """
    Get the dates for diving a season into 1, ..., n_partitions partitions
    of approximately equal size. The function will create a sequence of 
    partition sizes to increase performance.

    Parameters
    ----------
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    n_partitions : integer
        The maximum number of partitions to consider.

    Returns
    -------
    partition_dict : dictionary
        A container of the type {partition_size: {partition: {start: date,
                                                              end: date}}}.

    """
    # Create database connection
    connection = connect_to_db("hockey")
    
    # Get all the dates and the number of games per date
    query = f"""SELECT date, Count(Distinct(GameId)) AS Count FROM pbp_view 
                WHERE GameId LIKE '{season}02%' GROUP BY date"""
    date_count = pd.read_sql(query, con=connection)
    
    # Initiate a dictionary for storing the results
    partition_dict = {}
    
    # Loop over all partitions from 1 until the desired number of partitions
    for partition_number in range(1, n_partitions + 1):
        # Start date of the first partition
        partition_start_date = int(date_count.loc[0, "date"])
    
        # Get the approximate size of the partitions
        partition_size = sum(date_count["Count"]) / partition_number
    
        # Count the cumulative sum of the number of games per date
        date_count["CumCount"] = np.cumsum(date_count["Count"])
        
        # Store the results for the current number of partitions
        partition_dict[partition_number] = {}
        
        for i in range(1, partition_number + 1):
            # Find the date which is closest to the desired partition size
            date_argmin = np.argmin(abs(date_count["CumCount"] - i * partition_size))
            
            # Get the end date for the partition
            partition_end_date = int(date_count.loc[date_argmin, "date"])
            
            # Store the start and end date for the givne partition
            partition_dict[partition_number][f"part{i}"] = {"start": partition_start_date, 
                                                            "end": partition_end_date}
            
            # Update the starting date
            partition_start_date = int(partition_end_date) + 1

    return partition_dict

    
if __name__ == "__main__":
    print("---Started execution---")

    ### Input arguments
    full_season = False
    playoffs_seasons = False
    partitioned_seasons = False
    multiple_seasons = True
    multiple_parts = False

    ######################### --- Full season --- #############################
    if full_season:
        # Main code    
        for season in range(2007, 2014):
            apply_weighted_reward(season=season, suffix=f"{season}", 
                                  create_copy=True,
                                  multiple_parts=False, playoffs=False) 
            
    ######################### --- Playoffs --- #############################
    if playoffs_seasons:
        # Main code    
        for season in range(2007, 2014):
            apply_weighted_reward(season=season, suffix=f"{season}_playoffs", 
                                  create_copy=True,
                                  multiple_parts=False, playoffs=True) 
        
    
    #################### --- Partitioned season --- ###########################
    if partitioned_seasons:
        # Input arguments
        n_partitions = 10
        
        # Main code
        for season in range(2013, 2014):
            partition_dict = partitioned_season(season, n_partitions)
            for partition in range(2, n_partitions+1): # Skip the full season
                print(f"Partition: {partition}")
                for part in partition_dict[partition]:
                    print(f"{season}_{partition}partitions_part{part[4:]}")
                    # Start and end date of the partitions
                    start_date_part = partition_dict[partition][part]["start"]
                    end_date_part = partition_dict[partition][part]["end"]
                    # Compute the reward
                    apply_weighted_reward(season=None,
                                          suffix=f"{season}_{partition}partitions_part{part[-1]}", 
                                          start_date=start_date_part, 
                                          end_date=end_date_part,
                                          create_copy=True, 
                                          multiple_parts=False, playoffs=False)
            
    
    ##################### --- Multiple parts --- ############################
    # Multiple seasons
    if multiple_seasons:
        # Input arguments
        start_season = 2007
        evaluation_season = 2013
        
        # Main code - Full seasons
        apply_weighted_reward(season=start_season, 
                              suffix=f"{start_season}_{evaluation_season}", 
                              create_copy=True, multiple_parts=True,
                              playoffs=False, evaluation_season=evaluation_season) 
        
        # Note to self: Fix table names for 2011-2013 and 2012-2013
        # Main code - playoffs
        apply_weighted_reward(season=start_season, 
                              suffix=f"{start_season}_{evaluation_season}_playoffs", 
                              create_copy=True, multiple_parts=True,
                              playoffs=True, evaluation_season=evaluation_season) 
        
    # Multiple parts within a season
    if multiple_parts:
        # Input arguments
        start_date = 20131001
        end_date = 20140713
        start_date_evaluation = 20140418
        
        # Main code
        apply_weighted_reward(suffix=f"{start_date}_{end_date}_{start_date_evaluation}_eval",
                              start_date=start_date, end_date=end_date,
                              create_copy=True, multiple_parts=True,
                              start_date_evaluation=start_date_evaluation)
        