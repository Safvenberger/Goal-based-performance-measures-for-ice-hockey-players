#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from db import connect_to_db, create_db_engine
from populateFields import extract_season
from outcomePerSecond import count_occurrences
from reward import apply_reward, create_table_copy
from weighted import create_weighted_metrics

def apply_weighted_reward(season, suffix, 
                          start_date=None, end_date=None,
                          create_copy=True, 
                          get_external_dates=False, add_external_dates=False,
                          calc_occur=True,
                          position_list=[],
                          multiple_seasons=False
                          ):
    """
    Extract the season, calculate occurrences, apply the reward function and 
    finally, calculate the weighted metrics and assign to the SQL database.

    Parameters
    ----------
    season : integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2014.
    suffix : string
        What to append at the end of the table name in the SQL database.
    start_date : integer value of format yyyymmdd
        The start date we are interested in examining.
    end_date : integer value of format yyyymmdd
        The end date we are interested in examining.
    create_copy : boolean
        If copies of the table, ending with suffix, should be added to the database. 
        The default is True.
    get_external_dates : boolean
        If we want to scrape dates from hockey-reference.com. 
        The default is False.
    add_external_dates : boolean
        If we want to create an SQL table 'match_date' with game dates. 
        The default is False.
    calc_occur : boolean
        Whether to create the mpbp table and calculate occurrences or simply
        apply the reward based on an already defined occurrences table.
    position_list : list
        A list of position(s) to calculate reward for.
        Default is an empty list (i.e. all positions).
    
    Returns
    -------
    None. The related changes are instead pushed to the SQL database.

    """
    # Create database connection and engine
    connection = connect_to_db("hockey")
    engine = create_db_engine("hockey")
    
    if calc_occur:
        # Extract the season
        extract_season(connection, engine, season, 
                       start_date=start_date, end_date=end_date, 
                       get_dates=get_external_dates, add_dates=add_external_dates, 
                       multiple_seasons=multiple_seasons)
    
        print("Counting occurrences...")
        # Count occurrences
        count_occurrences(connection, engine)
        
    print("Applying reward...")
    # Apply the reward function
    apply_reward(connection, engine, position_list)

    # Create copies of the desired tables
    if create_copy and suffix != "": 
        create_table_copy(connection, suffix)
    
    print("Creating weighted metrics...")
    # Calculate and create the weighted metrics and push to SQL
    create_weighted_metrics(season, connection, engine, suffix)
    
    print("Finished!")
    
if __name__ == "__main__":
    print("---Started execution---")

    # start_time_full = datetime.now()

    ### Full season ###
    # Full season
    apply_weighted_reward(season=2013, suffix="_playoffs", 
                          start_date="20140415", end_date="20140713",
                          create_copy=True,
                          get_external_dates=False, add_external_dates=False) 

    # end_time_full = datetime.now()
    
    # print(f"Full season extraction took: {(end_time_full-start_time_full).seconds} seconds")
    
    ### Partitioned season ###
    # # Partitioned season
    # dates = {2: {"part1": {"start": "20131001", "end": "20140101"}, 
    #               "part2": {"start": "20140102", "end": "20140413"}},
            
    #           3: {"part1": {"start": "20131001", "end": "20131201"}, 
    #               "part2": {"start": "20131202", "end": "20140131"},
    #               "part3": {"start": "20140201", "end": "20140413"}},
             
    #           4: {"part1": {"start": "20131001", "end": "20131118"}, 
    #               "part2": {"start": "20131119", "end": "20140101"},
    #               "part3": {"start": "20140102", "end": "20140303"},
    #               "part4": {"start": "20140304", "end": "20140413"}},
             
    #           5: {"part1": {"start": "20131001", "end": "20131108"}, 
    #               "part2": {"start": "20131109", "end": "20131213"},
    #               "part3": {"start": "20131214", "end": "20140119"},
    #               "part4": {"start": "20140120", "end": "20140312"},
    #               "part5": {"start": "20140313", "end": "20140413"}}}
  
    # start_time_part = datetime.now()

    # for partition in dates.keys():
    #     print(f"Partition: {partition}")
    #     for part in dates[partition]:
    #         start_date_part = dates[partition][part]["start"]
    #         end_date_part = dates[partition][part]["end"]
    #         apply_weighted_reward(season=2013, 
    #                               suffix=f"_partition_{partition}_part{part[-1]}", 
    #                               start_date=start_date_part, 
    #                               end_date=end_date_part,
    #                               create_copy=True,
    #                               get_external_dates=False, 
    #                               add_external_dates=False) 
    
    # end_time_part = datetime.now()
    
    # print(f"Partitioned season extraction took: {(end_time_part-start_time_part).seconds} seconds")
    
    ### Positional split###
    
    # # Positional split
    # positions = [["D"], ["C"], ["L", "R"]]
    
    # start_time_pos = datetime.now()
    # for pos in positions:
    #     print(f"Position {pos}")
    #     apply_weighted_reward(season=2013, 
    #                           suffix=f"_positional_{''.join(pos).lower()}", 
    #                           start_date="20131001", end_date="20140413",
    #                           create_copy=True,
    #                           get_external_dates=False, 
    #                           add_external_dates=False, 
    #                           calc_occur=False,
    #                           position_list=pos)
        
    # end_time_pos = datetime.now()
    
    # print(f"Positional season extraction took: {(end_time_pos-start_time_pos).seconds} seconds")
    
    # start_time_full = datetime.now()

    ### Multiple seasons ###
    # # Run multiple seasons
    # apply_weighted_reward(season=2012, suffix="_season12_14", 
    #                       #start_date="20131001", end_date="20140413",
    #                       create_copy=True, multiple_seasons=True,
    #                       #calc_occur=False,
    #                       get_external_dates=False, add_external_dates=False) 

    # end_time_full = datetime.now()
    
    # print(f"Full season extraction took: {(end_time_full-start_time_full).seconds} seconds")
    
    # # Apply reward for 2013
    # apply_weighted_reward(season=2013, suffix="_season11_14", 
    #                       #start_date="20131001", end_date="20140413",
    #                       create_copy=True, multiple_seasons=True,
    #                       calc_occur=False,
    #                       get_external_dates=False, add_external_dates=False) 