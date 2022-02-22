#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Rasmus Säfvenberg

import pandas as pd
from db import connect_to_db


def get_player_rankings(connection, season: int, 
                        metric: str, position: list=None) -> pd.DataFrame:
    """
    Get all players in a given season and metric and rank them by their GPIV
    value.

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        A connection to the SQL database we are working with.
    season : int
        The season to consider (2007-2013).
    metric : str
        The metric to consider (Goals, Assists, First_Assists, Points, PlusMinus).
    position : list, default is None.
        A list of the position(s) to subset. Currently supports two positions 
        at a time.

    Returns
    -------
    player_rankings : pd.DataFrame
        Data frame with all players ranked according to GPIV metric.

    """

    if position is not None:
        if len(position) == 1:
            # Get the players for a given position ranked according to a specific position
            query = f"""SELECT * FROM weighted_{metric}_ranked{season} 
                        WHERE Position = '{position[0]}' 
                        ORDER BY weighted{metric} DESC"""
        else: 
            query = f"""SELECT * FROM weighted_{metric}_ranked{season} 
                        WHERE Position = '{position[0]}' OR
                              Position = '{position[1]}'
                        ORDER BY weighted{metric} DESC"""
    else: 
        # Get all players, regardless of position
        query = f"""SELECT * FROM weighted_{metric}_ranked{season} 
                    ORDER BY weighted{metric} DESC"""
                    
    # Save as pandas data frame
    player_rankings = pd.read_sql(query, con=connection)
                
    return player_rankings            


def write_to_excel(connection, position: list=None, file_name: str="GPIV"):
    """
    Create an .xlsx file per metric containing the rankings for all seasons
    sorted by the top GPIV values.

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        A connection to the SQL database we are working with.
    position : list, default is None.
        The position(s) to subset.
    file_name : str, default is "GPIV".
        The prefix of the filename to write/overwrite.
    Returns
    -------
    None. Instead, the results are saved in .xlsx files.

    """
    # Get the name of all tables with full season data
    table_query = """SELECT TABLE_NAME 
                     FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_TYPE = 'BASE TABLE' AND 
                     TABLE_SCHEMA = 'hockey' AND 
                     Table_Name LIKE 'weighted%_20__'
                  """
                      
    # Create a data frame of all table names
    table_names = pd.read_sql(table_query, con=connection)
    
    # Name of all metrics
    metrics = table_names["TABLE_NAME"].str.\
        replace("weighted|ranked|\d{4}|(?<!first)_", "", regex=True).unique()

    # Name/year of all seasons 
    seasons = table_names["TABLE_NAME"].str.\
        replace("_|[A-z]+(?=\d{4})", "", regex=True).unique()            

    for metric in metrics:
        with pd.ExcelWriter(f"../Results/{file_name}-{metric}.xlsx") as writer:
            for season in seasons:
                # Get the ranking for the given season and metric
                ranking = get_player_rankings(connection, season, metric, position)

                # Special case for the first assists metrics
                if metric.lower() == "first_assists":
                    metric = "First_Assists"
                else: 
                    metric = metric.capitalize()
                
                # Rename columns
                ranking.rename(columns={"Rank_trad": "Trad. rank",
                                        "Rank_w": "GPIV rank",
                                        "Rank_diff": "Rank diff.",
                                        f"Weighted{metric}": 
                                        f"GPIV {metric}"}, 
                               inplace=True)
                
                # Remove additional columns (goals/assists) from points
                if metric.lower() == "points":
                    ranking.drop(["Goals", "WeightedGoals", 
                                  "Assists", "WeightedAssists"], 
                                 axis=1, inplace=True)
                
                # Write to the excel file
                ranking.to_excel(writer, f"{season}-{int(season)+1}", 
                                 index=False)  
                
                # Get the xlsxwriter workbook and worksheet objects.
                workbook  = writer.book
                worksheet = writer.sheets[f"{season}-{int(season)+1}"]
                
                # Number formatting
                format_num = workbook.add_format({'num_format': '#,##0.000'})

                # Specify column width for column A-C
                worksheet.set_column(0, 2, 10)
                
                # Specify column width for column E
                worksheet.set_column(4, 4, 30)
                
                # Specify column width for column G
                worksheet.set_column(6, 6, 15)

                # Specify column width for column H
                worksheet.set_column(7, 7, 15, format_num)
        
        
if __name__ == "__main__":
    # Create connection
    connection = connect_to_db("hockey")

    # Full season metric rankings for all positions
    write_to_excel(connection)
    
    # Create a specific file for positions
    for position in [["D"], ["C"], ["L", "R"]]:
        # Full season metric rankings for all positions
        write_to_excel(connection, position, f"Positions/GPIV-{''.join(position)}")
    