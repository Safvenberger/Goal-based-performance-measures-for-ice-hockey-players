#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Rasmus Säfvenberg

from scipy import stats
import sys
sys.path.insert(0, "../Scripts")
from db import connect_to_db
import pandas as pd
import numpy as np

def get_table_names(connection):
    """
    Retrieve all the table names from the database.

    Parameters
    ----------
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.

    Returns
    -------
    full_table_names : pd.DataFrame
        A dataframe with the names of only the full (singular) seasons.
    mult_table_names : pd.DataFrame
        A dataframe with the names of the tables that span multiple seasons.
    part_table_names : pd.DataFrame
        A dataframe with the names of the tables containing partitioned season data.

    """
    # Get the name of all tables with data of interest
    table_query = """SELECT TABLE_NAME 
                     FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_TYPE = 'BASE TABLE' AND 
                     TABLE_SCHEMA = 'hockey' AND 
                     Table_Name LIKE 
                     """
    
    # Create a data frame of all table names for full seasons (and multiple)
    full_table_names = pd.read_sql(table_query + "'weighted%20__'", con=connection)
    
    # Create a data frame of all table names for multiple_seasons
    mult_table_names = pd.read_sql(table_query + "'weighted%20__%20__%'", con=connection)
    
    # Unique table names of multiple seasons
    unique_mult = mult_table_names.TABLE_NAME.str.replace("_playoffs", "").unique()
    
    # Full season tables (regular seasons)
    full_table_names = full_table_names.loc[~full_table_names.TABLE_NAME.isin(unique_mult)]
    
    # Full season tables (playoffs)
    full_playoff_table_names = pd.read_sql(table_query + "'weighted%20__%_playoffs'", con=connection)
    mult_playoffs = full_playoff_table_names.TABLE_NAME.isin(mult_table_names.TABLE_NAME)
    full_playoff_table_names = full_playoff_table_names.loc[~mult_playoffs]

    # Combine regular season and playoff table names
    full_table_names = pd.concat([full_table_names, full_playoff_table_names]).sort_values("TABLE_NAME")

    # Names of partitioned seasons
    part_table_names = pd.read_sql(table_query + "'weighted%20__%_part%'", con=connection)
    # Extract the season
    part_table_names["season"] = part_table_names.TABLE_NAME.str.extract("(?<=ranked)(\d+)").astype(float)
    # Extract the partition size
    part_table_names["partition_size"] = part_table_names.TABLE_NAME.str.extract("(\d+)(?=partitions)").astype(float)
    # Extract the partition value
    part_table_names["partition"] = part_table_names.TABLE_NAME.str.extract("(?<=part)(\d+)").astype(float)
    # Sort values in logical order
    part_table_names.sort_values(["season", "partition_size", "partition"], 
                                 inplace=True)    
    
    return full_table_names, mult_table_names, part_table_names
    

def correlation(season, metric, n, connection, generalize=False, traditional=False, 
                mixed=False, playoffs=False, multiple=False, partitioned=False,
                evaluation_start=None, evaluation_end=None):
    """
    Calculate the correlation coefficients (Pearson/Spearman) for a specific
    number of partitions, n.

    Parameters
    ----------
    season : integer
        integer value of 4 characters (e.g. 2013)
        Selects games from the given season.
        Valid inputs are 2007 - 2013.
    metric : string
        The name of the metric to consider.
    n : integer
        What partition to consider (if applicable). Currently used {1, ..., 10}
    connection : MySQLconnection as created by db.connect_to_db
        a connection to the SQL database we are working with.
    generalize : boolean, default is False
        Whether to generalize the results, i.e. n*partition_value
    traditional : boolean, default is False
        Whether to consider the generalization of traditional metrics.
    mixed : boolean, default is False.
        Whether to consider a mix of weighted and traditional metrics.
    playoffs : boolean, default is False
        Whether to consider only the playoffs
    multiple : boolean, default is False
        Whether to consider multiple parts worth of data.
    partitioned : boolean, default is False
        Whether to consider a partitioned season.
    evaluation_start : integer, default is None
        The part on which the occurrences were counted. ("Training data")
    evaluation_end : integer, default is None
        The part on which the evaluation takes place. ("Test data")
    

    Returns
    -------
    corr_df : pd.DataFrame
        Data frame of correlation coefficients.

    """

    if multiple and partitioned:
        raise ValueError("Only one of multiple or partitioned can be chosen at a time.")
        
    # Get all table names
    full_tables, multiple_tables, partitioned_tables = get_table_names(connection)

    # If no partitions should be consider, this is a fail-safe
    if not partitioned:
         n = 1
       
    if playoffs:
        play_table = "_playoffs"
    else:
        play_table = ""
       
    # Initialize empty arrays
    pearson = np.zeros(n)
    spearman = np.zeros(n)
    
    # Loop over all partitions
    for i in range(1, n+1):
        # Select the relevant partition
        if n == 1:
            if multiple:
                # Multiple season/parts
                idx = multiple_tables.TABLE_NAME.str.contains(f"{evaluation_start}_{evaluation_end}{play_table}$") 
                metric_idx = multiple_tables.TABLE_NAME.str.contains(f"weighted_{metric.lower()}")
                table_name = multiple_tables.loc[idx & metric_idx].TABLE_NAME.values[0]
            else:
                # One full season
                idx = full_tables.TABLE_NAME.str.contains(f"{season}{play_table}$")
                metric_idx = full_tables.TABLE_NAME.str.contains(f"weighted_{metric.lower()}")
                table_name = full_tables.loc[idx & metric_idx].TABLE_NAME.values[0]
        else:
            # Partitions
            idx = partitioned_tables.TABLE_NAME.str.contains(f"{season}_{n}partitions_part{i}")
            metric_idx = partitioned_tables.TABLE_NAME.str.contains(f"weighted_{metric.lower()}")
            table_name = partitioned_tables.loc[idx & metric_idx].TABLE_NAME.values[0]
        
        table = pd.read_sql(f"SELECT * FROM {table_name}", connection)
        
        if generalize:
            # Get the full season data
            idx = full_tables.TABLE_NAME.str.contains(f"{season}{play_table}$")
            metric_idx = full_tables.TABLE_NAME.str.contains(f"weighted_{metric.lower()}")
            full_table_name = full_tables.loc[idx & metric_idx].TABLE_NAME.values[0]
            
            full_table = pd.read_sql(f"SELECT * FROM {full_table_name}", connection)

            # Combine the two tables
            merged_table = full_table.merge(table, 
                                            on=["PlayerId", "PlayerName", "Position"], 
                                            how="left")
            
            # Replace NA with 0
            merged_table.fillna(0, inplace=True)
            
            # For First_Assists
            # metric = metric.replace("_", "")
            
            if traditional and not mixed: 
                # Traditional metrics
                pear, _ = stats.pearsonr(merged_table[f"{metric}_x"], 
                                         n*merged_table[f"{metric}_y"])

                # Traditional metrics
                spear, _ = stats.spearmanr(merged_table[f"{metric}_x"], 
                                           n*merged_table[f"{metric}_y"])
            
            # Traditional and generalized weighted
            elif traditional and mixed:
                # n * Traditional metrics (x) and weighted metrics (y)
                pear, _ = stats.pearsonr(merged_table[f"Weighted{metric}_x"], 
                                         n*merged_table[f"{metric}_y"])

                spear, _ = stats.spearmanr(merged_table[f"Weighted{metric}_x"], 
                                           n*merged_table[f"{metric}_y"])

            # Generalized traditional and weighted
            elif not traditional and mixed:
                # Traditional metrics (x) and n * weighted metrics (y)
                pear, _ = stats.pearsonr(merged_table[f"{metric}_x"], 
                                         n*merged_table[f"Weighted{metric}_y"])

                spear, _ = stats.spearmanr(merged_table[f"{metric}_x"], 
                                           n*merged_table[f"Weighted{metric}_y"])
            else:
                # Calculate correlation coefficients between total weighted and n * weighted
                pear, _ = stats.pearsonr(merged_table[f"Weighted{metric}_x"], 
                                         n*merged_table[f"Weighted{metric}_y"])

                spear, _ = stats.spearmanr(merged_table[f"Weighted{metric}_x"], 
                                           n*merged_table[f"Weighted{metric}_y"])  
            
            
        else:
            # For First_Assists
            # metric = metric.replace("_", "")
            
            # Calculate correlation coefficients between traditional and weighted
            pear, _ = stats.pearsonr(table[f"{metric}"], 
                                     table[f"Weighted{metric}"])
            spear, _ = stats.spearmanr(table[f"{metric}"], 
                                       table[f"Weighted{metric}"])
            
        # Add to the arrays
        pearson[i-1] = pear
        spearman[i-1] = spear

    # Create a data frame having the correlation for each iteration and given metric.
    corr_df = pd.DataFrame([pearson, spearman]).transpose().\
            rename(columns={0: "Pearson", 1: "Spearman"})
    
    corr_df.index += 1
    
    return corr_df


def calculate_correlation(metric_list, season_list=None, n_partitions=1,
                          generalize=False, traditional=False, mixed=False,
                          playoffs=False, multiple=False, partitioned=False,
                          evaluation_start=None, evaluation_end=None
                          ):
    """
    Calculate the correlations (Pearson/Spearman) for all metrics in 
    the metric_list.

    Parameters
    ----------
    metric_list : list
        List of all metrics to check.
    season_list : iterable
        Iterable of all season to consider.
    n_partitions : integer
       The number of partitions in total.
    generalize : boolean, default is False
        Whether to generalize the results, i.e. n*partition_value
    traditional : boolean, default is False
        Whether to consider the generalization of traditional metrics.
    mixed : boolean, default is False.
        Whether to consider a mix of weighted and traditional metrics.
    playoffs : boolean, default is False
        Whether to consider only the playoffs
    multiple : boolean, default is False
        Whether to consider multiple parts worth of data.
    partitioned : boolean, default is False
        Whether to consider a partitioned season.
    evaluation_start : iterable, default is None
        The part on which the occurrences were counted. ("Training data")
    evaluation_end : integer, default is None
        The part on which the evaluation takes place. ("Test data")
    

    Returns
    -------
    corr : pd.DataFrame
        Data frame of all correlations.

    """
    # Empty dictionary for season and metrics
    corr = {}
    
    if season_list is not None:
        iterable = season_list
    else:
        if evaluation_start is not None:
            try: 
                if not isinstance(evaluation_start, str):
                    iterable = iter(evaluation_start)
                else:
                    iterable = [evaluation_start]
            except TypeError:
                raise TypeError("No iterable found for either season_list or evaluation_start.")
        
        
    # Loop over all seasons
    for season in iterable:
        # Empty dictionary for the season
        corr[season] = {}
        curr_eval = season
        # Go over all metrics
        for metric in metric_list:
            metric_df = pd.DataFrame()
            for part in range(1, n_partitions+1):
                # Correlation for each season, metric and partition part
                metric_df = metric_df.append(
                    correlation(season, metric, part, connection, 
                                generalize, traditional, mixed, playoffs, 
                                multiple, partitioned,
                                curr_eval, evaluation_end).\
                                             assign(Metric=metric, 
                                                    PartitionSize=part, 
                                                    Season=season))
                    
            # Save the metric for the season
            corr[season][metric] = metric_df.reset_index().\
                rename(columns={"index": "Part"})
        # Combine all metric and season data into one data frame per season
        corr[season] = pd.concat(corr[season])
        
    # Combine all correlations into one season
    corr = pd.concat(corr).reset_index(drop=True)
    
    return corr


if __name__ == "__main__": 
    # Connect to the database
    connection = connect_to_db("hockey")
    
    # Define the list of metrics to consider
    metric_list = ["Goals", "Assists", "First_Assists", "PlusMinus", "Points"]
    
    
    # Full season & partition correlation 
    corr_trad_GPIV = calculate_correlation(metric_list, 
                                           season_list=range(2007, 2014),
                                           n_partitions=10,
                                           generalize=False, traditional=False,
                                           playoffs=False, partitioned=True)
    
    # Correlation within playoffs
    corr_playoffs = calculate_correlation(metric_list, 
                                          season_list=range(2007, 2014),
                                          n_partitions=1,
                                          generalize=False, 
                                          traditional=False, playoffs=True)
    
    # Correlation within multiple seasons (regular season)
    corr_mult_reg = calculate_correlation(metric_list, 
                                          n_partitions=1,
                                          generalize=False, traditional=False, 
                                          playoffs=False, multiple=True,
                                          evaluation_start=range(2007, 2013),
                                          evaluation_end=2013)
    
    # Correlation within multiple seasons (playoffs)
    corr_mult_play = calculate_correlation(metric_list, 
                                           n_partitions=1,
                                           generalize=False, traditional=False, 
                                           playoffs=True, multiple=True,
                                           evaluation_start=range(2007, 2013),
                                           evaluation_end=2013)

    
    # Correlation between n*weighted and weighted
    corr_generalize_GPIV = calculate_correlation(metric_list, 
                                                 season_list=range(2007, 2014),
                                                 n_partitions=10,
                                                 generalize=True, traditional=False, 
                                                 playoffs=False, partitioned=True)
    
    # Correlation between n*traditional and traditional
    corr_generalize_trad = calculate_correlation(metric_list,
                                                 season_list=range(2007, 2014),
                                                 n_partitions=10,
                                                 generalize=True, traditional=True,
                                                 playoffs=False, partitioned=True)
    
    # Correlation between n*traditional and weighted
    corr_generalize_trad_GPIV = calculate_correlation(metric_list,
                                                      season_list=range(2007, 2014),
                                                      n_partitions=10,
                                                      generalize=True, traditional=True,
                                                      mixed=True,
                                                      playoffs=False, partitioned=True)
    
    # Correlation between n*GPIV and traditional
    corr_generalize_GPIV_trad = calculate_correlation(metric_list,
                                                      season_list=range(2007, 2014),
                                                      n_partitions=10,
                                                      generalize=True, traditional=False,
                                                      mixed=True,
                                                      playoffs=False, partitioned=True)
    
    # Save as csv files
    corr_trad_GPIV.to_csv(           "../Results/corr_trad_GPIV.csv", index=False)
    corr_playoffs.to_csv(            "../Results/corr_playoffs.csv", index=False)
    corr_mult_reg.to_csv(            "../Results/corr_mult_reg.csv", index=False)
    corr_mult_play.to_csv(           "../Results/corr_mult_play.csv", index=False)
    corr_generalize_GPIV.to_csv(     "../Results/corr_generalize_GPIV.csv", index=False)
    corr_generalize_trad.to_csv(     "../Results/corr_generalize_trad.csv", index=False)
    corr_generalize_trad_GPIV.to_csv("../Results/corr_generalize_trad_GPIV.csv", index=False)
    corr_generalize_GPIV_trad.to_csv("../Results/corr_generalize_GPIV_trad.csv", index=False)


