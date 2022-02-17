#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Rasmus Säfvenberg

from scipy import stats
import sys
sys.path.insert(0, "../Scripts")
from db import connect_to_db
import pandas as pd
import numpy as np


def correlation(season, metric, n, connection, generalize=False,
                traditional=False, playoffs=False, multiple_seasons=False):
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
    playoffs : boolean, default is False
        Whether to consider only the playoffs
    multiple_seasons : boolean, default is False
        Whether to consider multiple seasons worth of data.

    Returns
    -------
    corr_df : pd.DataFrame
        Data frame of correlation coefficients.

    """
    
    # Initialize empty arrays
    pearson = np.zeros(n)
    spearman = np.zeros(n)
    
    # Loop over all partitions
    for i in range(1, n+1):
        # Select the relevant partition
        if n == 1:
            if multiple_seasons:
                # Multiple seasons
                query = f"SELECT * FROM weighted_{metric}_ranked{season}_multiple"
            elif playoffs:
                query = f"SELECT * FROM weighted_{metric}_ranked{season}_playoffs"
            else:
                # One full season 
                query = f"SELECT * FROM weighted_{metric}_ranked{season}"
        else:
            query = f"SELECT * FROM weighted_{metric}_ranked{season}_{n}partitions_part{i}"
        
        # Read the data from database
        table = pd.read_sql(query, con=connection)
        
        if generalize:
            # Get the full season data
            full_query = f"SELECT * FROM weighted_{metric}_ranked{season}"
            full_table = pd.read_sql(full_query, connection)
            
            # Combine the two tables
            merged_table = full_table.merge(table, 
                                            on=["PlayerId", "PlayerName", "Position"], 
                                            how="left")
            
            # Replace NA with 0
            merged_table.fillna(0, inplace=True)
            
            if traditional: 
                # Traditional metrics
                pear, _ = stats.pearsonr(merged_table[f"{metric}_x"], 
                                          n*merged_table[f"{metric}_y"])

                # Traditional metrics
                spear, _ = stats.spearmanr(merged_table[f"{metric}_x"], 
                                            n*merged_table[f"{metric}_y"])

            else:
                # Calculate correlation coefficients between total weighted and n * weighted
                pear, _ = stats.pearsonr(merged_table[f"Weighted{metric}_x"], 
                                         n*merged_table[f"Weighted{metric}_y"])

                spear, _ = stats.spearmanr(merged_table[f"Weighted{metric}_x"], 
                                           n*merged_table[f"Weighted{metric}_y"])  
            
            
        else:
            # Calculate correlation coefficients between traditional and weighted
            pear, _ = stats.pearsonr(table[f"{metric}"], table[f"Weighted{metric}"])
            spear, _ = stats.spearmanr(table[f"{metric}"], table[f"Weighted{metric}"])
            
        # Add to the arrays
        pearson[i-1] = pear
        spearman[i-1] = spear

    # Create a data frame having the correlation for each iteration and given metric.
    corr_df = pd.DataFrame([pearson, spearman]).transpose().\
            rename(columns={0: "Pearson", 1: "Spearman"})
    
    corr_df.index += 1
    
    return corr_df


def calculate_correlation(metric_list, season_list, n_partitions,
                          generalize=False, traditional=False, 
                          playoffs=False):
    """
    Calculate the correlations (Pearson/Spearman) for all metrics in 
    the metric_list.

    Parameters
    ----------
    metric_list : list
        List of all metrics to check.
    season_list : list
        List of all season to consider.
    n_partitions : integer
       The number of partitions in total.
    generalize : boolean, default is False
        Whether to generalize the results, i.e. n*partition_value
    traditional : boolean, default is False
        Whether to consider the generalization of traditional metrics.
    playoffs : boolean, default is False
        Whether to consider only the playoffs


    Returns
    -------
    corr : pd.DataFrame
        Data frame of all correlations.

    """
    # Empty dictionary for season and metrics
    corr = {}
    # Loop over all seasons
    for season in season_list:
        # Empty dictionary for the season
        corr[season] = {}
        # Go over all metrics
        for metric in metric_list:
            metric_df = pd.DataFrame()
            for part in range(1, n_partitions+1):
                # Correlation for each season, metric and partition part
                metric_df = metric_df.append(
                    correlation(season, metric, part, connection, 
                                generalize, traditional, playoffs).\
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
                                           generalize=False, 
                                           traditional=False, playoffs=False)
    
    # Correlation within playoffs
    corr_playoffs = calculate_correlation(metric_list, 
                                          season_list=range(2007, 2014),
                                          n_partitions=1,
                                          generalize=False, 
                                          traditional=False, playoffs=True)
    
    # Correlation between n*weighted and weighted
    corr_generalize_GPIV = calculate_correlation(metric_list, 
                                                 season_list=range(2007, 2014),
                                                 n_partitions=10,
                                                 generalize=True, 
                                                 traditional=False, playoffs=False)
    
    # Correlation between n*traditional and traditional
    corr_generalize_trad = calculate_correlation(metric_list,
                                                 season_list=range(2007, 2014),
                                                 n_partitions=10,
                                                 generalize=True, 
                                                 traditional=True, playoffs=False)
    
    # Save as csv files
    corr_trad_GPIV.to_csv(      "../Results/corr_trad_GPIV.csv", index=False)
    corr_playoffs.to_csv(       "../Results/corr_playoffs.csv", index=False)
    corr_generalize_GPIV.to_csv("../Results/corr_generalize_GPIV.csv", index=False)
    corr_generalize_trad.to_csv("../Results/corr_generalize_trad.csv", index=False)


