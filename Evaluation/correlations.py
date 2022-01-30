from scipy import stats
from db import connect_to_db
import pandas as pd
import numpy as np

# Connect to the database
connection = connect_to_db("hockey")


def correlation(season, metric, n, connection, 
                weighted=False, multiple_seasons=False, traditional=False):
    """

    :param metric: Goals, Assists, PlusMinus or Points
    :param n: {1, 2,3,4,5}
    :param weighted: whether to do n*value where n is the amount of partitions
    :param multiple_seasons: whether to take from multiple seasons
    :param traditional: whether to use traditional or weighted metrics
    :return: data frame of correlation coefficients
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
                query = f"SELECT * FROM weighted_{metric}_ranked_reg_playoffs"
            else:
                # One full season (2013-2014)
                query = f"SELECT * FROM weighted_{metric}_ranked{season}"
        else:
            query = f"SELECT * FROM weighted_{metric}_ranked{season}_{n}partitions_part{i}"
        
        table = pd.read_sql(query, con=connection)
        
        if weighted:
            # Get the full season data
            full_query = f"SELECT * FROM weighted_{metric}_ranked_full"
            full_table = pd.read_sql(full_query, connection)
            
            # Combine the two tables
            merged_table = full_table.merge(table, 
                                            on=["PlayerId", "PlayerName", "Position"], 
                                            how="left")
            
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
    df = pd.DataFrame([pearson, spearman]).transpose().\
            rename(columns={0: "Pearson", 1: "Spearman"})
    
    df.index += 1
    
    return df


metric_list = ["Goals", "Assists", "PlusMinus", "Points"]

for metric in metric_list:
    print(correlation(metric, 1, connection, multiple_seasons=True))

# Standard cor(traditional, weighted)
for metric in metric_list:
    for part in range(1, 6):
        print(f"For {part} partitions, the metric {metric} has correlations\n"
              f"{correlation(2013, metric, part, connection)}\n")

# Correlation between n*weighted and weighted
for metric in metric_list:
    for part in range(1, 6):
        print(f"For {part} partitions, the metric {metric} has correlations\n"
              f"{correlation(metric, part, connection, True)}\n")

# Correlation between n*traditional and traditional
for metric in metric_list:
    for part in range(1, 6):
        print(f"For {part} partitions, the metric {metric} has correlations\n"
              f"{correlation(metric, part, connection, True, traditional=True)}\n")


pearson = []
spearman = []
metric_loop = []
for position in ["c", "d", "lr"]:
    for metric in metric_list:
        index = metric_list.index(metric)
        query = f"SELECT * FROM weighted_{metric}_ranked_positional_{position}"
        table = pd.read_sql(query, con=connection)
        pear, _ = stats.pearsonr(table[f"{metric}"], table[f"Weighted{metric}"])
        spear, _ = stats.spearmanr(table[f"{metric}"], table[f"Weighted{metric}"])
        metric_loop.append(metric + "_" + position)
        pearson.append(pear)
        spearman.append(spear)

pd.DataFrame([pearson, spearman], columns=metric_loop).T.rename(columns={0: "Pearson", 1: "Spearman"})



def summarized(metric, n):
    results = pd.DataFrame()
    for i in range(1, n+1):
        query = f"SELECT * FROM weighted_{metric}_rankedpartition_{n}_part{i}"
        table = pd.read_sql(query, con=connection)
        table = table.groupby("PlayerName")[[f"{metric}", f"Weighted{metric}"]].sum().reset_index()
        results = pd.concat([results, table])
        results = results.groupby("PlayerName")[[f"{metric}", f"Weighted{metric}"]].sum().reset_index()

    pear, _ = stats.pearsonr(results[f"{metric}"],results[f"Weighted{metric}"])

    spear, _ = stats.spearmanr(results[f"{metric}"],results[f"Weighted{metric}"])

    return pear, spear


for metric in metric_list:
    for part in range(2, 6):
        print(f"Metric: {metric}, n = {part}, corr = {summarized(metric, part)}")
        


# ALSO DO metric from part n * 5 corr with actual metric
