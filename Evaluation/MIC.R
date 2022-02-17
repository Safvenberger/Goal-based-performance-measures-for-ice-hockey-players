# Author: Rasmus Säfvenberg

# Import packages
library(RMySQL)
library(minerva)
library(dplyr)

# Create database connection
db <- dbConnect(MySQL(), 
                user = 'root', password = 'password', 
                dbname = 'hockey', host = 'localhost', port = 3306)


mic <- function(season, metric, n, db, generalize=FALSE, traditional=FALSE,
                playoffs=FALSE, multiple_seasons=FALSE){
  ## Calculates the MIC value.
  ## 
  ## Input: 
  ##    season: integer representing the year of the season (4 digits).
  ##    metric: string; the name of the metric to consider
  ##    n: the specific partition to consider
  ##    db: the database connection
  ##    generalize:  boolean; whether to generalize, i.e. n * partition_size
  ##    traditional: boolean; whether to consider generalization of traditional metrics.
  ##    playoffs:    boolean; whether to consider playoffs or not
  ##    multiple_season:  boolean; whether to consider more than one season
  ##
  ## Output:
  ##    mic_df: data frame of MIC values.
  
  # Intialize empty vector 
  mic_vector <- numeric(n)
  # Go over all partitions 1:n
  for (i in 1:n){
    if(n == 1){
      if(multiple_seasons){
        # Multiple seasons
        query <- paste0("SELECT * FROM weighted_", 
                        metric, "_ranked", season, "_multiple")
      }
      else if(playoffs){
        # Playoffs
        query <- paste0("SELECT * FROM weighted_", 
                        metric, "_ranked", season, "_playoffs")
      }
      else {
        # Full season
        query <- paste0("SELECT * FROM weighted_", 
                      metric, "_ranked", season)
      }
    }
    else {
      # Partitioned season
      query <- paste0("SELECT * FROM weighted_", 
                      metric, "_ranked", season, "_", n, "partitions_part", i)
    }
    # Retrieve the data
    table <- dbGetQuery(db, query)
    
    if(generalize){
      # Get the full season
      full_query <- paste0("SELECT * FROM weighted_", 
                           metric, "_ranked", season)
      full_table <- dbGetQuery(db, full_query)
      
      # Combine full season and partitioned season
      merged_table <- merge(full_table, table, 
                            by=c("PlayerId", "PlayerName", "Position"), 
                            all.x=TRUE)
      
      # Fill NA with 0
      merged_table[is.na(merged_table)] <- 0
      
      if(traditional){
        # Generalize traditional metrics
        mic_vector[i] <- mine(merged_table[, paste0(metric, ".x")], 
                            n*merged_table[, paste0(metric, ".y")])$MIC
      } else {
        # Generalize GPIV metrics
        mic_vector[i] <- mine(merged_table[, paste0("Weighted", metric, ".x")], 
                            n*merged_table[, paste0("Weighted", metric, ".y")])$MIC
      }
    }
    else {
      # Correlation between traditional and GPIV
      mic_vector[i] <- mine(table[, paste0(metric)], 
                            table[, paste0("Weighted", metric)])$MIC
    }
  }
  # Create a data frame
  mic_df <- data.frame(MIC = mic_vector, Metric = metric, PartitionSize = n, 
                       Season = season)
  # Add a column Part to match python output
  mic_df$Part <- rownames(mic_df)
  
  return(mic_df)
}

calculate_mic <- function(metric_list, season_list, n_partitions,
                          generalize=FALSE, traditional=FALSE, playoffs=FALSE){
    ## Calculates the MIC value for all metrics and seasons.
    ## 
    ## Input: 
    ##    metric_list: vector of all metrics to consider.
    ##    season_list: vector of all seasons to consider.
    ##    generalize:  boolean; whether to generalize, i.e. n * partition_size
    ##    traditional: boolean; whether to consider generalization of traditional metrics.
    ##    playoffs:    boolean; whether to consider playoffs or not
    ##
    ## Output:
    ##    corr_df: data frame of all MIC values.
  
    # Loop over all seasons => metrics => 1:partition size
    corr <- lapply(season_list, function(season){
      lapply(metric_list, function(metric){
        lapply(1:n_partitions, function(part) mic(season, metric, part, db, 
                                                  generalize, traditional, playoffs))
      })
    })
      
    # Combine the list of lists into a data frame
    corr_df <- bind_rows(lapply(1:length(corr), function(x) bind_rows(corr[[x]])))
    
    return(corr_df)
}

# Specify the metrics to consider
metric_list <- c("Goals", "Assists", "First_Assists", "PlusMinus", "Points")

# Full season & partition correlation 
mic_trad_GPIV <- calculate_mic(metric_list, 2007:2013, 10,
                               generalize=FALSE, traditional=FALSE, playoffs=FALSE)

# Correlation within playoffs
mic_playoffs <- calculate_mic(metric_list, 2007:2013, 1,
                              generalize=FALSE, traditional=FALSE, playoffs=TRUE)

# Correlation between n*weighted and weighted
mic_generalize_GPIV <- calculate_mic(metric_list, 2007:2013, 10,
                                    generalize=TRUE, traditional=FALSE, playoffs=FALSE)

# Correlation between n*traditional and traditional
mic_generalize_trad <- calculate_mic(metric_list, 2007:2013, 10,
                                     generalize=TRUE, traditional=TRUE, playoffs=FALSE)

# Save as csv files
write.csv(mic_trad_GPIV,       "./Results/mic_trad_GPIV.csv", row.names = FALSE)
write.csv(mic_playoffs,        "./Results/mic_playoffs.csv", row.names = FALSE)
write.csv(mic_generalize_GPIV, "./Results/mic_generalize_GPIV.csv", row.names = FALSE)
write.csv(mic_generalize_trad, "./Results/mic_generalize_trad.csv", row.names = FALSE)
