# Author: Rasmus Säfvenberg

# Import packages
library(RMySQL)
library(minerva)
library(dplyr)

# Create database connection
db <- dbConnect(MySQL(), 
                user = 'root', password = 'password', 
                dbname = 'hockey', host = 'localhost', port = 3306)


get_table_names <- function(db){
  # Get the name of all tables with data of interest
  table_query <- "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'hockey' AND 
                  Table_Name LIKE "
  
  # Create a data frame of all table names for full seasons (and multiple)
  full_table_names <- dbGetQuery(db, paste0(table_query, "'weighted%20__'"))
  
  # Create a data frame of all table names for multiple
  mult_table_names <- dbGetQuery(db, paste0(table_query, "'weighted%20__%20__%'"))
  
  # Unique table names of multiple seasons
  unique_mult <- unique(gsub("_playoffs", "", mult_table_names$TABLE_NAME))
  
  # Full season tables (regular seasons)
  full_table_names <- full_table_names[!(full_table_names$TABLE_NAME %in% unique_mult), ,drop=FALSE]
  
  # Full season tables (playoffs)
  full_playoff_table_names <- dbGetQuery(db, paste0(table_query, "'weighted%20__%_playoffs'"))
  mult_playoffs <- (full_playoff_table_names$TABLE_NAME %in% mult_table_names$TABLE_NAME)
  full_playoff_table_names <- full_playoff_table_names[!mult_playoffs, ,drop=FALSE]
  
  # Combine regular season and playoff table names
  full_table_names <- bind_rows(full_table_names, full_playoff_table_names) %>% arrange("TABLE_NAME")
  
  # Names of partitioned seasons
  part_table_names <- dbGetQuery(db, paste0(table_query, "'weighted%20__%_part%'"))
  # Extract the season
  part_table_names["season"] <- regmatches(part_table_names$TABLE_NAME, 
                                           regexpr("(?<=ranked)(\\d+)", 
                                                   part_table_names$TABLE_NAME, 
                                                   perl=TRUE)) %>% as.integer()
  
  # Extract the partition size
  part_table_names["partition_size"] <- regmatches(part_table_names$TABLE_NAME, 
                                                   regexpr("(\\d+)(?=partitions)", 
                                                           part_table_names$TABLE_NAME, 
                                                           perl=TRUE)) %>% as.integer()
  # Extract the partition value
  part_table_names["partition"] <- regmatches(part_table_names$TABLE_NAME, 
                                              regexpr("(?<=part)(\\d+)", 
                                                      part_table_names$TABLE_NAME, 
                                                      perl=TRUE)) %>% as.integer()
  # Sort values in logical order
  part_table_names <- part_table_names %>% arrange(season, partition_size, partition)

  return(list(full_table_names = full_table_names, 
              mult_table_names = mult_table_names, 
              part_table_names = part_table_names))
  
}


mic <- function(season, metric, n, db, generalize=FALSE, traditional=FALSE, mixed=FALSE,
                playoffs=FALSE, multiple=FALSE, partitioned=FALSE,
                evaluation_start=NA, evaluation_end=NA){
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
  ##    mixed:       boolean; whether to consider a mix of weighted and traditional metrics.
  ##    multiple:    boolean; whether to consider more than one season/part
  ##    partitioned: boolean; whether to consider partitions
  ##    evaluation_start:  integer; the part on which the occurrences were counted. ("Training data")
  ##    evaluation_end:  integer; the part on which the evaluation takes place. ("Test data")
  ##
  ## Output:
  ##    mic_df: data frame of MIC values.
  
  
  if(multiple & partitioned){
    stop("Only one of multiple or partitioned can be chosen at a time.")
  }
  
  # Get all table names
  table_names <- get_table_names(db)
  full_tables <- table_names$full_table_names
  multiple_tables <- table_names$mult_table_names
  partitioned_tables <- table_names$part_table_names 
  
  # If no partitions should be consider, this is a fail-safe
  if(!partitioned){
    n <- 1
  }
  
  if(playoffs){
    play_table <- "_playoffs" 
  }
  else {
    play_table <- ""
  }
  # Intialize empty vector 
  mic_vector <- numeric(n)
  
  # Go over all partitions 1:n
  for (i in 1:n){
    if(n == 1){
      if(multiple){
        # Multiple season/parts
        idx <- grepl(paste0(evaluation_start, "_", evaluation_end, play_table, "$"), 
                     multiple_tables$TABLE_NAME)
        metric_idx <- grepl(tolower(paste0("weighted_", metric)), multiple_tables$TABLE_NAME)
        table_name <- multiple_tables[idx & metric_idx, ][1]
      }
      else {
        # One full season
        idx <- grepl(paste0(season, play_table, "$"), 
                     full_tables$TABLE_NAME)
        metric_idx <- grepl(tolower(paste0("weighted_", metric)), full_tables$TABLE_NAME)
        table_name <- full_tables[idx & metric_idx, ][1]
      }
    }
    else {
      # Partitions
      idx <- grepl(paste0(season, "_", n, "partitions_part", i, "$"), 
                   partitioned_tables$TABLE_NAME)
      metric_idx <- grepl(tolower(paste0("weighted_", metric)), partitioned_tables$TABLE_NAME)
      table_name <- partitioned_tables[idx & metric_idx, ][1]
    }
    # print(table_name)
    # Retrieve the data
    table <- dbGetQuery(db, paste0("SELECT * FROM ", table_name))
    
    if(generalize){
      # Get the full season
      idx <- grepl(paste0(season, play_table, "$"), 
                   full_tables$TABLE_NAME)
      metric_idx <- grepl(tolower(paste0("weighted_", metric)), full_tables$TABLE_NAME)
      full_table_name <- full_tables[idx & metric_idx, ][1]
      
      full_table <- dbGetQuery(db, paste0("SELECT * FROM ", full_table_name))
      
      # Combine full season and partitioned season
      merged_table <- merge(full_table, table, 
                            by=c("PlayerId", "PlayerName", "Position"), 
                            all.x=TRUE)
      
      # Fill NA with 0
      merged_table[is.na(merged_table)] <- 0
      
      # For First_Assists
      # metric <- gsub("_", "", metric)
      
      if(traditional & !mixed){
        # Generalize traditional metrics
        mic_vector[i] <- mine(merged_table[, paste0(metric, ".x")], 
                              n*merged_table[, paste0(metric, ".y")])$MIC
      } else if(traditional & mixed){
        # n * Traditional metrics (x) and weighted metrics (y)
        mic_vector[i] <- mine(merged_table[, paste0("Weighted", metric, ".x")], 
                              n*merged_table[, paste0(metric, ".y")])$MIC
      } else if(!traditional & mixed){
        # Traditional metrics (x) and n * weighted metrics (y)
        mic_vector[i] <- mine(merged_table[, paste0(metric, ".x")], 
                              n*merged_table[, paste0("Weighted", metric, ".y")])$MIC
      } 
      else {
        # Generalize GPIV metrics
        mic_vector[i] <- mine(merged_table[, paste0("Weighted", metric, ".x")], 
                              n*merged_table[, paste0("Weighted", metric, ".y")])$MIC
      }
    }
    else {
      # For First_Assists
      # metric <- gsub("_", "", metric)
      
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

calculate_mic <- function(metric_list, season_list=NA, n_partitions=1,
                          generalize=FALSE, traditional=FALSE, 
                          mixed=FALSE, playoffs=FALSE,
                          multiple=FALSE, partitioned=FALSE,
                          evaluation_start=NA, evaluation_end=NA){
    ## Calculates the MIC value for all metrics and seasons.
    ## 
    ## Input: 
    ##    metric_list: vector of all metrics to consider.
    ##    season_list: vector of all seasons to consider.
    ##    generalize:  boolean; whether to generalize, i.e. n * partition_size
    ##    traditional: boolean; whether to consider generalization of traditional metrics.
    ##    mixed:       boolean; whether to consider a mix of weighted and traditional metrics.
    ##    playoffs:    boolean; whether to consider playoffs or not
    ##    multiple:    boolean; whether to consider more than one season/part
    ##    partitioned: boolean; whether to consider partitions
    ##    evaluation_start:  integer; the part on which the occurrences were counted. ("Training data")
    ##    evaluation_end:  integer; the part on which the evaluation takes place. ("Test data")
    ## Output:
    ##    corr_df: data frame of all MIC values.
  
    if(any(is.na(season_list))){
      iterable <- evaluation_start
    } else {
      iterable <- season_list
    }
    # Loop over all iterable => metrics 
    corr <- lapply(iterable, function(season){
      lapply(metric_list, function(metric){
        lapply(1:n_partitions, function(part) mic(season, metric, part, db, 
                                                  generalize, traditional, mixed,
                                                  playoffs, multiple, partitioned,
                                                  season, evaluation_end))
      })
    })
      
    # Combine the list of lists into a data frame
    corr_df <- bind_rows(lapply(1:length(corr), function(x) bind_rows(corr[[x]])))
    
    return(corr_df)
}

### Main program

# Specify the metrics to consider
metric_list <- c("Goals", "Assists", "First_Assists", "PlusMinus", "Points")

# Full season & partition correlation 
mic_trad_GPIV <- calculate_mic(metric_list, 2007:2013, 10,
                               generalize=FALSE, traditional=FALSE, 
                               playoffs=FALSE, partitioned=TRUE)

# Correlation within playoffs
mic_playoffs <- calculate_mic(metric_list, 2007:2013, 1,
                              generalize=FALSE, traditional=FALSE, 
                              playoffs=TRUE)

# Correlation within multiple seasons (regular season)
mic_mult_reg <- calculate_mic(metric_list, 
                              n_partitions=1,
                              generalize=FALSE, traditional=FALSe, 
                              playoffs=FALSE, multiple=TRUE,
                              evaluation_start=2007:2012,
                              evaluation_end=2013)

# Correlation within multiple seasons (playoffs)
mic_mult_play <- calculate_mic(metric_list, 
                               n_partitions=1,
                               generalize=FALSE, traditional=FALSe, 
                               playoffs=TRUE, multiple=TRUE,
                               evaluation_start=2007:2012,
                               evaluation_end=2013)


# Correlation between n*weighted and weighted
mic_generalize_GPIV <- calculate_mic(metric_list, 2007:2013, 10,
                                    generalize=TRUE, traditional=FALSE, 
                                    playoffs=FALSE, partitioned=TRUE)

# Correlation between n*traditional and traditional
mic_generalize_trad <- calculate_mic(metric_list, 2007:2013, 10,
                                     generalize=TRUE, traditional=TRUE, 
                                     playoffs=FALSE, partitioned=TRUE)
                                     
# Correlation between n*traditional and GPIV
mic_generalize_trad_GPIV <- calculate_mic(metric_list, 2007:2013, 10,
                                          generalize=TRUE, traditional=TRUE, 
                                          mixed=TRUE, playoffs=FALSE,
                                          partitioned=TRUE)
                                     
# Correlation between n*GPIV and traditional
mic_generalize_GPIV_trad <- calculate_mic(metric_list, 2007:2013, 10,
                                          generalize=TRUE, traditional=FALSE,
                                          mixed=TRUE, playoffs=FALSE,
                                          partitioned=TRUE)

# Save as csv files
write.csv(mic_trad_GPIV,            "./Results/mic_trad_GPIV.csv", row.names = FALSE)
write.csv(mic_playoffs,             "./Results/mic_playoffs.csv",  row.names = FALSE)
write.csv(mic_mult_reg,             "./Results/mic_mult_reg.csv",  row.names = FALSE)
write.csv(mic_mult_play,            "./Results/mic_mult_play.csv", row.names = FALSE)
write.csv(mic_generalize_GPIV,      "./Results/mic_generalize_GPIV.csv", row.names = FALSE)
write.csv(mic_generalize_trad,      "./Results/mic_generalize_trad.csv", row.names = FALSE)
write.csv(mic_generalize_trad_GPIV, "./Results/mic_generalize_trad_GPIV.csv", row.names = FALSE)
write.csv(mic_generalize_GPIV_trad, "./Results/mic_generalize_GPIV_trad.csv", row.names = FALSE)
