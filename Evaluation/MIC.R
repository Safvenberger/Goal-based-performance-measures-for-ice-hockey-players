library(RMySQL)
library(minerva)
library(dplyr)
db <- dbConnect(MySQL(), 
                user = 'root', password = 'password', 
                dbname = 'hockey', host = 'localhost', port = 3306)


mic <- function(metric, n, weighted=FALSE, multiple_seasons=FALSE, traditional=FALSE){
  mic_vector <- numeric(n)
  for (i in 1:n){
    if(n == 1){
      query <- paste0("SELECT * FROM weighted_", 
                      metric, "_ranked_full")
    }
    else {
      query <- paste0("SELECT * FROM weighted_", 
                      metric, "_rankedpartition_", n, "_part", i)
    }
    table <- dbGetQuery(db, query)
    
    if(weighted){
      full_query <- paste0("SELECT * FROM weighted_", 
                           metric, "_ranked_full")
      full_table <- dbGetQuery(db, full_query)
      
      merged_table <- merge(full_table, table, 
                            by=c("PlayerId", "PlayerName", "Position"), 
                            all.x=TRUE)
      
      merged_table[is.na(merged_table)] <- 0
      
      if(traditional){
        mic_vector[i] <- mine(merged_table[, paste0(metric, ".x")], 
                            n*merged_table[, paste0(metric, ".y")])$MIC
      } else {
        mic_vector[i] <- mine(merged_table[, paste0("Weighted", metric, ".x")], 
                            n*merged_table[, paste0("Weighted", metric, ".y")])$MIC
      }
    }
    else {
      mic_vector[i] <- mine(table[, paste0(metric)], 
                            table[, paste0("Weighted", metric)])$MIC
    }
  }
  return(mic_vector)
}

metric_list <- c("Goals", "Assists", "PlusMinus", "Points")
# Multiple seasons
sapply(metric_list, function(x) mic(x, 1))


mic_list <- list()
for(metric in metric_list){
  mic_vector <- numeric() 
  for(part in 1:5){
    mic_vector <- c(mic_vector, mic(metric, part))
  }
  names(mic_vector) <- c(rep(1, 1), rep(2, 2), rep(3, 3), rep(4, 4), rep(5, 5))
  mic_list[[metric]] <- mic_vector
}

# Weighted
mic_list <- list()
for(metric in metric_list){
  mic_vector <- numeric() 
  for(part in 1:5){
    mic_vector <- c(mic_vector, mic(metric, part, weighted = TRUE))
  }
  names(mic_vector) <- c(rep(1, 1), rep(2, 2), rep(3, 3), rep(4, 4), rep(5, 5))
  mic_list[[metric]] <- mic_vector
}
cat(mic_list$Goals)

# Traditional
mic_list <- list()
for(metric in metric_list){
  mic_vector <- numeric() 
  for(part in 1:5){
    mic_vector <- c(mic_vector, mic(metric, part, weighted=TRUE, traditional = TRUE))
  }
  names(mic_vector) <- c(rep(1, 1), rep(2, 2), rep(3, 3), rep(4, 4), rep(5, 5))
  mic_list[[metric]] <- mic_vector
}
cat(mic_list$Assists)




# # Positional
# mic_list_pos <- list() 
# i <- 0
# for(position in c("c", "d", "f")){
#   i <- i+1
#   mic_vector_pos <- numeric() 
#   for(metric in metric_list){
#     index <- match(metric, metric_list)
#     metric_name <- metric_names[index]
#     query <- paste0("SELECT * FROM weighted_", metric, "_grouped_named_positional_", position, "_ranked")
#     table <- dbGetQuery(db, query)
#     mic_value <- mine(table[, paste0(metric_name)], 
#                       table[, paste0(metric_name, "_w")])$MIC
#     mic_vector_pos <- c(mic_vector_pos, mic_value)
#     #  print(mic_vector)
#   }
#   names(mic_vector_pos) <- rep(metric_names, times = 1)
#   mic_list_pos[[i]] <- mic_vector_pos
# }
# 
# mic_list_pos


# Re-work this one
summarized <- function(metric, n){
  metric_name <- metric
  metric_name_w <- paste0("Weighted", metric_name)
  metric_name_dplyr <- enquo(metric_name)
  metric_name_dplyr_w <- enquo(metric_name_w)
  results <- data.frame()
  for (i in 1:n){
    query <- paste0("SELECT * FROM weighted_", 
                    metric, "_rankedpartition_", n, "_part", i)
    table <- dbGetQuery(db, query)
    table <- table %>% 
      select(PlayerName, !!metric_name_dplyr, !!metric_name_dplyr_w) %>% 
      group_by(PlayerName) %>% 
      summarise(across(everything(), sum)) %>%
      ungroup()
    
    results <- bind_rows(results, table)
    results <- results %>% 
      group_by(PlayerName) %>% 
      summarise(across(everything(), sum))
  }
  results <- as.data.frame(results)
  mic_value <- mine(results[, paste0(metric_name)], 
                    results[, paste0("Weighted", metric_name)])$MIC

  return(mic_value)
}

mic_list_sum <- list()
for(metric in metric_list){
  mic_vector_sum <- numeric() 
  for(part in 2:5){
    mic_vector_sum <- c(mic_vector_sum, summarized(metric, part))
  }
  #names(mic_vector_sum) <- rep(metric_names, times = 1)
  mic_list_sum[[metric]] <- mic_vector_sum
}

mic_list_sum[["Goals"]]


#### PLOTS ####
goalPerDate <- read.csv("goalsPerDate.csv")
nTimesMetric <- readxl::read_excel("Correlations.xlsx", sheet = "PlotData")

library(ggplot2)
library(dplyr)
library(tidyr)
ggplot(goalPerDate, aes(as.factor(Date), gpd, group = 1)) + 
  geom_path() + theme_minimal() + geom_smooth() + 
  theme(panel.grid = element_blank(), axis.text.x = element_blank())

nTimesMetric %>% 
  filter(Partition != 1) %>% 
  pivot_longer(-c(Metric, Partition, Weighted), 
               names_to = "Corr", values_to = "Value") %>% 
  mutate(Corr = factor(Corr, levels = c("Pearson", "Spearman", "MIC")),
         Metric = factor(Metric, levels = c("Goals", "Assists", "PlusMinus", "Points"))) %>%
  #group_by(Metric, Partition, Corr, Weighted) %>% 
  #mutate(mean_value = mean(Value)) %>% 
  ggplot(., aes(Partition, Value, fill = factor(Weighted), shape = factor(Weighted))) + 
  geom_point(#position=position_dodge(width = 0.3), 
             position=position_dodge(width = 0.5), 
             alpha = 1, size = 1.7, color = "black") +
  #geom_line() +
  #geom_smooth(se=FALSE, fullrange=TRUE, size=1.5,
  #            #position=position_dodge(width = 0.3)
  #            position=position_dodge(width = 0)) + 
  #geom_point(aes(Partition, mean_value), color="red") +
  facet_grid(Corr~Metric) + theme_bw() + 
  xlab("Number of partitions") + ylab("Correlation") +
  scale_fill_manual(name = "", labels = c("Traditional metric", "GPIV metric"),
                     values = c("#E69F00", "#56B4E9")) +
  scale_shape_manual(name = "", labels = c("Traditional metric", "GPIV metric"),
                     values = c(21, 23)) +
  scale_y_continuous(limits = c(0, 1), expand = c(0, 0)) + 
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        axis.title = element_text(face = "bold", size=14),
        axis.text = element_text(size=12),
        legend.position = "top",
        legend.text = element_text(size=16, vjust=1.0),
        strip.text = element_text(face = "bold", size=14),
        panel.spacing = unit(0.8, "lines")
        ) 

ggsave("corr_plot_732a76.svg", width = 7, height = 4.5)
