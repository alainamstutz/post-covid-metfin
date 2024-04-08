################################################################################
#
# Data wrangling seq trials
#
# The output of this script is:
# feather file ./output/data/data_seq_trials_*.feather
# where * \in (monthly, bimonthly, weekly)
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library(readr)
library(magrittr)
library(dplyr)
library(fs)
library(here)
library(purrr)
library(optparse)
library(arrow)
library(tictoc)
library(doParallel)
library(foreach)
library(magrittr)
print("Sourcing functions")
tic()
source(here::here("analysis", "seq_trials", "functions", "split_data.R"))
source(here::here("analysis", "seq_trials", "functions", "add_trt_lags.R"))
source(here::here("analysis", "seq_trials", "functions", "construct_trial_no.R"))
source(here::here("analysis", "seq_trials", "functions", "construct_trials.R"))
source(here::here("analysis", "seq_trials", "functions", "construct_trial2.R"))
toc()

################################################################################
# 0.1 Create directories for output
################################################################################
print("Create output directory")
tic()
output_dir <- here::here("output", "data")
fs::dir_create(output_dir)
toc()

################################################################################
# 0.2 Import command-line arguments
################################################################################
print("Import command-line arguments")
tic()
args <- commandArgs(trailingOnly=TRUE)
if(length(args)==0){
  # use for interactive testing
  period <- "month"
  period_colname <- paste0("period_", period)
} else {
  option_list <- list(
    make_option("--period", type = "character", default = "month",
                help = "Subsets in wich data is cut, options are 'month', '2month', '3month' and 'week' [default %default]. ",
                metavar = "period")
  )
  opt_parser <- OptionParser(usage = "prepare_data:[version] [options]", option_list = option_list)
  opt <- parse_args(opt_parser)
  period <- opt$period
  period_colname <- paste0("period_", period)
}
study_dates <-
  jsonlite::read_json(path = here::here("lib", "design", "study-dates.json")) %>%
  map(as.Date)
toc()

################################################################################
# 0.3 Import data
################################################################################
print("Import data")
tic()
data <-
  read_feather(here("output", "data", "data_processed.feather")) %>%
  mutate(period = .data[[period_colname]]) %>%
  select(-starts_with("period_"))
cuts <- data %>% pull(period) %>% unique() %>% sort()
toc()

################################################################################
# 0.4 Data manipulation
################################################################################
print("Split data")
tic()
data_splitted <-
  data %>%
  group_by(patient_id) %>%
  split_data() %>% # create follow-up splitting by day (i.e. daily splitting until event/end of FUP, creating daily tstart-tend intervals), irrespective of period
  add_trt_lags() %>%
  ungroup()
toc()
size_data_splitted <- object.size(data_splitted)
format(size_data_splitted, units = "Mb", standard = "SI")

# print("Remove data to clean up memory")
# tic()
# rm(data)
# toc()

# make dummy data better
if(Sys.getenv("OPENSAFELY_BACKEND") %in% c("", "expectations")){
  # data_splitted <-
  #   data_splitted %>%
  #   select(patient_id, tstart, tend, period, status_seq, starts_with("treatment_seq"))
}

################################################################################
# 1.0 Construct trials (eg monthly, bimonthly and weekly, depending on input args)
################################################################################
# print("Construct trials")
# tic()
# nCores <- detectCores() - 1
# print(nCores)
# cluster <- parallel::makeForkCluster(cores = nCores)
# registerDoParallel(cluster)
# getDoParWorkers() %>% print()
# trials <-
#   foreach(i = cuts, .combine = rbind, .packages = "magrittr") %:%
#   foreach(j = 0:4, .combine = rbind, .packages = "magrittr") %dopar% {
#     construct_trial2(
#       data = data_splitted,
#       period = i,
#       trial_no = j
#     )}
# stopCluster(cluster)
# toc()
print("Construct trials 2")
tic()
cluster <- parallel::makeForkCluster(cores = 4) #  set up a fork-based parallel processing environment with four cores in R, allowing for parallel execution of code to speed up computations
registerDoParallel(cluster)

trials <- # create 7 trials per participants (0-6), according to grace period 7, keeping them in their respective period (see below), according to cuts.
  foreach(i = cuts, .combine = rbind, .packages = "magrittr") %dopar% { # Initiates a loop over the elements of cuts, with i as the loop variable and the results from each iteration will be stacked vertically (by rows). "package" is important because the %>% pipe operator from magrittr is used within the loop.
    construct_trials(
      data = data_splitted,
      period = i,
      treat_window = 7,
      construct_trial_no = construct_trial_no
    )}

stopCluster(cluster)
toc()

# "keeping them in their respective period"
table(data$period) # 6 participants, across 4 periods during study duration (period 19, 21, 24, 27)
table(data_splitted$period) # 6x28 participant-days (28 daily follow-up), across 4 periods during study duration (period 19, 21, 24, 27)
table(trials$period)
# Period 19/21/24 person: Trial 0 (28 days) + Trial 1 (27 days) + Trial 2 (26 days) + Trial 3 (25 days) + Trial 4 (24 days) + Trial 5 (23 days) + Trial 6 (22 days) => 175 rows
# Period 27 people (3x): 3x175

table(trials$trial) # why shorter follow-up for later trials?
# check lag variables / next step?

# print("Construct trials 3")
# tic()
# trials <-
#   map_dfr(
#     .x = cuts,
#     .f = ~
#       construct_trials(
#         data = data_splitted,
#         period = .x,
#         5,
#         construct_trial_no = construct_trial_no)
#     )
# toc()

trials %<>%
  mutate(arm = factor(arm, levels = c(0, 1)),
         trial = factor(trial, levels = 0:6), # grace period 7 days, i.e. 7 trials, i.e. 0-6
         period = factor(period, levels = 1:27)) # 27 months total study period


size_trials <- object.size(trials)
format(size_trials, units = "Mb", standard = "SI")

################################################################################
# 2.0 Save output
################################################################################
print("Save output")
tic()
file_name <- paste0("data_seq_trials_", period, "ly.feather")
write_feather(trials, fs::path(output_dir, file_name))
toc()
