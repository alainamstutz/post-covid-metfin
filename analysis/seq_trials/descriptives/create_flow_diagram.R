################################################################################
#
# Size sequential trials
# Number of patients in each trial in each period
#
# The output of this script is:
# csv file ./output/seq_trials/descriptives/flow_diagram/flow_diagram_*ly.csv
# where * is monthly, bimonthly or weekly (_red for redacted file)
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library(readr)
library(data.table)
library(dplyr)
library(tidyr)
library(fs)
library(here)
library(purrr)
library(optparse)
library(tictoc)
source(here::here("lib", "design", "redaction.R"))
source(here::here("analysis", "seq_trials", "descriptives", "functions", "calc_n_excluded_in_each_trial2.R"))
source(here::here("analysis", "seq_trials", "descriptives", "functions", "calc_size_trials.R"))

################################################################################
# 0.1 Create directories for output
################################################################################
output_dir <- here::here("output", "seq_trials", "descriptives", "flow_diagram")
fs::dir_create(output_dir)

################################################################################
# 0.2 Import command-line arguments
################################################################################
args <- commandArgs(trailingOnly=TRUE)
if(length(args)==0){
  # use for interactive testing
  period <- "month"
  period_colname <- paste0("period_", period)
} else {

  option_list <- list(
    make_option("--period", type = "character", default = "month",
                help = "Subsets of data used, options are 'month', '2month', '3month' and 'week' [default %default]. ",
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

################################################################################
# 0.3 Import data
################################################################################
print("Import data")
tic()
data <-
  arrow::read_feather(here("output", "data", "data_processed.feather")) %>%
  mutate(period = .data[[period_colname]])
file_name <- paste0("data_seq_trials_", period, "ly.feather")
trials <- arrow::read_feather(here::here("output", "data", file_name))
toc()

################################################################################
# 1.0 Sense check size of trials based on data
################################################################################
print("Number of individuals excluded in each trial")
tic()
n_excluded_trials <- calc_n_excluded_in_each_trial2(data)
toc()

################################################################################
# 2.0 Number of unique patients included in each arm of trials
################################################################################
print("Number of individuals in each trial arm across periods")
tic()
size_trials <- calc_size_trials(trials)
toc()

################################################################################
# 4.0 Join two tables from step 1 and 2
################################################################################
data_flow <-
  size_trials %>%
  left_join(n_excluded_trials,
            by = c("period", "trial")) %>%
  relocate(starts_with("n_total"), .after = trial)
data_flow_red <-
  data_flow %>%
  mutate(
    across(starts_with("n_"),
           ~ if_else(.x > 0 & .x <= redaction_threshold,
                     "[REDACTED]",
                     .x %>% plyr::round_any(rounding_threshold) %>% as.character()))
  )

################################################################################
# 5.0 Save output
################################################################################
file_name <- paste0("flow_diagram_", period, "ly.csv")
file_name_red <- paste0("flow_diagram_", period, "ly_red.csv")
data.table::fwrite(data_flow,
                   fs::path(output_dir, file_name))
data.table::fwrite(data_flow_red,
                   fs::path(output_dir, file_name_red))
