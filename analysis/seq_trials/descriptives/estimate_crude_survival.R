################################################################################
#
# INTENTION TO TREAT ANALYSIS
# 
# 
# The output of this script is:
# csv file ./output/seq_trials/itt/itt_fit_*.csv
# where * is simple or interaction_*
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library(dplyr)
library(tidyr)
library(fs)
library(here)
library(purrr)
library(data.table)
library(glue)

################################################################################
# 0.1 Create directories for output
################################################################################
output_dir <- here::here("output", "seq_trials", "descriptives", "survival")
fs::dir_create(output_dir)

################################################################################
# 0.2 Import command-line arguments
################################################################################

################################################################################
# 0.3 Import data
################################################################################
trials <- arrow::read_feather(here("output", "data", "data_seq_trials_monthly.feather"))

################################################################################
# 1. Frequency of outcomes
################################################################################
surv <-
  trials %>%
  group_by(arm, tend) %>%
  summarise(n_outcomes = sum(status_seq),
            n = n(),
            n_survived = n - n_outcomes,
            p_outcome = n_outcomes / n,
            p_surv = n_survived / n) %>%
  ungroup() %>%
  filter(n_outcomes != 0) %>%
  group_by(arm) %>%
  mutate(surv = cumprod(p_surv),
         cum_inc2 = 1 - surv)
surv_by_period <-
  surv_main <-
  trials %>%
  group_by(period, arm, tend) %>%
  summarise(n_outcomes = sum(status_seq),
            n = n(),
            n_survived = n - n_outcomes,
            p_outcome = n_outcomes / n,
            p_surv = n_survived / n,
            .groups = "keep") %>%
  ungroup() %>%
  filter(n_outcomes != 0) %>%
  group_by(period, arm) %>%
  mutate(surv = cumprod(p_surv),
         cum_inc = 1 - surv)
surv_by_trial <-
  surv_main <-
  trials %>%
  group_by(trial, arm, tend) %>%
  summarise(n_outcomes = sum(status_seq),
            n = n(),
            n_survived = n - n_outcomes,
            p_outcome = n_outcomes / n,
            p_surv = n_survived / n,
            .groups = "keep") %>%
  ungroup() %>%
  filter(n_outcomes != 0) %>%
  group_by(trial, arm) %>%
  mutate(surv = cumprod(p_surv),
         cum_inc = 1 - surv)

################################################################################
# 2. Save output
################################################################################
iwalk(.x = list("main" = surv, "by_period" = surv_by_period, "by_trial" = surv_by_trial),
      .f = ~ data.table::fwrite(
        .x,
        fs::path(output_dir, glue("surv_", .y, ".csv")))
)
