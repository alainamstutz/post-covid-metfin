################################################################################
#
# Processing data
#
# This script can be run via an action in project.yaml
#
# The output of this script is:
# -./output/data/data_processed.rds
# - ./output/data_properties/n_excluded.rds
#
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library('dplyr')
library('lubridate')
library('here')
library('readr')
library('purrr')
## Import custom user functions
source(here::here("analysis", "data_import", "extract_data.R"))
source(here::here("analysis", "data_import", "process_data.R"))

source(here::here("analysis", "data_import", "calc_n_excluded.R"))
source(here::here("analysis", "data_import", "calc_n_excluded_contraindicated.R"))
source(here::here("analysis", "data_import", "functions", "add_contraindicated_indicator.R"))

################################################################################
# 0.1 Create directories for output
################################################################################
fs::dir_create(here::here("output", "data"))
fs::dir_create(here::here("output", "data_properties"))

################################################################################
# 0.2 Import command-line arguments
################################################################################
args <- commandArgs(trailingOnly=TRUE)
study_dates <-
    jsonlite::read_json(path = here::here("lib", "design", "study-dates.json")) %>%
    map(as.Date)

################################################################################
# 1 Import data
################################################################################
input_filename <- "dataset.arrow"
data_extracted <-
  extract_data(input_filename)

# # change data if run using dummy data
# if(Sys.getenv("OPENSAFELY_BACKEND") %in% c("", "expectations")){
#   data_extracted <-
#     data_extracted %>%
#     mutate(died_ons_covid_any_date =
#              if_else(!is.na(death_date), death_date, died_ons_covid_any_date),
#            death_date =
#              if_else(!is.na(died_ons_covid_any_date), died_ons_covid_any_date, death_date),
#            date_treated = if_else(!is.na(date_treated),
#                                   covid_test_positive_date + runif(nrow(data_extracted), 0, 4) %>% round(),
#                                   NA_Date_),
#            paxlovid_covid_therapeutics = if_else(!is.na(paxlovid_covid_therapeutics),
#                                                  date_treated,
#                                                  NA_Date_),
#            sotrovimab_covid_therapeutics = if_else(!is.na(sotrovimab_covid_therapeutics),
#                                                    date_treated,
#                                                    NA_Date_),
#            molnupiravir_covid_therapeutics = if_else(!is.na(molnupiravir_covid_therapeutics),
#                                                      date_treated,
#                                                      NA_Date_)
#     )
# }

################################################################################
# 2 Process data
################################################################################
data_processed_g7 <- process_data(data_extracted, study_dates, treat_window_days = 6) # grace period 7 dataset only
# data_processed_g7 <- data_processed_g7 %>%
#   select(patient_id, baseline_date, period_week, period_month, period_2month, period_3month) %>%
#   View()

data_processed <-
  map(.x = list(6, 7, 8, 9), # add additional longer grace periods besides the # primary grace period 7 days (baseline_date + 6)
      .f = ~ process_data(data_extracted, study_dates, treat_window_days = .x))
names(data_processed) <- c("grace7", "grace8", "grace9", "grace10")

# change data if run using dummy data
if(Sys.getenv("OPENSAFELY_BACKEND") %in% c("", "expectations")){
  data_processed <-
    map(.x = data_processed,
        .f = ~ .x %>% group_by(patient_id) %>%
          mutate(period_month = runif(1, 0, 12) %>% ceiling(),
                 period_2month = runif(1, 0, 6) %>% ceiling(),
                 period_3month = runif(1, 0, 4) %>% ceiling(),
                 period_week = runif(1, 0, 52) %>% ceiling(),
                 covid_test_positive_date = sample(seq(ymd("20220210"), ymd("20230209"), by = 1), 1),
                 tb_postest_vacc_cat = sample(c(">= 84 days or unknown", "< 7 days", "7-27 days", "28-83 days"), 1) %>%
                   factor(levels = c(">= 84 days or unknown", "< 7 days", "7-27 days", "28-83 days"))) %>%
          ungroup() %>%
          mutate(ageband = if_else(is.na(ageband), "18-39", ageband %>% as.character()) %>%
                   factor(levels = c("18-39", "40-59", "60-79", "80+"))))
}

################################################################################
# 3 Apply additional eligibility and exclusion criteria
################################################################################
# calc n excluded
n_excluded <- calc_n_excluded(data_processed$grace5)

data_processed <-
  map(.x = data_processed,
      .f = ~ .x %>%
        # Exclude patients treated with paxlovid and molnupiravir or sotrovimab on the
        # same day
        filter(treated_pax_mol_same_day  == 0 & treated_pax_sot_same_day  == 0) %>%
        # Exclude patients hospitalised on day of positive test
        filter(!(status_all %in% c("covid_hosp", "noncovid_hosp", "covid_death", "noncovid_death") &
                   fu_all == 0)) %>%
        # if treated with remidesivir --> exclude
        filter(is.na(remdesivir_covid_therapeutics)))

# contraindications
n_excluded_contraindicated <- calc_n_excluded_contraindicated(data_processed$grace5)
data_processed <-
  map(.x = data_processed,
      .f = ~ .x %>% add_contraindicated_indicator())
data_processed_excl_contraindicated  <-
  map(.x = data_processed,
      .f = ~ .x %>% filter(contraindicated == FALSE))

################################################################################
# 4 Save data
################################################################################
# data_processed are saved
iwalk(.x = data_processed,
      .f = ~ write_rds(.x,
                       here::here("output", "data",
                                  paste0(
                                    "data_processed",
                                    "_"[!.y == "grace5"],
                                    .y[!.y == "grace5"],
                                    ".rds"))))
iwalk(.x = data_processed_excl_contraindicated,
      .f = ~ write_rds(.x,
                       here::here("output", "data",
                                  paste0(
                                    "data_processed_excl_contraindicated",
                                    "_"[!.y == "grace5"],
                                    .y[!.y == "grace5"],
                                    ".rds"))))
write_rds(n_excluded,
          here::here("output", "data_properties", "n_excluded.rds"))
write_rds(n_excluded_contraindicated,
          here::here("output", "data_properties", "n_excluded_contraindicated.rds"))
