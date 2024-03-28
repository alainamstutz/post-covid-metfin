# # # # # # # # # # # # # # # # # # # # #
# Purpose: Define the study dates that are used throughout the rest of the project
# Notes:
# This script is separate from the design.R script as the dates are used by the study definition as well as analysis R scripts.
# # # # # # # # # # # # # # # # # # # # #

## Import libraries ----
library('here')

## create output directories ----
fs::dir_create(here::here("lib", "design"))

# define key dates ----
study_dates <- tibble::lst(
  studystart_date = "2020-01-01", # first possible study entry date (start of pandemic in UK)
  studyend_date = "2022-04-01", # last study entry date (end of mass testing in UK)
  # followupend_date = "2023-02-01", # end of follow-up (+ 300 days after last recruitment date)
  # vaccine_peak_date = "2021-06-18", # to stratify analysis / to be discussed
)

jsonlite::write_json(study_dates, path = here("lib", "design", "study-dates.json"), auto_unbox=TRUE, pretty =TRUE)
