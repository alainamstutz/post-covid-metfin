################################################################################
#
# Select data to optimise code speed (not having script rely on data_process)
#
# The output of this script is:
# feather file ./output/data/data_processed_excl_contraindicated.feather
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library(readr)
library(here)
library(arrow)
library(dplyr)
source(here::here("analysis", "seq_trials", "functions", "simplify_data.R"))
source(here::here("lib", "design", "covars_seq_trials.R"))

################################################################################
# 0.1 Create directories for output
################################################################################
output_dir <- here::here("output", "data")
fs::dir_create(output_dir)

################################################################################
# 0.2 Import data
################################################################################
file_name <- "data_processed"
data <-
  read_rds(here("output", "data", paste0(file_name, ".rds"))) %>%
  simplify_data() %>%
  select(patient_id,
         status_seq,
         tb_postest_treat_seq,
         treatment_seq,
         # tb_postest_treat_seq_sotmol,
         # treatment_seq_sotmol,
         fup_seq,
         baseline_date,
         all_of(covars),
         dplyr::starts_with("period_"))

# check data: all NA, should be FALSE or NA? Rename "missing"

################################################################################
# 2.0 Save output
################################################################################.
write_feather(data, fs::path(output_dir, paste0(file_name, ".feather")))
