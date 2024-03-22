# Load libraries ---------------------------------------------------------------
tictoc::tic()
library(magrittr)
library(dplyr)
library(tidyverse)
library(lubridate)
library(readr)
library(arrow)

# print current R version
print(R.version.string)

# Specify command arguments ----------------------------------------------------
args <- commandArgs(trailingOnly=TRUE)
print(length(args))

if(length(args)==0){
  # use for interactive testing
  cohort_name <- "firstmonth"
} else {
  cohort_name <- args[[1]]
}

# Create output folders  -------------------------------------------------------
fs::dir_create(here::here("output", "not-for-review"))
fs::dir_create(here::here("output", "review"))

# Read cohort dataset ----------------------------------------------------------
# read a feather file, decompressed automatically
df <- arrow::read_feather("output/extracts/dataset.arrow")
message(paste0("Dataset has been read successfully with N = ", nrow(df), " rows"))

# Format columns ---------------------------------------------------------------
# dates, numerics, factors, logicals
df <- df %>%
  mutate(across(c(contains("_date")),
                ~ floor_date(as.Date(., format="%Y-%m-%d"), unit = "days")), # rounding down the date to the nearest day
         across(contains('_birth_year'),
                ~ format(as.Date(.), "%Y")),
         across(contains('_num') & !contains('date'), ~ as.numeric(.)),
         across(contains('_cat'), ~ as.factor(.)),
         across(contains('_bin'), ~ as.logical(.)))

# Describe data ----------------------------------------------------------------
sink(paste0("output/not-for-review/describe_",cohort_name,".txt"))
print(Hmisc::describe(df))
sink()
message ("Cohort ",cohort_name, " description written successfully!")

# BMI values -------------------------------------------------------------------
# Remove biologically implausible: BMI numerical (for table 1 only to 12 min and 70 max)
df <- df %>%
  mutate(cov_num_bmi = replace(cov_num_bmi, cov_num_bmi > 70 | cov_num_bmi < 12, NA))

# Combine BMI variables to create one history of obesity variable
df$cov_bin_obesity <- ifelse(df$cov_bin_obesity == TRUE |
                               df$cov_cat_bmi_groups=="Obese",TRUE,FALSE)

# QC for consultation variable--------------------------------------------------
# max to 365 (average of one per day)
df <- df %>%
  mutate(cov_num_consultation_rate = replace(cov_num_consultation_rate,
                                             cov_num_consultation_rate > 365, 365))

#COVID19 severity -------------------------------------------------------------- ### needed?
# df <- df %>%
#   mutate(sub_cat_covid19_hospital =
#            ifelse(!is.na(exp_date_covid19_confirmed) &
#                     !is.na(sub_date_covid19_hospital) &
#                     sub_date_covid19_hospital - exp_date_covid19_confirmed >= 0 &
#                     sub_date_covid19_hospital - exp_date_covid19_confirmed < 29, "hospitalised",
#                   ifelse(!is.na(exp_date_covid19_confirmed), "non_hospitalised",
#                          ifelse(is.na(exp_date_covid19_confirmed), "no_infection", NA)))) %>%
#   mutate(across(sub_cat_covid19_hospital, factor))
# df <- df[!is.na(df$patient_id),]
# df[,c("sub_date_covid19_hospital")] <- NULL
# message("COVID19 severity determined successfully")

# TC/HDL ratio values ----------------------------------------------------------

## Remove biologically implausible: https://doi.org/10.1093/ije/dyz099
## remove TC < 1.75 or > 20
## remove HDL < 0.4 or > 5
df <- df %>%
  mutate(tmp_cov_num_cholesterol = replace(tmp_cov_num_cholesterol, tmp_cov_num_cholesterol < 1.75 | tmp_cov_num_cholesterol > 20, NA),
         tmp_cov_num_hdl_cholesterol = replace(tmp_cov_num_hdl_cholesterol, tmp_cov_num_hdl_cholesterol < 0.4 | tmp_cov_num_hdl_cholesterol > 5, NA)) %>%
  mutate(cov_num_tc_hdl_ratio = tmp_cov_num_cholesterol / tmp_cov_num_hdl_cholesterol) %>%
  mutate(cov_num_tc_hdl_ratio = replace(cov_num_tc_hdl_ratio, cov_num_tc_hdl_ratio > 50 | cov_num_tc_hdl_ratio < 1, NA))
# replace NaN and Inf with NA's (probably only an issue with dummy data)
df$cov_num_tc_hdl_ratio[is.nan(df$cov_num_tc_hdl_ratio)] <- NA
df$cov_num_tc_hdl_ratio[is.infinite(df$cov_num_tc_hdl_ratio)] <- NA
print("Cholesterol ratio variable created successfully and QC'd")
summary(df$cov_num_tc_hdl_ratio)

# Define diabetes variables (using Sophie Eastwood algorithm) -------------------
## First, define age-dependent variables needed for step 5 in diabetes algorithm
df <- df %>%
  mutate(tmp_cov_year_latest_diabetes_diag = format(tmp_cov_date_latest_diabetes_diag,"%Y")) %>%
  mutate(tmp_cov_year_latest_diabetes_diag = as.integer(tmp_cov_year_latest_diabetes_diag),
         age_1st_diag = tmp_cov_year_latest_diabetes_diag - qa_num_birth_year) %>%
  mutate(age_1st_diag = replace(age_1st_diag, which(age_1st_diag < 0), NA)) %>% # assign negative ages to NA
  mutate(age_under_35_30_1st_diag = ifelse(!is.na(age_1st_diag) &
                                             (age_1st_diag < 35 &
                                                (cov_cat_ethnicity == 1 | cov_cat_ethnicity == 2  | cov_cat_ethnicity == 5)) |
                                             (age_1st_diag < 30), "Yes", "No")) %>%
  # HBA1C date var - earliest date for only those with >=47.5
  mutate(hba1c_date_step7 = as_date(case_when(tmp_cov_num_max_hba1c_mmol_mol >= 47.5 ~ pmin(tmp_cov_num_max_hba1c_date, na.rm = TRUE))),
         # process codes - this is taking the first process code date in those individuals that have 5 or more process codes
         over5_pocc_step7 = as_date(case_when(tmp_cov_count_poccdm_ctv3 >= 5 ~ pmin(cov_date_poccdm, na.rm = TRUE))))
print("Diabetes and HbA1c variables needed for algorithm created successfully")

## Second, apply the diabetes algorithm
scripts_dir <- "analysis"
source(file.path(scripts_dir,"diabetes_algorithm.R"))
df <- diabetes_algo(df)
print("Diabetes algorithm run successfully")
print(paste0(nrow(df), " rows in df after diabetes algo"))

# Third, extract T2DM as a separate variable
df <- df %>% mutate(cov_bin_t2dm = case_when(cov_cat_diabetes == "T2DM" ~ T, TRUE ~ F))

# table(df1$cov_cat_diabetes)

# Restrict columns and save analysis dataset df1 -------------------------------
df1 <- df%>% select(patient_id,
                    baseline_date,
                    # "death_date",
                    # starts_with("index_date_"),
                    # has_follow_up_previous_6months,
                    # dereg_date,
                    # starts_with("end_date_"),
                    # contains("sub_"), # Subgroups
                    starts_with("exp_"), # Exposures
                    starts_with("out_"), # Outcomes
                    starts_with("cov_"), # Covariates
                    starts_with("qa_"), # Quality assurance
                    # contains("step"), # diabetes steps
                    # contains("vax_date_eligible"), # Vaccination eligibility
                    # contains("vax_date_"), # Vaccination dates and vax type
                    # contains("vax_cat_")# Vaccination products
)

saveRDS(df1, file = paste0("output/input_",cohort_name,".rds"))
message(paste0("Input data saved successfully with N = ", nrow(df1), " rows"))

# Describe data df 1------------------------------------------------------------
sink(paste0("output/not-for-review/describe_input_",cohort_name,"_stage0.txt"))
print(Hmisc::describe(df1))
sink()

# Restrict columns and save Venn diagram input dataset -------------------------
df2 <- df %>% select(starts_with(c("patient_id","tmp_cov_date","cov_date")))

# Describe data Venn diagram input dataset  ------------------------------------
sink(paste0("output/not-for-review/describe_venn_",cohort_name,".txt"))
print(Hmisc::describe(df2))
sink()
saveRDS(df2, file = paste0("output/venn_",cohort_name,".rds"))
message("Venn diagram data saved successfully")

tictoc::toc()
