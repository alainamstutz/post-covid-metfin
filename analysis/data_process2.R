######################################
# Purpose: process (tidy, clean, reshape, derive) data extracted using ehrQL (or dummy data)
#
# standardises some variables (eg convert to factor) and derives some new ones
# organises vaccination date data to "vax X type", "vax X date" in long format
######################################

# Preliminaries ----
tictoc::tic()

## Import libraries ----
library('tidyverse')
library('lubridate')
library('arrow')
library('here')
# library(magrittr)
# library(readr)

# print current R version

print(R.version.string)

## Import custom user functions from lib
source(here("analysis", "functions", "utility.R"))

## Import design elements
source(here("analysis", "design", "design.R"))

## create output directories for data ----------------------------------------
fs::dir_create(here("output", "data"))
fs::dir_create(here("output", "not-for-review"))
fs::dir_create(here("output", "review"))

# Import and process data ----

## Import extracted dataset ----
df <- read_feather(here("output", "extracts", "dataset.arrow"))

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


# saveRDS(df1, file = paste0("output/input_",cohort_name,".rds"))
# message(paste0("Input data saved successfully with N = ", nrow(df1), " rows"))
#
# # Describe data df 1------------------------------------------------------------
# sink(paste0("output/not-for-review/describe_input_",cohort_name,"_stage0.txt"))
# print(Hmisc::describe(df1))
# sink()
#
# # Restrict columns and save Venn diagram input dataset -------------------------
# df2 <- df %>% select(starts_with(c("patient_id","tmp_cov_date","cov_date")))
#
# # Describe data Venn diagram input dataset  ------------------------------------
# sink(paste0("output/not-for-review/describe_venn_",cohort_name,".txt"))
# print(Hmisc::describe(df2))
# sink()
# saveRDS(df2, file = paste0("output/venn_",cohort_name,".rds"))
# message("Venn diagram data saved successfully")




## Process extracted dataset ----
data_processed <- df1 %>%
  mutate(

    # use short product names
    # boost_type = factor(boost_type, vax_product_lookup, names(vax_product_lookup)) %>% fct_explicit_na("other"),

    # binary variable for the exposure
    # helpful for various downstream matching / plotting / table functions
    treatment = case_when(
      exp_bin_7d_metfin == TRUE ~ 1L,
      exp_bin_7d_metfin == FALSE ~ 0L,
      TRUE ~ NA_integer_
    ),

    # boost date represented as an integer, using for matching instead of date-formatted variable to avoid issues
    # boost_day = as.integer(boost_date - study_dates$studystart_date),

    # ageband = cut(
    #   age_july2023, # use fixed date to ascertain age so that age bands align with eligibility. because age and vax date are closely matched, this doesn't cause any problems
    #   breaks=c(-Inf, 50, 65, 75, 80, 85, Inf),
    #   labels=c("under 50", "50-64", "65-74", "75-79", "80-84", "85+"),
    #   right=FALSE
    # ),

    cov_cat_ethnicity = fct_case_when(
      cov_cat_ethnicity == "1" ~ "White",
      cov_cat_ethnicity == "4" ~ "Black",
      cov_cat_ethnicity == "3" ~ "South Asian",
      cov_cat_ethnicity == "2" ~ "Mixed",
      cov_cat_ethnicity == "5" ~ "Other",
      TRUE ~ "Unknown"
    ),

    cov_cat_region = fct_collapse(
      cov_cat_region,
      `East of England` = "East",
      `London` = "London",
      `Midlands` = c("West Midlands", "East Midlands"),
      `North East and Yorkshire` = c("Yorkshire and The Humber", "North East"),
      `North West` = "North West",
      `South East` = "South East",
      `South West` = "South West"
    ),

    # imd_Q5 = cut(
    #   imd,
    #   breaks = (32844/5)*c(-0.1,1,2,3,4,5),
    #   labels = c("1 most deprived", "2", "3", "4", "5 least deprived"),
    #   include.lowest = TRUE,
    #   right = FALSE
    # ),

    cov_cat_rural_urban = fct_case_when(
      cov_cat_rural_urban %in% c(1,2) ~ "Urban conurbation",
      cov_cat_rural_urban %in% c(3,4) ~ "Urban city or town",
      cov_cat_rural_urban %in% c(5,6,7,8) ~ "Rural town or village",
      TRUE ~ NA_character_
    ),

    # care_home_combined = care_home_tpp | care_home_code, # any carehome flag

    # immuno_any = immunosuppressed | asplenia | cancer | solid_organ_transplant |  hiv_aids,

    # clinically at-risk group
    # cv = immuno_any | chronic_kidney_disease | chronic_resp_disease | diabetes | chronic_liver_disease |
    #   chronic_neuro_disease | chronic_heart_disease | learndis | sev_mental,
    #
    # multimorb =
    #   (sev_obesity) +
    #   (chronic_heart_disease) +
    #   (chronic_kidney_disease)+
    #   (diabetes) +
    #   (chronic_liver_disease)+
    #   (chronic_resp_disease)+
    #   (chronic_neuro_disease)+
    #   (cancer)+
    #   #(learndis)+
    #   #(sev_mental),
    #   0,
    # multimorb = cut(multimorb, breaks = c(0, 1, 2, Inf), labels=c("0", "1", "2+"), right=FALSE),

    ## define additional subgroups used for subgroup analyses
    # all=factor("all"),

    # age75plus = age_july2023>=75,

    # prior_tests_cat = cut(prior_covid_test_frequency, breaks=c(0, 1, 2, 3, Inf), labels=c("0", "1", "2", "3+"), right=FALSE),

    # prior_covid_infection = (!is.na(postest_0_date))  | (!is.na(covidemergency_0_date)) | (!is.na(covidadmitted_0_date)) | (!is.na(primary_care_covid_case_0_date)),


    ## process outcomes data

#     # latest covid event before study start
#     anycovid_0_date = pmax(postest_0_date, covidemergency_0_date, covidadmitted_0_date, na.rm=TRUE),
#
#     coviddeath_date = if_else(death_cause_covid, death_date, NA_Date_),
#     noncoviddeath_date = if_else(!is.na(death_date) & is.na(coviddeath_date), death_date, as.Date(NA_character_)),
#
#     # replace events dates with more severe dates if the precede less severe dates
#     #covidemergency_date = pmin(covidemergency_date, covidadmitted_date, covidcritcare_date, coviddeath_date, na.rm=TRUE),
#     #covidadmitted_date = pmin(covidadmitted_date, covidcritcare_date, coviddeath_date, na.rm=TRUE),
#     #covidcritcare_date = pmin(covidcritcare_date, coviddeath_date, na.rm=TRUE),
#
#     # earliest covid event after study start
#     anycovid_date = pmin(postest_date, covidemergency_date, covidadmitted_date, coviddeath_date, na.rm=TRUE),
#
#     # cause_of_death = fct_case_when(
#     #   !is.na(coviddeath_date) ~ "covid-related",
#     #   !is.na(death_date) ~ "not covid-related",
#     #   TRUE ~ NA_character_
#     # ),
#
#     cause_of_death = fct_case_when(
#       death_cause_covid ~ "covid-related",
#       !death_cause_covid ~ "not covid-related",
#       TRUE ~ NA_character_
#     ),
#
#     fracturedeath_date = if_else(death_cause_fracture, death_date, NA_Date_),
#     fracture_date = pmin(fractureemergency_date, fractureadmitted_date, fracturedeath_date, na.rm=TRUE),
#
#     pericarditisdeath_date = if_else(death_cause_pericarditis, death_date, NA_Date_),
#     pericarditis_date = pmin(pericarditisemergency_date, pericarditisadmitted_date, pericarditisdeath_date, na.rm=TRUE),
#
#     myocarditisdeath_date = if_else(death_cause_myocarditis, death_date, NA_Date_),
#     myocarditis_date = pmin(myocarditisemergency_date, myocarditisadmitted_date, myocarditisdeath_date, na.rm=TRUE),
#
#     # define cohorts
#
#     age75plus = age_july2023 >= 75,
#     #cv = cv,
#     is_eligible = age75plus | cv,
#
   )

# # Process vaccination dates ----
#
# ## Reshape vaccination data from wide to long ----
# data_vax <-
#   data_processed %>%
#   select(
#     patient_id,
#     matches("covid_vax\\_\\d+\\_date"),
#     matches("covid_vax_type_\\d+"),
#   ) %>%
#   pivot_longer(
#     cols = -patient_id,
#     names_to = c(".value", "vax_index"),
#     names_pattern = "^(.*)_(\\d+)",
#     values_drop_na = TRUE,
#     names_transform = list(vax_index = as.integer)
#   ) %>%
#   rename(
#     vax_date = covid_vax,
#     vax_type = covid_vax_type,
#   ) %>%
#   # relabel vaccination codes from long ttp product names to short readable names
#   mutate(
#     vax_type = fct_recode(factor(vax_type, vax_product_lookup), !!!vax_product_lookup) %>% fct_explicit_na("other")
#   ) %>%
#
#   # calculate time between vaccine intervals
#   arrange(patient_id, vax_date) %>%
#   group_by(patient_id) %>%
#   mutate(
#     vax_interval = as.integer(vax_date - lag(vax_date,1)),
#     duplicate = vax_interval==0
#   ) %>%
#   ungroup() %>%
#
#   # remove vaccinations that occur after booster date
#   # boost_date, as per dataset_definition, is the first vaccination to occur between 1 April and 30 June 2023
#   left_join(
#     data_processed %>% select(patient_id, boost_date), by = "patient_id"
#   ) %>%
#   filter(
#     vax_date<=boost_date
#   )
#
# write_rds(data_vax, here("output", "data", "data_vaxlong.rds"), compress="gz")
#
# ## Summarise vaccination history to one-row-per-patient info ----
# data_vax_history <- data_vax %>%
#   group_by(patient_id) %>%
#   summarise(
#     vax_count = n(),
#     vax_previous_count = last(vax_index)-1,
#     vax_interval = last(vax_interval),
#     vax_intervals_atleast14days = all(vax_interval[-1]>=14),
#     vax_dates_possible = first(vax_date)>=as.Date("2020-06-01"), # earlier than 2020-12-08 to include trial participants, but excludes "unknowns" coded as eg 1900-01-01
#     vaxhist_pfizer = "pfizer" %in% (vax_type[-vax_count]),
#     vaxhist_az = "az" %in% (vax_type[-vax_count]),
#     vaxhist_moderna = "moderna" %in% (vax_type[-vax_count]),
#     vaxhist_pfizerBA1 = "pfizerBA1" %in% (vax_type[-vax_count]),
#     vaxhist_pfizerBA45 = "pfizerBA45" %in% (vax_type[-vax_count]),
#     vaxhist_pfizerXBB15 = "pfizerXBB15" %in% (vax_type[-vax_count]),
#     vaxhist_sanofi = "sanofi" %in% (vax_type[-vax_count]),
#     vaxhist_modernaomicron = "modernaomicron" %in% (vax_type[-vax_count]),
#     vaxhist_modernaXBB15 = "modernaXBB15" %in% (vax_type[-vax_count]),
#   ) %>%
#   ungroup() %>%
#   mutate(
#     vax_interval_bigM = if_else(vax_previous_count==0, 365L*5L, vax_interval), #if spring "booster" is first recorded vaccine, then set vax_interval to be very large
#     vax_previous_group = cut(
#       vax_previous_count,
#       breaks = c(0,2,5,6, Inf),
#       labels = c("0-1", "2-4", "5", "6+"),
#       include.lowest = TRUE,
#       right=FALSE
#     )
#   )
#
# stopifnot("vax_count should be equal to vax_previous_count+1" = all(data_vax_history$vax_count == data_vax_history$vax_previous_count+1))
# table(data_vax_history$vaxhist_sanofi)
# table(data_vax_history$vaxhist_pfizerBA45)

# Combine and output ----

# data_processed <- data_processed %>%
#   left_join(data_vax_history, by ="patient_id") %>%
#   select(-starts_with("covid_vax_"))

write_rds(data_processed, here("output", "data", "data_processed.rds"), compress="gz")

tictoc::toc()
