# # # # # # # # # # # # # # # # # # # # #
# Purpose: import processed data and filter out people who are excluded from the main analysis
# outputs:
#  - inclusion/exclusions flowchart data (up to matching step)
# # # # # # # # # # # # # # # # # # # # #

# Preliminaries ----

## Import libraries ----
library('tidyverse')
library('here')
library('glue')
library('gt')
library('gtsummary')


## Import custom user functions from lib
source(here("analysis", "functions", "utility.R"))

## Import design elements
source(here("analysis", "design", "design.R"))

## Import redaction functions
source(here("analysis", "functions", "redaction.R"))


## import command-line arguments ----
args <- commandArgs(trailingOnly=TRUE)

if(length(args)==0){
  # use for interactive testing
  removeobjects <- FALSE
  cohort <- "age75plus" #currently `age75plus` or `cv`
} else {
  removeobjects <- TRUE
  cohort <- args[[1]]
}

# derive subgroup info
cohort_sym <- sym(cohort)

## create output directories for data ----
output_dir <- here("output", cohort)
fs::dir_create(output_dir)


# Main ----

## Import processed data ----
data_processed <- read_rds(here("output", "data", "data_processed.rds"))


# Report total number of people vaccinated in time period, by vaccine type ----
# This output ignores cohort, so is the same across different cohorts
# but it is more lightweight than outputting in the `data_process` script, because then
# the release action will need to copy in the entire processed data set

# Report total number of people being prescribed metformin in time period, by ??? ----
# This output ignores cohort, so is the same across different cohorts
# but it is more lightweight than outputting in the `data_process` script, because then
# the release action will need to copy in the entire processed data set

# ## unrounded totals
# total_n_unrounded <-
#   bind_rows(
#     tibble(boost_type="any", n=nrow(data_processed)),
#     count(data_processed %>% mutate(boost_type=fct_other(boost_type, keep=treatment_lookup$treatment, other_level="other")), boost_type, .drop=FALSE)
#   ) %>%
#   mutate(
#     pct = n/first(n)
#   )
# write_csv(total_n_unrounded, fs::path(output_dir, "total_allcohorts_unrounded.csv"))
#
# ## rounded totals
# total_n_rounded <-
#   total_n_unrounded %>%
#   mutate(
#     n= ceiling_any(n, threshold),
#     pct = n/first(n)
#   )
# write_csv(total_n_rounded, fs::path(output_dir, "total_allcohorts_rounded.csv"))


## Quality assurance criteria --------------------------------------------------
data_qa_criteria <- data_processed %>%
  # filter(!!cohort_sym) %>%
  transmute(
    patient_id,
    baseline_date,
    # Rule 1: Year of birth is missing
    rule1 = is.na(qa_num_birth_year),
    # Rule 2: Year of birth is after year of death
    rule2 = (!is.na(qa_date_of_death) & (qa_num_birth_year > year(qa_date_of_death))),
    # Rule 3: Year of birth predates NHS established year or year of birth exceeds current date
    rule3 = (qa_num_birth_year < 1793 | qa_num_birth_year > year(Sys.Date())),
    # Rule 4: Date of death is on or before 1/1/1900 and not NULL or after current date and not NULL
    rule4 = (!is.na(qa_date_of_death) & (qa_date_of_death <= as.Date("1900-01-01")) | !is.na(qa_date_of_death) & (qa_date_of_death > Sys.Date())),
    # Rule 5: Pregnancy/birth codes for men
    rule5 = (qa_bin_pregnancy & cov_cat_sex == "Male"),
    # Rule 6: HRT or COCP meds for men
    rule6 = ((cov_cat_sex == "Male" & qa_bin_hrt) | (cov_cat_sex == "Male" & qa_bin_cocp)),
    # Rule 7: Prostate cancer codes for women
    rule7 = (qa_bin_prostate_cancer & cov_cat_sex == "Female"),

    include = (
      !rule1 & !rule2 & !rule3 & !rule4 & !rule5 & !rule6 & !rule7
    ),
  )

# Keep only the include == TRUE
data_processed_qa <- data_qa_criteria %>%
  filter(include) %>%
  select(patient_id) %>%
  left_join(data_processed, by="patient_id") %>%
  droplevels()

# QA summary
QA_summary <- data.frame(
  rule = character(),
  n_exclude = numeric())
QA_summary <- QA_summary %>%
  add_row(rule = "Rule 1: Year of birth is missing",
          n_exclude = sum(data_qa_criteria$rule1)) %>%
  add_row(rule = "Rule 2: Year of birth is after year of death",
          n_exclude = sum(data_qa_criteria$rule1)) %>%
  add_row(rule = "Rule 3: Year of birth predates NHS established year or year of birth exceeds current date",
          n_exclude = sum(data_qa_criteria$rule2)) %>%
  add_row(rule = "Rule 4: Date of death is invalid (on or before 1/1/1900 or after current date)",
          n_exclude = sum(data_qa_criteria$rule3)) %>%
  add_row(rule = "Rule 5: Pregnancy/birth codes for men",
          n_exclude = sum(data_qa_criteria$rule4)) %>%
  add_row(rule = "Rule 6: HRT or COCP meds for men",
          n_exclude = sum(data_qa_criteria$rule5)) %>%
  add_row(rule = "Rule 7: Prostate cancer codes for women",
          n_exclude = sum(data_qa_criteria$rule6)) %>%
  add_row(rule = "Total excluded from QA",
          n_exclude = nrow(data_qa_criteria) - nrow(data_processed_qa))

# Save QA summary as .csv
# write.csv(QA_summary, file = file.path("output/review/descriptives", paste0("QA_summary_", cohort_name, "_", group, "_v2.csv")), row.names = FALSE)



## Eligibility criteria --------------------------------------------------------
data_criteria <- data_processed_qa %>%
  # filter(!!cohort_sym) %>%
  transmute(
    patient_id,
    treatment,
    # completeness criteria
    is_alive = qa_bin_was_alive == TRUE,
    is_female_or_male = qa_bin_is_female_or_male == TRUE,
    has_imd = qa_bin_known_imd == TRUE,
    has_region = !is.na(cov_cat_region),
    is_registered = qa_bin_was_registered == TRUE,
    # inclusion criteria
    has_adult_age = qa_bin_was_adult == TRUE,
    has_t2dm = cov_bin_t2dm == TRUE,
    has_covid_infection = !is.na(baseline_date),
    # exclusion criteria
    isnot_inhospital = !cov_bin_hosp_baseline,
    notalready_metfin = !cov_bin_metfin_before_baseline,
    no_metfin_allergy = !cov_bin_metfin_allergy,
    no_ckd_history = !cov_bin_ckd_45,
    no_cliver_history = !cov_bin_liver_cirrhosis,
    no_intmefin = !cov_bin_metfin_interaction,
    no_longcovid_history = !cov_bin_long_covid, # short-term outcome (covid-related mortality & covid-related hospitalisation are captured as part of "is_alive" & "isnot_inhospital")
    # mark the include == TRUE
    include = (
      is_alive & is_female_or_male & has_imd & has_region & is_registered &
      has_adult_age & has_t2dm & has_covid_infection &
      isnot_inhospital &
      notalready_metfin &
      no_metfin_allergy &
      no_ckd_history &
      no_cliver_history &
      no_intmefin &
      no_longcovid_history
    ),
  )
# Only keep the include == TRUE
data_cohort <- data_criteria %>%
  filter(include) %>%
  select(patient_id) %>%
  left_join(data_processed_qa, by="patient_id") %>%
  droplevels()

write_rds(data_cohort, fs::path(output_dir, "data_cohort.rds"), compress="gz")
#arrow::write_feather(data_cohort, fs::path(output_dir, "data_cohort.arrow"))

data_inclusioncriteria <- data_criteria %>%
  transmute(
    patient_id,
    treatment,
    c0 = TRUE,
    c1 = c0 & has_covid_infection,
    c2 = c1 & has_adult_age,
    c3 = c2 & has_t2dm,
    c4_1 = c3 & (is_alive & is_female_or_male & has_imd & has_region & is_registered),
    c4_2 = c3 & (no_longcovid_history),
    c4_3 = c3 & (no_intmefin),
    c4_4 = c3 & (no_cliver_history),
    c4_5 = c3 & (no_ckd_history),
    c4_6 = c3 & (no_metfin_allergy),
    c4_7 = c3 & (notalready_metfin),
    c4_8 = c3 & (isnot_inhospital),
    c4 = c4_1 & c4_2 & c4_3 & c4_4 & c4_5 & c4_6 & c4_7 & c4_8
  ) %>%
  filter(c0)

# remove large in-memory objects
remove(data_criteria)
write_rds(data_inclusioncriteria, fs::path(output_dir, "data_inclusioncriteria.rds"), compress="gz")


## Create flowchart ----
create_flowchart <- function(round_level = 1){

  flowchart_output <-
    data_inclusioncriteria %>%
    select(-patient_id, -c0) %>%
    filter(c1) %>%
    group_by(treatment) %>%
    summarise(
      across(.cols=everything(), .fns=~ceiling_any(sum(.), round_level))
    ) %>%
    pivot_longer(
      cols=-c(treatment),
      names_to="criteria",
      values_to="n"
    ) %>%
    mutate(
      level = if_else(str_detect(criteria, "c\\d+$"), 1, 2),
      n_level1 = if_else(level==1, n, NA_real_),
      n_level1_fill = n_level1
    ) %>%
    fill(n_level1_fill) %>%
    group_by(treatment) %>%
    mutate(
      n_exclude = lag(n_level1_fill) - n,
      pct_exclude = n_exclude/lag(n_level1_fill),
      pct_all = n_level1 / first(n),
      pct_step = n_level1 / lag(n_level1_fill),
      #crit = str_extract(criteria, "^c\\d+"),
      crit = criteria,
      criteria = fct_case_when(
        crit == "c1" ~ "with a SARS-CoV-2 infection, defined as evidence of a positive test (PCR or antigen)",
        crit == "c2" ~ "  aged ≥ 18 years",
        crit == "c3" ~ "  with type 2 diabetes mellitus",
        crit == "c4_1" ~ "    no missing demographic information",
        crit == "c4_2" ~ "    no Long COVID before inclusion",
        crit == "c4_3" ~ "    not taking medication interacting with Metformin",
        crit == "c4_4" ~ "    no chronic liver disease",
        crit == "c4_5" ~ "    no chronic kidney disease",
        crit == "c4_6" ~ "    no metformin allergy",
        crit == "c4_7" ~ "    not already taking metformin",
        crit == "c4_8" ~ "    not admitted in hospital at time of inclusion",
        crit == "c4" ~ "  included",
        TRUE ~ "NA_character_boop" # should not appear
      )
    )

  return(flowchart_output)
}


## unrounded flowchart
data_flowchart <- create_flowchart(1)
write_rds(data_flowchart, fs::path(output_dir, "flowchart.rds"))
#write_csv(data_flowchart, here("output", "data", "flowchart.csv"))

## rounded flowchart
data_flowchart_rounded <- create_flowchart(threshold)
write_rds(data_flowchart_rounded, fs::path(output_dir, "flowchart_rounded.rds"))
write_csv(data_flowchart_rounded, fs::path(output_dir, "flowchart_rounded.csv"))

# ## unrounded totals
# total_n_unrounded <-
#   bind_rows(
#     tibble(boost_type="any", n=nrow(data_inclusioncriteria)),
#     count(data_inclusioncriteria %>% mutate(boost_type=fct_other(boost_type, keep=treatment_lookup$treatment, other_level="other")), boost_type, .drop=FALSE)
#   ) %>%
#   mutate(
#     pct = n/first(n)
#   )
# write_csv(total_n_unrounded, fs::path(output_dir, "total_unrounded.csv"))
#
# ## rounded totals
# total_n_rounded <-
#   total_n_unrounded %>%
#   mutate(
#     n= ceiling_any(n, threshold),
#     pct = n/first(n)
#   )
# write_csv(total_n_rounded, fs::path(output_dir, "total_rounded.csv"))

## remove large in-memory objects
remove(data_inclusioncriteria)


# table 1 style baseline characteristics amongst those eligible for matching ----

var_labels <- list(
  N  ~ "Total N",
  treatment_descr ~ "Vaccine type",
  vax_interval ~ "Days since previous vaccine",
  vax_previous_group ~ "Previous vaccine count",
  age_july2023 ~ "Age",
  ageband ~ "Age band",
  sex ~ "Sex",
  ethnicity ~ "Ethnicity",
  imd_Q5 ~ "Deprivation",
  region ~ "Region",
  cv ~ "Clinically at-risk",

  housebound ~ "Clinically housebound",
  care_home_combined ~ "Care/nursing home resident",

  sev_obesity ~ "Body Mass Index > 40 kg/m^2",

  chronic_heart_disease ~ "Chronic heart disease",
  chronic_kidney_disease ~ "Chronic kidney disease",
  diabetes ~ "Diabetes",
  chronic_liver_disease ~ "Chronic liver disease",
  chronic_resp_disease ~ "Chronic respiratory disease",
  asthma ~ "Asthma",
  chronic_neuro_disease ~ "Chronic neurological disease",

  immunosuppressed ~ "Immunosuppressed",
  immuno_any ~ "Immunosuppressed (all)",

  immdx ~ "Immunocompromising diagnosis",
  immrx ~ "Immunosuppressive medications, previous 3 years",
  dxt_chemo ~ "Chemotherapy, previous 3 years",
  cancer ~ "Cancer, previous 3 years",
  asplenia ~ "Asplenia or poor spleen function",
  solid_organ_transplant ~ "Solid organ transplant",
  hiv_aids ~ "HIV/AIDS",

  multimorb ~ "Morbidity count",

  learndis ~ "Learning disabilities",
  sev_mental ~ "Serious mental illness",

  prior_tests_cat ~ "Number of SARS-CoV-2 tests",

  prior_covid_infection ~ "Prior documented SARS-CoV-2 infection",

  vaxhist_pfizer  ~ "Previously received Pfizer (original)",
  vaxhist_az  ~ "Previously received AZ",
  vaxhist_moderna  ~ "Previously received Moderna",
  vaxhist_pfizerBA1  ~ "Previously received Pfizer/BA.1",
  vaxhist_pfizerXBB15  ~ "Previously received Pfizer/XBB.1.5",
  vaxhist_modernaomicron  ~ "Previously received Moderna/Omicron",
  vaxhist_modernaXBB15  ~ "Previously received Moderna/XBB.1.5"
) %>%
  set_names(., map_chr(., all.vars))



tab_summary_prematch <-
  data_cohort %>%
  mutate(
    N=1L,
    treatment_descr = fct_recoderelevel(as.character(treatment), recoder$treatment),
  ) %>%
  select(
    treatment_descr,
    all_of(names(var_labels)),
  ) %>%
  tbl_summary(
    by = treatment_descr,
    label = unname(var_labels[names(.)]),
    statistic = list(
      N ~ "{N}",
      all_continuous() ~ "{median} ({p25}, {p75});  {mean} ({sd})"
    ),
  )


raw_stats <- tab_summary_prematch$meta_data %>%
  select(var_label, df_stats) %>%
  unnest(df_stats)

raw_stats_redacted <- raw_stats %>%
  mutate(
    n = roundmid_any(n, threshold),
    N = roundmid_any(N, threshold),
    p = n / N,
    N_miss = roundmid_any(N_miss, threshold),
    N_obs = roundmid_any(N_obs, threshold),
    p_miss = N_miss / N_obs,
    N_nonmiss = roundmid_any(N_nonmiss, threshold),
    p_nonmiss = N_nonmiss / N_obs,
    var_label = factor(var_label, levels = map_chr(var_labels[-c(1, 2)], ~ last(as.character(.)))),
    variable_levels = replace_na(as.character(variable_levels), "")
  )

write_csv(raw_stats_redacted, fs::path(output_dir, "table1.csv"))


#
# # love / smd plot ----
#
# data_smd <- tab_summary_baseline$meta_data %>%
#   select(var_label, df_stats) %>%
#   unnest(df_stats) %>%
#   filter(
#     variable != "N"
#   ) %>%
#   group_by(var_label, variable_levels) %>%
#   summarise(
#     diff = diff(p),
#     sd = sqrt(sum(p*(1-p))),
#     smd = diff/sd
#   ) %>%
#   ungroup() %>%
#   mutate(
#     variable = factor(var_label, levels=map_chr(var_labels[-c(1,2)], ~last(as.character(.)))),
#     variable_card = as.numeric(variable)%%2,
#     variable_levels = replace_na(as.character(variable_levels), ""),
#   ) %>%
#   arrange(variable) %>%
#   mutate(
#     level = fct_rev(fct_inorder(str_replace(paste(variable, variable_levels, sep=": "),  "\\:\\s$", ""))),
#     cardn = row_number()
#   )
#
# write_csv(data_smd, fs::path(output_dir, "smd.csv"))

