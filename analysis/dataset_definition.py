# IMPORT

## ehrQL
from ehrql import (
    case,
    codelist_from_csv,
    create_dataset,
    days,
    when,
    weeks,
    minimum_of,
    maximum_of
)
from ehrql.tables.beta.tpp import (
    addresses,
    clinical_events,
    hospital_admissions,
    medications,
    patients,
    practice_registrations,
    ons_deaths,
    sgss_covid_all_tests,
    hospital_admissions,
    ethnicity_from_sus,
    vaccinations, 
)

## Study definition helper
import study_definition_helper_functions as helpers

## from codelists.py
from codelists import *

## datetime function
from datetime import date


# DEFINE the DATES
pandemic_start_date = "2020-01-01"
vaccine_peak_date = "2021-06-18"
medical_history_date = "1990-01-01" # e.g. to define DM diagnosis
index_date = pandemic_start_date


# START the dataset and set the dummy dataset size
dataset = create_dataset()
dataset.configure_dummy_data(population_size=50)




# population variables for dataset definition
is_female_or_male = patients.sex.is_in(["female", "male"]) # only include f, m and no missing values
was_adult = (patients.age_on(index_date) >= 18) & (patients.age_on(index_date) <= 110) # only include adults and no missing values
was_alive = (patients.date_of_death.is_after(index_date) | patients.date_of_death.is_null()) # only include if alive 
was_registered = practice_registrations.for_patient_on(index_date).exists_for_patient() # only include if registered on index date

# define/create dataset
dataset.define_population(
    is_female_or_male
    & was_adult
    & was_alive
    & was_registered
) 




# FUNCTIONS -----------------------------------------------------------------

## primary care 
## BEFORE INDEX DATE
# Any events occurring BEFORE INDEX DATE (from codelist / clinical_events) 
prior_events = clinical_events.where(clinical_events.date.is_on_or_before(index_date))
def has_prior_event_snomed(codelist, where=True):
    return (
        prior_events.where(where)
        .where(prior_events.snomedct_code.is_in(codelist))
        .exists_for_patient()
    )
def has_prior_event_ctv3(codelist, where=True):
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .exists_for_patient()
    )
# Most recent event occurring BEFORE INDEX DATE (from codelist / clinical_events)
def prior_event_date_ctv3(codelist, where=True):
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .sort_by(prior_events.date)
        .last_for_patient()
        .date
    )
# Count prior events occurring BEFORE INDEX DATE (from codelist / clinical_events) 
def prior_events_count_ctv3(codelist, where=True):
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .count_for_patient()
    )
## ON or AFTER MEDICAL HISTORY DATE
# First date of event occurring ON or AFTER MEDICAL HISTORY DATE (from codelist / clinical_events)
after_events = clinical_events.where(clinical_events.date.is_on_or_after(medical_history_date))
def after_event_date_ctv3(codelist, where=True):
    return (
        after_events.where(where)
        .where(after_events.ctv3_code.is_in(codelist))
        .sort_by(after_events.date)
        .first_for_patient()
        .date
    )
# Count all events occurring ON or AFTER MEDICAL HISTORY DATE (from codelist / clinical_events) 
def after_events_count_ctv3(codelist, where=True):
    return (
        after_events.where(where)
        .where(after_events.ctv3_code.is_in(codelist))
        .count_for_patient()
    )

### admissions
## BEFORE INDEX DATE
# Most recent admission occurring BEFORE INDEX DATE (from codelist / hospital_admissions)
prior_admissions = hospital_admissions.where(hospital_admissions.admission_date.is_on_or_before(index_date))
def prior_admission_date(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .sort_by(prior_admissions.admission_date)
        .last_for_patient()
        .admission_date
    )
# Count prior admission occurring BEFORE INDEX DATE (from codelist / hospital_admissions) 
def prior_admissions_count(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .count_for_patient()
    )
## ON or AFTER MEDICAL HISTORY DATE
# First date of admission for diagnosis from codelist occurring ON or AFTER MEDICAL HISTORY DATE (from codelist / hospital_admissions)
after_admissions = hospital_admissions.where(hospital_admissions.admission_date.is_on_or_after(medical_history_date))
def after_admission_date(codelist, where=True):
    return (
        after_admissions.where(where)
        .where(after_admissions.all_diagnoses.is_in(codelist))
        .sort_by(after_admissions.admission_date)
        .first_for_patient()
        .admission_date
    )
# Count all admissions for diagnosis (from codelist) occurring ON or AFTER MEDICAL HISTORY DATE (from codelist / hospital_admissions)
def after_admissions_count(codelist, where=True):
    return (
        after_admissions.where(where)
        .where(after_admissions.all_diagnoses.is_in(codelist))
        .count_for_patient()
    )

# DEMOGRAPHIC variables / COVARIATES ------------------------------------------------------

## age
dataset.cov_num_age = patients.age_on(index_date)

## sex
dataset.sex = patients.sex # only include f, m and no missing values?
#dataset.sex = patients.sex.is_in(["female", "male"]) # only include f, m and no missing values. Depends on population variable definition above

## ethnicity in 6 categories based on codelists/opensafely-ethnicity.csv only. https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py  
dataset.cov_cat_ethnicity = (
    clinical_events.where(clinical_events.ctv3_code.is_in(ethnicity_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codes)
)
### ethnicity_from_sus? primis_covid19_vacc_update_ethnicity? see post-covid-diabetes repo

## Deprivation
imd_rounded = addresses.for_patient_on(index_date).imd_rounded
dataset.cov_cat_deprivation = case(
    when((imd_rounded >=0) & (imd_rounded < int(32844 * 1 / 10))).then("1 (most deprived)"), # double-check
    when(imd_rounded < int(32844 * 2 / 10)).then("2"),
    when(imd_rounded < int(32844 * 3 / 10)).then("3"),
    when(imd_rounded < int(32844 * 4 / 10)).then("4"),
    when(imd_rounded < int(32844 * 5 / 10)).then("5"),
    when(imd_rounded < int(32844 * 6 / 10)).then("6"),
    when(imd_rounded < int(32844 * 7 / 10)).then("7"),
    when(imd_rounded < int(32844 * 8 / 10)).then("8"),
    when(imd_rounded < int(32844 * 9 / 10)).then("9"),
    when(imd_rounded < int(32844 * 10 / 10)).then("10 (least deprived)"),
    default="unknown"
)

## Region
cov_cat_region = (
    practice_registrations.for_patient_on(index_date)
    .practice_nuts1_region_name
)

## Smoking status
# cov_cat_smoking_status 
tmp_most_recent_smoking_code = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(smoking_clear))
        .where(clinical_events.date.is_on_or_before(index_date))
        .sort_by(clinical_events.date)
        .last_for_patient()
        .ctv3_code
)
most_recent_smoking_code = tmp_most_recent_smoking_code.to_category(smoking_clear)
tmp_ever_smoked = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(smoking_clear))
        .where(clinical_events.date.is_on_or_before(index_date))
        .ctv3_code
)
tmp_ever_smoked_cat = tmp_ever_smoked.to_category(smoking_clear)
ever_smoked = case(
    when(tmp_ever_smoked_cat == "S").then("S"),
    when(tmp_ever_smoked_cat == "E").then("E"),
)
"""
cov_cat_smoking_status = case(
    when(most_recent_smoking_code == "S").then("S"),
    when(most_recent_smoking_code == "E").then("E"),
    when(most_recent_smoking_code == "N" & ever_smoked == "S").then("E"),
    when(most_recent_smoking_code == "N" & (ever_smoked != "S" & ever_smoked != "E")).then("N"),
)
"""

## Care home status
# Flag care home based on primis (patients in long-stay nursing and residential care)
care_home_code = has_prior_event_snomed(carehome)
#dataset.care_home_code = care_home_code
# Flag care home based on TPP
care_home_tpp = addresses.for_patient_on(index_date).care_home_is_potential_match 
#dataset.care_home_tpp = care_home_tpp
dataset.cov_bin_carehome_status = case(
    when(care_home_code).then(True),
    when(care_home_tpp).then(True),
    default=False
)

# Vaccination history?
covid_vaccinations = (
  vaccinations
  .where(vaccinations.target_disease.is_in(["SARS-2 CORONAVIRUS"]))
  .sort_by(vaccinations.date)
)



# EXPOSURE variables ------------------------------------------------------

### 1) Who are the people who take Metfin (index_date onwards) in terms of underlying disease?
#### T2DM, pre(T2)DM, PCO, HbA1c high (date of first ever recording): Primary care & HES APC
### 2) COVID-19 infection
#### "Generate variable to identify first date of confirmed COVID"
### How many were started on Metfin at/shortly after COVID-19 infection?


#### DIABETES ---------
# BASED on https://github.com/opensafely/post-covid-diabetes/blob/main/analysis/common_variables.py 

### Type 1 Diabetes
## Date of first ever recording
# Primary care
tmp_exp_date_t1dm_ctv3 = prior_event_date_ctv3(diabetes_type1_ctv3_clinical)
# HES APC
tmp_exp_date_t1dm_hes = prior_admission_date(diabetes_type1_icd10)
# Combined
exp_date_t1dm = minimum_of(tmp_exp_date_t1dm_ctv3, tmp_exp_date_t1dm_hes)

## Count of number of records
# Primary care
tmp_exp_count_t1dm_snomed = prior_events_count_ctv3(diabetes_type1_ctv3_clinical) # change name to ctv3
# HES APC
tmp_exp_count_t1dm_hes = prior_admissions_count(diabetes_type1_icd10)

### Type 2 Diabetes
## Date of first ever recording
# Primary care
tmp_exp_date_t2dm_ctv3 = prior_event_date_ctv3(diabetes_type2_ctv3_clinical)
# HES APC
tmp_exp_date_t2dm_hes = prior_admission_date(diabetes_type2_icd10)
# Combined
exp_date_t2dm = minimum_of(tmp_exp_date_t2dm_ctv3, tmp_exp_date_t2dm_hes)

## Count of number of records
# Primary care
tmp_exp_count_t2dm_snomed = prior_events_count_ctv3(diabetes_type2_ctv3_clinical) # change name to ctv3
# HES APC
tmp_exp_count_t2dm_hes = prior_admissions_count(diabetes_type2_icd10)

### Diabetes unspecified
## Date of first ever recording
# Primary care
exp_date_otherdm = prior_event_date_ctv3(diabetes_other_ctv3_clinical)

## Count of number of records
# Primary care
tmp_exp_count_otherdm = prior_events_count_ctv3(diabetes_other_ctv3_clinical) # change name to ctv3

### Gestational diabetes
## Date of first ever recording
# Primary care
exp_date_gestationaldm = prior_event_date_ctv3(diabetes_gestational_ctv3_clinical)

### Diabetes diagnostic codes
## Date of first ever recording
# Primary care
exp_date_poccdm = prior_event_date_ctv3(diabetes_diagnostic_ctv3_clinical)

## Count of number of records
# Primary care
tmp_exp_count_poccdm_snomed = prior_events_count_ctv3(diabetes_diagnostic_ctv3_clinical) # change name to ctv3

### Other variables needed to define diabetes
## Maximum HbA1c measure
tmp_exp_num_max_hba1c_mmol_mol = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_after(medical_history_date))
        .numeric_value.maximum_for_patient()
)
## Date of maximum latest HbA1c measure
tmp_exp_num_max_hba1c_date = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.numeric_value == tmp_exp_num_max_hba1c_mmol_mol)
        .sort_by(clinical_events.date)
        .last_for_patient() # translates in cohortextractor to "on_most_recent_day_of_measurement=True"
        .date
)
##  Diabetes drugs
tmp_exp_date_insulin_snomed = (
    medications.where(
        medications.dmd_code.is_in(insulin_snomed_clinical)) # medications. only has dmd_code, no snomed. The codes look the same to me; dmd = snomed?
        .where(medications.date.is_on_or_after(medical_history_date))
        .sort_by(medications.date)
        .first_for_patient() # translates in cohortextractor to "find_first_match_in_period=True"
        .date
)
tmp_exp_date_antidiabetic_drugs_snomed = (
    medications.where(
        medications.dmd_code.is_in(antidiabetic_drugs_snomed_clinical))
        .where(medications.date.is_on_or_after(medical_history_date))
        .sort_by(medications.date)
        .first_for_patient()
        .date
)   
tmp_exp_date_nonmetform_drugs_snomed = ( ## why is this needed; tmp_exp_date_antidiabetic_drugs_snomed not sufficient?
    medications.where(
        medications.dmd_code.is_in(non_metformin_dmd))
        .where(medications.date.is_on_or_after(medical_history_date))
        .sort_by(medications.date)
        .first_for_patient()
        .date
)      

## Generate variable to identify earliest date any diabetes medication prescribed
tmp_exp_date_diabetes_medication = minimum_of(
    tmp_exp_date_insulin_snomed, 
    tmp_exp_date_antidiabetic_drugs_snomed) # why excluding tmp_exp_date_nonmetform_drugs_snomed? Is tmp_exp_date_diabetes_medication even needed?

## Generate variable to identify earliest date any diabetes diagnosis codes recorded
dataset.tmp_exp_date_first_diabetes_diag = minimum_of(
         exp_date_gestationaldm,
         exp_date_otherdm,
         exp_date_t1dm, 
         exp_date_t2dm, 
         exp_date_poccdm,
         tmp_exp_date_diabetes_medication,
         tmp_exp_date_nonmetform_drugs_snomed
)

#### METFORMIN ---------

# https://www.opencodelists.org/codelist/user/john-tazare/metformin-dmd/48e43356/
dataset.first_metfin_date = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes))
        .where(medications.date.is_on_or_after(index_date))
        .sort_by(medications.date)
        .first_for_patient()
        .date
)
dataset.num_metfin_prescriptions_within_1y = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes))
        .where(medications.date.is_on_or_between(index_date, "2023-01-01"))
        .count_for_patient()
)

#### SARS-CoV-2 ---------

# All COVID-19 events in primary care
primary_care_covid_events = clinical_events.where(
    clinical_events.ctv3_code.is_in(
        covid_primary_care_code
        + covid_primary_care_positive_test
        + covid_primary_care_sequelae
    )
)
# First COVID-19 code (diagnosis, positive test or sequalae) in primary care after index date
tmp_exp_date_covid19_confirmed_snomed = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_after(index_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)

# Date of first positive SARS-COV-2 PCR antigen test after index date. Around/before index date?
tmp_exp_date_covid19_confirmed_sgss = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_after(index_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)

# First covid-19 related hospital admission after index date
tmp_exp_date_covid19_confirmed_hes = (
    hospital_admissions.where(hospital_admissions.all_diagnoses.is_in(covid_codes)) # https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
    .where(hospital_admissions.admission_date.is_on_or_after(index_date))
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)

"""
import operator
from functools import reduce
def any_of(conditions):
    return reduce(operator.or_, conditions)

# query if emergency attentance diagnosis codes match a given codelist
def emergency_diagnosis_matches(codelist):
    conditions = [
        getattr(emergency, column_name).is_in(codelist)
        for column_name in [f"diagnosis_{i:02d}" for i in range(1, 25)]
    ]
    return emergency.where(any_of(conditions))

# Emergency attendance for covid after index date?
dataset.covidemergency_0_date = (
    emergency_diagnosis_matches(covid_emergency)
    .where(emergency.arrival_date.is_on_or_after(index_date))
    .sort_by(emergency.arrival_date)
    .last_for_patient()
    .arrival_date
)
"""

# all-cause death ## death table: I need to search in all cause of death or only underlying_cause_of_death ?
dataset.death_date = ons_deaths.date

"""
# covid-related death (stated anywhere on any of the 15 death certificate options)
def cause_of_death_matches(codelist):
    conditions = [
        getattr(ons_deaths, column_name).is_in(codelist)
        for column_name in (["underlying_cause_of_death"]+[f"cause_of_death_{i:02d}" for i in range(1, 16)])
    ]
    return any_of(conditions)
dataset.tmp_exp_date_covid19_confirmed_death = cause_of_death_matches(covid_codes) # https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
"""


# OUTCOME variables ------------------------------------------------------


## long/post covid: https://github.com/opensafely/long-covid/blob/main/analysis/codelists.py
dataset.long_covid = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(long_covid_diagnostic_codes))
        .where(clinical_events.date.is_on_or_after(index_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .exists_for_patient()
        |
    clinical_events.where(
        clinical_events.snomedct_code.is_in(long_covid_referral_codes))
        .where(clinical_events.date.is_on_or_after(index_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .exists_for_patient()  
        |
    clinical_events.where(
        clinical_events.snomedct_code.is_in(long_covid_assessment_codes))
        .where(clinical_events.date.is_on_or_after(index_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .exists_for_patient() 
)
dataset.post_viral_fatigue = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(post_viral_fatigue_codes))
        .where(clinical_events.date.is_on_or_after(index_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)


