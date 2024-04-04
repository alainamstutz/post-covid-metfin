#######################################################################################
# IMPORT
#######################################################################################
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
    appointments,
    occupation_on_covid_vaccine_record,
    ons_deaths,
    sgss_covid_all_tests,
    ethnicity_from_sus,
    vaccinations, 
    emergency_care_attendances
)

from databuilder.codes import CTV3Code, ICD10Code # for BMI variable (among others)
# from ehrql.codes import CTV3Code, ICD10Code # see line above
from ehrql.tables.beta import tpp as schema # for BMI variable (among others) .beta or without .beta?
# from ehrql.tables import tpp as schema # see line above

import operator
from functools import reduce # for function building, e.g. any_of

## from codelists.py
from codelists import *

## datetime function
from datetime import date ## needed?

## to import json files (for the dates)
import json
from pathlib import Path

## helper function
import study_definition_helper_functions as helpers

import numpy as np # random seed
np.random.seed(1928374) # random seed


#######################################################################################
# DEFINE the dates: Import study dates defined in "study-dates.R" and exported to JSON
#######################################################################################
study_dates = json.loads(
    Path("analysis/design/study-dates.json").read_text(),
)
# Change these in ./lib/design/study-dates.R if necessary
studystart_date = study_dates["studystart_date"]
studyend_date = study_dates["studyend_date"]
#followupend_date = study_dates["followupend_date"]
#vaccine_peak_date = study_dates["vaccine_peak_date"]
# add sub-cohort dates, to replace studystart_date & studyend_date for each sub-cohort?

#######################################################################################
# DEFINE the baseline date based on SARS-CoV-2 infection
#######################################################################################
## All COVID-19 events in primary care
primary_care_covid_events = clinical_events.where(
    clinical_events.ctv3_code.is_in(
        covid_primary_care_code
        + covid_primary_care_positive_test
        + covid_primary_care_sequelae
    )
)
## First COVID-19 code (diagnosis, positive test or sequelae) in primary care in recruitment period
tmp_covid19_primary_care_date = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_between(studystart_date,studyend_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
tmp_covid19_primary_care_events = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_between(studystart_date,studyend_date))
    .exists_for_patient()
)

## First positive SARS-COV-2 PCR in recruitment period
tmp_covid19_sgss_date = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_between(studystart_date,studyend_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)

tmp_covid19_sgss_events = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_between(studystart_date,studyend_date))
        .exists_for_patient()
)

"""
## First covid-19 related hospital admission in recruitment period // include or exclude since we are only (?) interested in recruitment in primary care -> only include as outcome
tmp_covid19_hes_date = (
    hospital_admissions.where(hospital_admissions.all_diagnoses.is_in(covid_codes)) # double-check with https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
    .where(hospital_admissions.admission_date.is_on_or_between(studystart_date,studyend_date))
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)
"""

### Define (first) baseline date within recruitment period
baseline_date = minimum_of(tmp_covid19_primary_care_date, tmp_covid19_sgss_date)

#######################################################################################
# FUNCTIONS (all based on baseline_date)
#######################################################################################
### PRIMARY CARE
## EVER BEFORE BASELINE DATE (any history of)
# Any event (clinical_events table)
prior_events = clinical_events.where(clinical_events.date.is_on_or_before(baseline_date))
def has_prior_event_snomed(codelist, where=True): # snomed codelist
    return (
        prior_events.where(where)
        .where(prior_events.snomedct_code.is_in(codelist))
        .exists_for_patient()
    )
def has_prior_event_ctv3(codelist, where=True): # ctv3 codelist
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .exists_for_patient()
    )
# Most recent event date
def prior_event_date_snomed(codelist, where=True): # snomed codelist
    return (
        prior_events.where(where)
        .where(prior_events.snomedct_code.is_in(codelist))
        .sort_by(prior_events.date)
        .last_for_patient()
        .date
    )
def prior_event_date_ctv3(codelist, where=True): # ctv3 codelist
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .sort_by(prior_events.date)
        .last_for_patient()
        .date
    )
# Count all prior events
def prior_events_count_ctv3(codelist, where=True): # ctv3 codelist
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .count_for_patient()
    )
# Any medication prescription (medications table) 
prior_prescription = medications.where(medications.date.is_on_or_before(baseline_date))
def has_prior_prescription(codelist, where=True): # always DMD codes
    return (
        prior_prescription.where(where)
        .where(prior_prescription.dmd_code.is_in(codelist))
        .exists_for_patient()
    )
# Most recent prescription date
def has_prior_prescription_date(codelist, where=True):
    return (
        prior_prescription.where(where)
        .where(prior_prescription.dmd_code.is_in(codelist))
        .sort_by(prior_prescription.date)
        .last_for_patient()
        .date
    )

## 2y BEFORE BASELINE DATE 
# Most recent value (clinical_events table)
recent_value_2y = clinical_events.where(clinical_events.date.is_on_or_between(baseline_date - days(2*366), baseline_date)) # Calculated from 1 year = 365.25 days, taking into account leap years. 
def recent_value_2y_snomed(codelist, where=True): # snomed codelist
    return (
        recent_value_2y.where(where)
        .where(recent_value_2y.snomedct_code.is_in(codelist))
        .numeric_value.maximum_for_patient()
    )
def recent_value_2y_ctv3(codelist, where=True): # ctv3 codelist
    return (
        recent_value_2y.where(where)
        .where(recent_value_2y.ctv3_code.is_in(codelist))
        .numeric_value.maximum_for_patient()
    )

## 6M BEFORE BASELINE DATE (only for prescription data)
# Any medication prescription (medications table) 
prior_prescription_6m = medications.where(medications.date.is_on_or_between(baseline_date - days(183), baseline_date)) # Calculated from 1 year = 365.25 days, taking into account leap years. 
def has_prior_prescription_6m(codelist, where=True): # always DMD codes
    return (
        prior_prescription_6m.where(where)
        .where(prior_prescription_6m.dmd_code.is_in(codelist))
        .exists_for_patient()
    )
# Most recent prescription date
def has_prior_prescription_6m_date(codelist, where=True):
    return (
        prior_prescription_6m.where(where)
        .where(prior_prescription_6m.dmd_code.is_in(codelist))
        .sort_by(prior_prescription_6m.date)
        .last_for_patient()
        .date
    )
## 14 Days BEFORE BASELINE DATE (only for prescription data)
# Any medication prescription (medications table) 
prior_prescription_14d = medications.where(medications.date.is_on_or_between(baseline_date - days(14), baseline_date))
def has_prior_prescription_14d(codelist, where=True): # always DMD codes
    return (
        prior_prescription_14d.where(where)
        .where(prior_prescription_14d.dmd_code.is_in(codelist))
        .exists_for_patient()
    )
# Most recent prescription date
def has_prior_prescription_14d_date(codelist, where=True):
    return (
        prior_prescription_14d.where(where)
        .where(prior_prescription_14d.dmd_code.is_in(codelist))
        .sort_by(prior_prescription_14d.date)
        .last_for_patient()
        .date
    )

### HOSPITAL ADMISSIONS (HES APC)
## EVER BEFORE BASELINE DATE (any history of)
# Any event (hospital_admissions table)
prior_admissions = hospital_admissions.where(hospital_admissions.admission_date.is_on_or_before(baseline_date))
def has_prior_admission(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .exists_for_patient()
    )
# Most recent event (date)
def prior_admission_date(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .sort_by(prior_admissions.admission_date)
        .last_for_patient()
        .admission_date
    )
# Count all prior admissions
def prior_admissions_count(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .count_for_patient()
    )

### OTHER functions, based on https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/dataset_definition.py
# helper function
def any_of(conditions):
    return reduce(operator.or_, conditions)

# for BMI calculation, 
def most_recent_bmi(*, minimum_age_at_measurement, where=True):
    clinical_events = schema.clinical_events
    age_threshold = schema.patients.date_of_birth + days(
        # This is obviously inexact but, given that the dates of birth are rounded to
        # the first of the month anyway, there's no point trying to be more accurate
        int(365.25 * minimum_age_at_measurement)
    )
    return (
        # This captures just explicitly recorded BMI observations rather than attempting
        # to calculate it from height and weight measurements. Investigation has shown
        # this to have no real benefit it terms of coverage or accuracy.
        clinical_events.where(clinical_events.ctv3_code == CTV3Code("22K.."))
        .where(clinical_events.date >= age_threshold)
        .where(where)
        .sort_by(clinical_events.date)
        .last_for_patient()
    )

# query if emergency attentance diagnosis codes match a given codelist
def emergency_diagnosis_matches(codelist):
    conditions = [
        getattr(emergency_care_attendances, column_name).is_in(codelist)
        for column_name in [f"diagnosis_{i:02d}" for i in range(1, 25)]
    ]
    return emergency_care_attendances.where(any_of(conditions))

# query if causes of death match a given codelist
def cause_of_death_matches(codelist):
    conditions = [
        getattr(ons_deaths, column_name).is_in(codelist)
        for column_name in (["underlying_cause_of_death"]+[f"cause_of_death_{i:02d}" for i in range(1, 16)])
    ]
    return any_of(conditions)


#######################################################################################
# INITIALISE the dataset, define the baseline date and event, and set the dummy dataset size
#######################################################################################
dataset = create_dataset()
dataset.configure_dummy_data(population_size=5000)
dataset.baseline_date = baseline_date
dataset.define_population(patients.exists_for_patient())

dataset.cov_bin_pos_covid = tmp_covid19_primary_care_events | tmp_covid19_sgss_events

#######################################################################################
# QUALITY ASSURANCES and completeness criteria
#######################################################################################

# population variables for dataset definition 
dataset.qa_bin_is_female_or_male = patients.sex.is_in(["female", "male"]) 
dataset.qa_bin_was_adult = (patients.age_on(baseline_date) >= 18) & (patients.age_on(baseline_date) <= 110) 
dataset.qa_bin_was_alive = (patients.date_of_death.is_after(baseline_date) | patients.date_of_death.is_null()) 
dataset.qa_bin_known_imd = addresses.for_patient_on(baseline_date).exists_for_patient() # known deprivation
dataset.qa_bin_was_registered = practice_registrations.spanning(baseline_date - days(366), baseline_date).exists_for_patient() # only include if registered on baseline date spanning back 1 year. Calculated from 1 year = 365.25 days, taking into account leap years.
# double-check line above against code from Will, line 98: https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/dataset_definition.py 

"""
# define/create dataset // eventually take out to show entire flow chart
dataset.define_population(
    is_female_or_male
    & was_adult
    & was_alive
    & was_registered
) 
"""

## Year of birth
dataset.qa_num_birth_year = patients.date_of_birth

## Date of death
dataset.qa_date_of_death = ons_deaths.date

## Pregnancy (over entire study period, not based on baseline date -> no function for this)
dataset.qa_bin_pregnancy = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(pregnancy_snomed_clinical))
        .exists_for_patient()
        )

## Combined oral contraceptive pill
dataset.qa_bin_cocp = has_prior_prescription(cocp_dmd)

## Hormone replacement therapy
dataset.qa_bin_hrt = has_prior_prescription(hrt_dmd)

## Prostate cancer (over entire study period, not based on baseline date -> no function for this)
### Primary care
prostate_cancer_snomed = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(prostate_cancer_snomed_clinical))
        .exists_for_patient()
        )
### HES APC
prostate_cancer_hes = (
    hospital_admissions.where(
        hospital_admissions.all_diagnoses.is_in(prostate_cancer_icd10))
        .exists_for_patient()
        )
### ONS (stated anywhere on death certificate)
prostate_cancer_death = cause_of_death_matches(prostate_cancer_icd10)
# Combined: Any prostate cancer diagnosis
dataset.qa_bin_prostate_cancer = case(
    when(prostate_cancer_snomed).then(True),
    when(prostate_cancer_hes).then(True),
    when(prostate_cancer_death).then(True),
    default=False
)


#######################################################################################
# DEMOGRAPHIC variables
#######################################################################################

## sex
dataset.cov_cat_sex = patients.sex

## age
dataset.cov_num_age = patients.age_on(baseline_date)

### Age on 1 January 2020
#dataset.age_jan2020 = patients.age_on("2020-01-01")

## ethnicity in 6 categories based on codelists/opensafely-ethnicity.csv only. https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py  
dataset.cov_cat_ethnicity = (
    clinical_events.where(clinical_events.ctv3_code.is_in(ethnicity_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codes)
)

## Deprivation
# Index of Multiple Deprevation Rank (rounded down to nearest 100)
imd_rounded = addresses.for_patient_on(baseline_date).imd_rounded
dataset.cov_cat_deprivation_10 = case(
    when((imd_rounded >=0) & (imd_rounded < int(32844 * 1 / 10))).then("1 (most deprived)"),
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

dataset.cov_cat_deprivation_5 = case(
    when((imd_rounded >=0) & (imd_rounded < int(32844 * 1 / 5))).then("1 (most deprived)"),
    when(imd_rounded < int(32844 * 2 / 5)).then("2"),
    when(imd_rounded < int(32844 * 3 / 5)).then("3"),
    when(imd_rounded < int(32844 * 4 / 5)).then("4"),
    when(imd_rounded < int(32844 * 5 / 5)).then("5 (least deprived)"),
    default="unknown"
)

# registration info as at baseline date
registered = practice_registrations.for_patient_on(baseline_date)

## Region
dataset.cov_cat_region = registered.practice_nuts1_region_name

## Rurality
dataset.cov_cat_rural_urban = addresses.for_patient_on(baseline_date).rural_urban_classification

## Practice registration
dataset.cov_cat_stp = registered.practice_stp


#######################################################################################
# ELIGIBILITY variables
#######################################################################################

### DIABETES start ---------
# BASED on https://github.com/opensafely/post-covid-diabetes/blob/main/analysis/common_variables.py 
### Type 1 Diabetes
# Date of latest recording
# Primary care
tmp_cov_date_t1dm_ctv3 = prior_event_date_ctv3(diabetes_type1_ctv3_clinical) # changed name to ctv3
dataset.tmp_cov_date_t1dm_ctv3 = tmp_cov_date_t1dm_ctv3
# HES APC
tmp_cov_date_t1dm_hes = prior_admission_date(diabetes_type1_icd10)
# Combined
cov_date_t1dm = minimum_of(tmp_cov_date_t1dm_ctv3, tmp_cov_date_t1dm_hes)
dataset.cov_date_t1dm = cov_date_t1dm

# Count of number of records
# Primary care
tmp_cov_count_t1dm_ctv3 = prior_events_count_ctv3(diabetes_type1_ctv3_clinical) # changed name to ctv3
dataset.tmp_cov_count_t1dm_ctv3 = tmp_cov_count_t1dm_ctv3
# HES APC
tmp_cov_count_t1dm_hes = prior_admissions_count(diabetes_type1_icd10)
dataset.tmp_cov_count_t1dm_hes = tmp_cov_count_t1dm_hes
# Combined
dataset.tmp_cov_count_t1dm = tmp_cov_count_t1dm_ctv3 + tmp_cov_count_t1dm_hes

### Type 2 Diabetes
# Date of latest recording
# Primary care
tmp_cov_date_t2dm_ctv3 = prior_event_date_ctv3(diabetes_type2_ctv3_clinical) # change name to ctv3
dataset.tmp_cov_date_t2dm_ctv3 = tmp_cov_date_t2dm_ctv3
# HES APC
tmp_cov_date_t2dm_hes = prior_admission_date(diabetes_type2_icd10)
# Combined
cov_date_t2dm = minimum_of(tmp_cov_date_t2dm_ctv3, tmp_cov_date_t2dm_hes)
dataset.cov_date_t2dm = cov_date_t2dm

# Count of number of records
# Primary care
tmp_cov_count_t2dm_ctv3 = prior_events_count_ctv3(diabetes_type2_ctv3_clinical) # change name to ctv3
dataset.tmp_cov_count_t2dm_ctv3 = tmp_cov_count_t2dm_ctv3
# HES APC
tmp_cov_count_t2dm_hes = prior_admissions_count(diabetes_type2_icd10)
dataset.tmp_cov_count_t2dm_hes = tmp_cov_count_t2dm_hes
# Combined
dataset.tmp_cov_count_t2dm = tmp_cov_count_t2dm_ctv3 + tmp_cov_count_t2dm_hes

### Diabetes unspecified/other
# Date of latest recording
# Primary care
cov_date_otherdm = prior_event_date_ctv3(diabetes_other_ctv3_clinical)
dataset.cov_date_otherdm = cov_date_otherdm

# Count of number of records
# Primary care
dataset.tmp_cov_count_otherdm = prior_events_count_ctv3(diabetes_other_ctv3_clinical)

### Gestational diabetes
# Date of latest recording
# Primary care
cov_date_gestationaldm = prior_event_date_ctv3(diabetes_gestational_ctv3_clinical)
dataset.cov_date_gestationaldm = cov_date_gestationaldm

### Diabetes diagnostic codes
# Date of latest recording
# Primary care
tmp_cov_date_poccdm = prior_event_date_ctv3(diabetes_diagnostic_ctv3_clinical)
dataset.tmp_cov_date_poccdm = tmp_cov_date_poccdm

# Count of number of records
# Primary care
dataset.tmp_cov_count_poccdm_ctv3 = prior_events_count_ctv3(diabetes_diagnostic_ctv3_clinical) # changed name to ctv3

### Other variables needed to define diabetes
# Maximum HbA1c measure (in period before baseline_date)
tmp_cov_num_max_hba1c_mmol_mol = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .numeric_value.maximum_for_patient()
)
dataset.tmp_cov_num_max_hba1c_mmol_mol = tmp_cov_num_max_hba1c_mmol_mol

# Date of latest maximum HbA1c measure
dataset.tmp_cov_date_max_hba1c = ( 
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date)) # this line of code probably not needed again
        .where(clinical_events.numeric_value == tmp_cov_num_max_hba1c_mmol_mol)
        .sort_by(clinical_events.date)
        .last_for_patient() # translates in cohortextractor to "on_most_recent_day_of_measurement=True"
        .date
)
#  Diabetes drugs
tmp_cov_date_insulin_snomed = has_prior_prescription_date(insulin_snomed_clinical)
dataset.tmp_cov_date_insulin_snomed = tmp_cov_date_insulin_snomed
tmp_cov_date_antidiabetic_drugs_snomed = has_prior_prescription_date(antidiabetic_drugs_snomed_clinical)
dataset.tmp_cov_date_antidiabetic_drugs_snomed = tmp_cov_date_antidiabetic_drugs_snomed
tmp_cov_date_nonmetform_drugs_snomed = has_prior_prescription_date(non_metformin_dmd) # this extra step makes sense for the diabetes algorithm (otherwise not)
dataset.tmp_cov_date_nonmetform_drugs_snomed = tmp_cov_date_nonmetform_drugs_snomed

# Generate variable to identify latest date (in period before baseline_date) that any diabetes medication was prescribed
tmp_cov_date_diabetes_medication = maximum_of(
    tmp_cov_date_insulin_snomed, 
    tmp_cov_date_antidiabetic_drugs_snomed) # why excluding tmp_cov_date_nonmetform_drugs_snomed? -> this extra step makes sense for the diabetes algorithm (otherwise not)
dataset.tmp_cov_date_diabetes_medication = tmp_cov_date_diabetes_medication

# Generate variable to identify latest date (in period before baseline_date) that any diabetes diagnosis codes were recorded
dataset.tmp_cov_date_latest_diabetes_diag = maximum_of( # changed name to latest
         cov_date_t2dm, 
         cov_date_t1dm,
         cov_date_otherdm,
         cov_date_gestationaldm,
         tmp_cov_date_poccdm,
         tmp_cov_date_diabetes_medication,
         tmp_cov_date_nonmetform_drugs_snomed
)

### DIABETES end ---------


## Prediabetes, on or before baseline
# Date of preDM code in primary care
tmp_cov_date_prediabetes = prior_event_date_snomed(prediabetes_snomed)
# Date of preDM HbA1c measure in period before baseline_date in preDM range (mmol/mol): 42-47.9
tmp_cov_date_predm_hba1c_mmol_mol = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .where((clinical_events.numeric_value>=42) & (clinical_events.numeric_value<=47.9))
        .sort_by(clinical_events.date)
        .last_for_patient()
        .date
)
# Latest date (in period before baseline_date) that any prediabetes was diagnosed or HbA1c in preDM range
dataset.cov_date_prediabetes = maximum_of(
    tmp_cov_date_prediabetes, 
    tmp_cov_date_predm_hba1c_mmol_mol) 

# Any preDM diagnosis in primary care
tmp_cov_bin_prediabetes = has_prior_event_snomed(prediabetes_snomed)
# Any HbA1c preDM in primary care
tmp_cov_bin_predm_hba1c_mmol_mol = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .where((clinical_events.numeric_value>=42) & (clinical_events.numeric_value<=47.9))
        .exists_for_patient()
)
# Any preDM diagnosis or Hb1Ac preDM range value (in period before baseline_date)
dataset.cov_bin_prediabetes = tmp_cov_bin_prediabetes | tmp_cov_bin_predm_hba1c_mmol_mol

## Hospitalization at baseline (incl. 1 day prior)
dataset.cov_bin_hosp_baseline = (
    hospital_admissions.where(hospital_admissions.admission_date.is_on_or_between(baseline_date - days(1), baseline_date))
    .exists_for_patient()
)

## Metformin use at baseline, defined as receiving a metformin prescription up until 6 months prior to baseline date (assuming half-yearly prescription for stable diabetes across GPs in the UK)
dataset.cov_bin_metfin_before_baseline = has_prior_prescription_6m(metformin_codes) # https://www.opencodelists.org/codelist/user/john-tazare/metformin-dmd/48e43356/
dataset.cov_date_metfin_before_baseline = has_prior_prescription_6m_date(metformin_codes)

## Known hypersensitivity / intolerance to metformin, on or before baseline
dataset.cov_bin_metfin_allergy = has_prior_event_snomed(metformin_allergy) 

## Moderate to severe renal impairment (eGFR of <30ml/min/1.73 m2; stage 4/5), on or before baseline
# Primary care
tmp_cov_bin_ckd45_snomed = has_prior_event_snomed(ckd_snomed_clinical_45) 
# HES APC
tmp_cov_bin_ckd4_hes = has_prior_admission(ckd_stage4_icd10)
tmp_cov_bin_ckd5_hes = has_prior_admission(ckd_stage5_icd10)
# Combined
dataset.cov_bin_ckd_45 = tmp_cov_bin_ckd45_snomed | tmp_cov_bin_ckd4_hes | tmp_cov_bin_ckd5_hes
# include kidney transplant? / dialysis? / eGFR? // https://github.com/opensafely/Paxlovid-and-sotrovimab/blob/main/analysis/study_definition.py#L595

## Advance decompensated liver cirrhosis, on or before baseline 
# Primary care
tmp_cov_bin_liver_cirrhosis_snomed = has_prior_event_snomed(advanced_decompensated_cirrhosis_snomed_codes)
tmp_cov_bin_ascitis_drainage_snomed = has_prior_event_snomed(ascitic_drainage_snomed_codes) # regular ascitic drainage
# HES APC
tmp_cov_bin_liver_cirrhosis_hes = has_prior_admission(advanced_decompensated_cirrhosis_icd10_codes)
# Combined
dataset.cov_bin_liver_cirrhosis = tmp_cov_bin_liver_cirrhosis_snomed | tmp_cov_bin_ascitis_drainage_snomed | tmp_cov_bin_liver_cirrhosis_hes

## Use of the following medications in the last 14 days (drug-drug interaction with metformin)
dataset.cov_bin_metfin_interaction = has_prior_prescription_14d(metformin_interaction_codes) 
dataset.cov_date_metfin_interaction = has_prior_prescription_14d_date(metformin_interaction_codes)

## Prior Long COVID diagnosis, based on https://github.com/opensafely/long-covid/blob/main/analysis/codelists.py
## All Long COVID-19 events in primary care
primary_care_long_covid = clinical_events.where(
    clinical_events.snomedct_code.is_in(
        long_covid_diagnostic_codes
        + long_covid_referral_codes
        + long_covid_assessment_codes
    )
)
# Any Long COVID code in primary care on or before baseline date
dataset.cov_bin_long_covid = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_before(baseline_date))
    .exists_for_patient()
)
dataset.cov_date_long_covid = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_before(baseline_date))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .date
)



#######################################################################################
# Potential CONFOUNDER variables / Covariates for IPTW and IPCW
#######################################################################################

## Smoking status at baseline
tmp_most_recent_smoking_code = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(smoking_clear))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .sort_by(clinical_events.date)
        .last_for_patient()
        .ctv3_code
)
tmp_most_recent_smoking_cat = tmp_most_recent_smoking_code.to_category(smoking_clear)
#dataset.tmp_most_recent_smoking_cat = tmp_most_recent_smoking_cat

ever_smoked = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(ever_smoking)) ### used a different codelist with ONLY smoking codes
        .where(clinical_events.date.is_on_or_before(baseline_date)) 
        .exists_for_patient()
)
#dataset.ever_smoked = ever_smoked

cov_cat_smoking_status = case(
    when(tmp_most_recent_smoking_cat == "S").then("S"),
    when(tmp_most_recent_smoking_cat == "E").then("E"),
    when((tmp_most_recent_smoking_cat == "N") & (ever_smoked == True)).then("E"),
    when(tmp_most_recent_smoking_cat == "N").then("N"),
    when((tmp_most_recent_smoking_cat == "M") & (ever_smoked == True)).then("E"),
    when(tmp_most_recent_smoking_cat == "M").then("M"),
    default = "M"
)
dataset.cov_cat_smoking_status = cov_cat_smoking_status

## Care home resident at baseline
# Flag care home based on primis (patients in long-stay nursing and residential care)
care_home_code = has_prior_event_snomed(carehome)
#dataset.care_home_code = care_home_code
# Flag care home based on TPP
care_home_tpp = addresses.for_patient_on(baseline_date).care_home_is_potential_match 
#dataset.care_home_tpp = care_home_tpp
dataset.cov_bin_carehome_status = case(
    when(care_home_code).then(True),
    when(care_home_tpp).then(True),
    default=False
)


## Obesity, on or before baseline
# Primary care
tmp_cov_bin_obesity_snomed = has_prior_event_snomed(bmi_obesity_snomed_clinical)
# HES APC
tmp_cov_bin_obesity_hes = has_prior_admission(bmi_obesity_icd10)
# Combined
dataset.cov_bin_obesity = tmp_cov_bin_obesity_snomed | tmp_cov_bin_obesity_hes

## Acute myocardial infarction, on or before baseline
# Primary care
tmp_cov_bin_ami_snomed = has_prior_event_snomed(ami_snomed_clinical)
# HES APC
tmp_cov_bin_ami_prior_hes = has_prior_admission(ami_prior_icd10)
tmp_cov_bin_ami_hes = has_prior_admission(ami_icd10)
# Combined
dataset.cov_bin_ami = tmp_cov_bin_ami_snomed | tmp_cov_bin_ami_prior_hes | tmp_cov_bin_ami_hes

## All stroke, on or before baseline
# Primary care
tmp_cov_bin_stroke_isch_snomed = has_prior_event_snomed(stroke_isch_snomed_clinical)
tmp_cov_bin_stroke_sah_hs_snomed = has_prior_event_snomed(stroke_sah_hs_snomed_clinical)
# HES APC
tmp_cov_bin_stroke_isch_hes = has_prior_admission(stroke_isch_icd10)
tmp_cov_bin_stroke_sah_hs_hes = has_prior_admission(stroke_sah_hs_icd10)
# Combined
dataset.cov_bin_all_stroke = tmp_cov_bin_stroke_isch_snomed | tmp_cov_bin_stroke_sah_hs_snomed | tmp_cov_bin_stroke_isch_hes | tmp_cov_bin_stroke_sah_hs_hes

## Other arterial embolism, on or before baseline
# Primary care
tmp_cov_bin_other_arterial_embolism_snomed = has_prior_event_snomed(other_arterial_embolism_snomed_clinical)
# HES APC
tmp_cov_bin_other_arterial_embolism_hes = has_prior_admission(ami_icd10)
# Combined
dataset.cov_bin_other_arterial_embolism = tmp_cov_bin_other_arterial_embolism_snomed | tmp_cov_bin_other_arterial_embolism_hes

## Venous thrombolism events, on or before baseline
# Primary care
# combine all VTE codelists
all_vte_codes_snomed_clinical = clinical_events.where(
    clinical_events.snomedct_code.is_in(
        portal_vein_thrombosis_snomed_clinical
        + dvt_dvt_snomed_clinical
        + dvt_icvt_snomed_clinical
        + dvt_pregnancy_snomed_clinical
        + other_dvt_snomed_clinical
        + pe_snomed_clinical
    )
)
tmp_cov_bin_vte_snomed = (
    all_vte_codes_snomed_clinical.where(clinical_events.date.is_on_or_before(baseline_date))
    #.sort_by(clinical_events.date) # this line of code needed?
    .exists_for_patient()
)
# HES APC
all_vte_codes_icd10 = hospital_admissions.where(
    hospital_admissions.all_diagnoses.is_in(
        portal_vein_thrombosis_icd10
        + dvt_dvt_icd10
        + dvt_icvt_icd10
        + dvt_pregnancy_icd10
        + other_dvt_icd10
        + icvt_pregnancy_icd10
        + pe_icd10
    )
)
tmp_cov_bin_vte_hes = (
    all_vte_codes_icd10.where(hospital_admissions.admission_date.is_on_or_before(baseline_date))
    #.sort_by(hospital_admissions.admission_date)
    .exists_for_patient()
)
# Combined
dataset.cov_bin_vte = tmp_cov_bin_vte_snomed | tmp_cov_bin_vte_hes

## Heart failure, on or before baseline
# Primary care
tmp_cov_bin_hf_snomed = has_prior_event_snomed(hf_snomed_clinical)
# HES APC
tmp_cov_bin_hf_hes = has_prior_admission(hf_icd10)
# Combined
dataset.cov_bin_hf = tmp_cov_bin_hf_snomed | tmp_cov_bin_hf_hes

## Angina, on or before baseline
# Primary care
tmp_cov_bin_angina_snomed = has_prior_event_snomed(angina_snomed_clinical)
# HES APC
tmp_cov_bin_angina_hes = has_prior_admission(angina_icd10)
# Combined
dataset.cov_bin_angina = tmp_cov_bin_angina_snomed | tmp_cov_bin_angina_hes

## Dementia, on or before baseline
# Primary care
tmp_cov_bin_dementia_snomed = has_prior_event_snomed(dementia_snomed_clinical)
tmp_cov_bin_dementia_vascular_snomed = has_prior_event_snomed(dementia_vascular_snomed_clinical)
# HES APC
tmp_cov_bin_dementia_hes = has_prior_admission(dementia_icd10)
tmp_cov_bin_dementia_vascular_hes = has_prior_admission(dementia_vascular_icd10)
# Combined
dataset.cov_bin_dementia = tmp_cov_bin_dementia_snomed | tmp_cov_bin_dementia_vascular_snomed | tmp_cov_bin_dementia_hes | tmp_cov_bin_dementia_vascular_hes

## Cancer, on or before baseline
# Primary care
tmp_cov_bin_cancer_snomed = has_prior_event_snomed(cancer_snomed_clinical)
# HES APC
tmp_cov_bin_cancer_hes = has_prior_admission(cancer_icd10)
# Combined
dataset.cov_bin_cancer = tmp_cov_bin_cancer_snomed | tmp_cov_bin_cancer_hes

## Hypertension, on or before baseline
# Primary care
tmp_cov_bin_hypertension_snomed = has_prior_event_snomed(hypertension_snomed_clinical)
# HES APC
tmp_cov_bin_hypertension_hes = has_prior_admission(hypertension_icd10)
# DMD
tmp_cov_bin_hypertension_drugs_dmd = (
    medications.where(
        medications.dmd_code.is_in(hypertension_drugs_dmd)) 
        .where(medications.date.is_on_or_before(baseline_date))
        .exists_for_patient()
)
# Combined
dataset.cov_bin_hypertension = tmp_cov_bin_hypertension_snomed | tmp_cov_bin_hypertension_hes | tmp_cov_bin_hypertension_drugs_dmd

## Depression, on or before baseline
# Primary care
tmp_cov_bin_depression_snomed = has_prior_event_snomed(depression_snomed_clinical)
# HES APC
tmp_cov_bin_depression_icd10 = has_prior_admission(depression_icd10)
# Combined
dataset.cov_bin_depression = tmp_cov_bin_depression_snomed | tmp_cov_bin_depression_icd10

## Chronic obstructive pulmonary disease, on or before baseline
# Primary care
tmp_cov_bin_chronic_obstructive_pulmonary_disease_snomed = has_prior_event_snomed(copd_snomed_clinical)
# HES APC
tmp_cov_bin_chronic_obstructive_pulmonary_disease_hes = has_prior_admission(copd_icd10)
# Combined
dataset.cov_bin_copd = tmp_cov_bin_chronic_obstructive_pulmonary_disease_snomed | tmp_cov_bin_chronic_obstructive_pulmonary_disease_hes

## Liver disease, on or before baseline
# Primary care
tmp_cov_bin_liver_disease_snomed = has_prior_event_snomed(liver_disease_snomed_clinical)
# HES APC
tmp_cov_bin_liver_disease_hes = has_prior_admission(liver_disease_icd10)
# Combined
dataset.cov_bin_liver_disease = tmp_cov_bin_liver_disease_snomed | tmp_cov_bin_liver_disease_hes

## Chronic kidney disease, on or before baseline 
# Primary care
tmp_cov_bin_chronic_kidney_disease_snomed = has_prior_event_snomed(ckd_snomed_clinical) 
# HES APC
tmp_cov_bin_chronic_kidney_disease_hes = has_prior_admission(ckd_icd10)
# Combined
dataset.cov_bin_chronic_kidney_disease = tmp_cov_bin_chronic_kidney_disease_snomed | tmp_cov_bin_chronic_kidney_disease_hes

## Gestational diabetes
# Primary care
tmp_cov_bin_gestationaldm_ctv3 = has_prior_event_ctv3(diabetes_gestational_ctv3_clinical)
# HES APC
tmp_cov_bin_gestationaldm_hes = has_prior_admission(gestationaldm_icd10)
# Combined
dataset.cov_bin_gestationaldm = tmp_cov_bin_gestationaldm_ctv3 | tmp_cov_bin_gestationaldm_hes

## PCOS
# Primary care
tmp_cov_bin_pcos_snomed = has_prior_event_snomed(pcos_snomed_clinical)
# HES APC
tmp_cov_bin_pcos_hes = has_prior_admission(pcos_icd10)
# Combined
dataset.cov_bin_pcos = tmp_cov_bin_pcos_snomed | tmp_cov_bin_pcos_hes

## Type 1 Diabetes
# Primary care
tmp_cov_bin_t1dm_ctv3 = has_prior_event_ctv3(diabetes_type1_ctv3_clinical)
# HES APC
tmp_cov_bin_t1dm_hes = has_prior_admission(diabetes_type1_icd10)
# Combined
dataset.cov_bin_t1dm = tmp_cov_bin_t1dm_ctv3 | tmp_cov_bin_t1dm_hes

## Diabetes complications (foot, retino, neuro, nephro)
# Primary care
tmp_cov_bin_diabetescomp_snomed = has_prior_event_snomed(diabetescomp_snomed_clinical)
# HES APC
tmp_cov_bin_diabetescomp_hes = has_prior_admission(diabetescomp_icd10)
# Combined
dataset.cov_bin_diabetescomp = tmp_cov_bin_diabetescomp_snomed | tmp_cov_bin_diabetescomp_hes

### Any HbA1c measurement
# Primary care
dataset.cov_bin_hba1c_measurement = has_prior_event_snomed(hba1c_measurement_snomed)

### Any OGTT done
# Primary care
dataset.cov_bin_ogtt_measurement = has_prior_event_snomed(ogtt_measurement_snomed)

### Covid-19 vaccination history
dataset.cov_count_covid_vaccines = (
  vaccinations
  .where(vaccinations.target_disease.is_in(["SARS-2 CORONAVIRUS"]))
  .where(vaccinations.date.is_on_or_before(baseline_date))
  .count_for_patient()
)
dataset.cov_date_recent_covid_vaccines = (
  vaccinations
  .where(vaccinations.target_disease.is_in(["SARS-2 CORONAVIRUS"]))
  .where(vaccinations.date.is_on_or_before(baseline_date))
  .sort_by(vaccinations.date)
  .last_for_patient()
  .date
)



## BMI, most recent value, within previous 2 years
bmi_measurement = most_recent_bmi(
    where=clinical_events.date.is_after(baseline_date - days(2 * 366)),
    minimum_age_at_measurement=16,
)
cov_num_bmi = bmi_measurement.numeric_value
dataset.cov_num_bmi = cov_num_bmi
dataset.cov_cat_bmi_groups = case(
    when(cov_num_bmi < 18.5).then("Underweight"),
    when((cov_num_bmi >= 18.5) & (cov_num_bmi < 25.0)).then("Healthy weight (18.5-24.9)"),
    when((cov_num_bmi >= 25.0) & (cov_num_bmi < 30.0)).then("Overweight (25-29.9)"),
    when((cov_num_bmi >= 30.0) & (cov_num_bmi < 70.0)).then("Obese (>30)"), # Set maximum to avoid any impossibly extreme values being classified as obese
    default="missing", 
)

## HbA1c, most recent value, within previous 2 years
dataset.cov_num_hba1c_mmol_mol = recent_value_2y_ctv3(hba1c_new_codes)

## Total Cholesterol, most recent value, within previous 2 years
dataset.tmp_cov_num_cholesterol = recent_value_2y_snomed(cholesterol_snomed)

## HDL Cholesterol, most recent value, within previous 2 years
dataset.tmp_cov_num_hdl_cholesterol = recent_value_2y_snomed(hdl_cholesterol_snomed)



## Number of consultations in year prior to pandemic (2019)
dataset.cov_num_consultation_rate = (
    appointments.where(
        appointments.status.is_in(["Arrived", "In Progress", "Finished", "Visit", "Waiting", "Patient Walked Out",]))
        .where(appointments.seen_date.is_on_or_between(studystart_date - days(366),studystart_date)) # the year before the pandemic
        .count_for_patient()
)

## Healthcare worker at the time they received a COVID-19 vaccination
dataset.cov_bin_healthcare_worker = (
    occupation_on_covid_vaccine_record.where(
        occupation_on_covid_vaccine_record.is_healthcare_worker.is_not_null())
        .exists_for_patient()
)




#######################################################################################
# INTERVENTION/EXPOSURE variables
#######################################################################################

# METFORMIN
dataset.exp_date_first_metfin = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes)) # https://www.opencodelists.org/codelist/user/john-tazare/metformin-dmd/48e43356/
        .where(medications.date.is_on_or_after(baseline_date))
        .sort_by(medications.date)
        .first_for_patient()
        .date
)
dataset.exp_count_metfin = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes))
        .where(medications.date.is_on_or_after(baseline_date))
        .count_for_patient()
)

dataset.exp_bin_7d_metfin = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes))
        .where(medications.date.is_on_or_between(baseline_date, baseline_date + days(7)))
        .exists_for_patient()
)

#######################################################################################
# OUTCOME variables
#######################################################################################
#### SARS-CoV-2, based on https://github.com/opensafely/long-covid/blob/main/analysis/codelists.py

# Code irrespective of outcome window, this will be coded in R script

# Practice deregistration date
dataset.out_date_dereg = registered.end_date

# First covid-19 related hospital admission, after baseline date
out_date_covid19_hes = (
    hospital_admissions.where(hospital_admissions.all_diagnoses.is_in(covid_codes_incl_clin_diag)) # includes the only clinically diagnosed cases: https://www.opencodelists.org/codelist/opensafely/covid-identification/2020-06-03/
    .where(hospital_admissions.admission_date.is_on_or_after(baseline_date))
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)
dataset.out_date_covid19_hes = out_date_covid19_hes

# First emergency attendance for covid, after baseline date
out_date_covid19_emergency = (
    emergency_diagnosis_matches(covid_emergency)
    .where(emergency_care_attendances.arrival_date.is_on_or_after(baseline_date))
    .sort_by(emergency_care_attendances.arrival_date)
    .first_for_patient()
    .arrival_date
)
dataset.out_date_covid19_emergency = out_date_covid19_emergency

## First hospitalisation or emergency attendance for covid, after baseline date
dataset.out_date_covid_hosp = minimum_of(out_date_covid19_hes, out_date_covid19_emergency)

# First COVID-19 code (diagnosis, positive test or sequelae) in primary care, after baseline date
tmp_out_date_covid19_primary_care = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_after(baseline_date)) # details re primary_care_covid_events see # DEFINE the baseline date based on SARS-CoV-2 infection
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
# First positive SARS-COV-2 PCR, after baseline date
tmp_out_date_covid19_sgss = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_after(baseline_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)

## First covid-19 event , after baseline date
dataset.out_date_covid19 = minimum_of(tmp_out_date_covid19_primary_care, tmp_out_date_covid19_sgss, out_date_covid19_hes, out_date_covid19_emergency)

## Long COVID --------- based on https://github.com/opensafely/long-covid/blob/main/analysis/codelists.py
## All Long COVID-19 events in primary care
primary_care_long_covid = clinical_events.where(
    clinical_events.snomedct_code.is_in(
        long_covid_diagnostic_codes
        + long_covid_referral_codes
        + long_covid_assessment_codes
    )
)
# Any Long COVID code in primary care after baseline date
dataset.out_bin_long_covid = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_after(baseline_date))
    .exists_for_patient()
)
# First Long COVID code in primary care after baseline date
dataset.out_date_long_covid_first = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_after(baseline_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
# Any viral fatigue code in primary care after baseline date
dataset.out_bin_viral_fatigue = (
    clinical_events.where(clinical_events.snomedct_code.is_in(post_viral_fatigue_codes))
    .where(clinical_events.date.is_on_or_after(baseline_date))
    .exists_for_patient()
)
# First viral fatigue code in primary care after baseline date
dataset.out_date_viral_fatigue_first = (
    clinical_events.where(clinical_events.snomedct_code.is_in(post_viral_fatigue_codes))
    .where(clinical_events.date.is_on_or_after(baseline_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)

## Death
# all-cause death ## death table: I need to search in all cause of death or only underlying_cause_of_death ?
# dataset.out_death_date = ons_deaths.date # already defined as QA

# covid-related death (stated anywhere on any of the 15 death certificate options) # https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
tmp_out_bin_death_cause_covid = cause_of_death_matches(covid_codes_incl_clin_diag)
# add default F
dataset.out_bin_death_cause_covid = case(
    when(tmp_out_bin_death_cause_covid).then(True),
    default=False
)
