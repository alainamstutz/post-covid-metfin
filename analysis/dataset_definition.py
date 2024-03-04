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
    ons_deaths,
    sgss_covid_all_tests,
    hospital_admissions,
    ethnicity_from_sus,
    vaccinations, 
    emergency_care_attendances
)
from databuilder.codes import CTV3Code, ICD10Code # for BMI variable (among others)
from ehrql.tables.beta import tpp as schema # for BMI variable (among others)

## Study definition helper
import study_definition_helper_functions as helpers

import operator
from functools import reduce
def any_of(conditions):
    return reduce(operator.or_, conditions)

## from codelists.py
from codelists import *

## datetime function
from datetime import date ## needed?


#######################################################################################
# DEFINE the dates
#######################################################################################
studystart_date = "2020-01-01" # start of the pandemic
studyend_date = "2022-04-01" # last study entry dates (end of mass testing?) / to be defined
followupend_date = "2023-04-30" # end of follow-up / to be defined
vaccine_peak_date = "2021-06-18"
medical_history_date = "1990-01-01" # e.g. to define DM diagnosis (for "ever" vs prior 2 years)


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
## First COVID-19 code (diagnosis, positive test or sequelae) in primary care in study period
tmp_covid19_primary_care_date = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_between(studystart_date,studyend_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
## First positive SARS-COV-2 PCR in study period
tmp_covid19_sgss_date = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_between(studystart_date,studyend_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)
## First covid-19 related hospital admission in study period // include or exclude since we are only (?) interested in recruitment in primary care
tmp_covid19_hes_date = (
    hospital_admissions.where(hospital_admissions.all_diagnoses.is_in(covid_codes)) # double-check with https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
    .where(hospital_admissions.admission_date.is_on_or_between(studystart_date,studyend_date))
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)
### Define (first) baseline date
baseline_date = minimum_of(tmp_covid19_primary_care_date, tmp_covid19_sgss_date, tmp_covid19_hes_date)


#######################################################################################
# INITIALISE the dataset and set the dummy dataset size
#######################################################################################
dataset = create_dataset()
dataset.configure_dummy_data(population_size=30)

dataset.baseline_date = baseline_date

# population variables for dataset definition // eventually take out to show entire flow chart
is_female_or_male = patients.sex.is_in(["female", "male"]) # only include f, m and no missing values
was_adult = (patients.age_on(baseline_date) >= 18) & (patients.age_on(baseline_date) <= 110) # only include adults and no missing values
was_alive = (patients.date_of_death.is_after(baseline_date) | patients.date_of_death.is_null()) # only include if alive 
was_registered = practice_registrations.for_patient_on(baseline_date).exists_for_patient() # only include if registered on baseline date

# define/create dataset
dataset.define_population(
    is_female_or_male
    & was_adult
    & was_alive
    & was_registered
) 

#######################################################################################
# FUNCTIONS
#######################################################################################
## PRIMARY CARE
## BEFORE BASELINE DATE
# Any events occurring BEFORE BASELINE DATE (clinical_events table) 
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
# Most recent event occurring BEFORE BASELINE DATE (clinical_events table) 
def prior_event_date_ctv3(codelist, where=True): # ctv3 codelist
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .sort_by(prior_events.date)
        .last_for_patient()
        .date
    )
# Count prior events occurring BEFORE BASELINE DATE (clinical_events table)
def prior_events_count_ctv3(codelist, where=True): # ctv3 codelist
    return (
        prior_events.where(where)
        .where(prior_events.ctv3_code.is_in(codelist))
        .count_for_patient()
    )

### ADMISSIONS
## BEFORE BASELINE DATE
# Most recent admission occurring BEFORE BASELINE DATE (hospital_admissions table)
prior_admissions = hospital_admissions.where(hospital_admissions.admission_date.is_on_or_before(baseline_date))
def has_prior_admission(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .exists_for_patient()
    )
def prior_admission_date(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .sort_by(prior_admissions.admission_date)
        .last_for_patient()
        .admission_date
    )
# Count prior admission occurring BEFORE BASELINE DATE (hospital_admissions table)
def prior_admissions_count(codelist, where=True):
    return (
        prior_admissions.where(where)
        .where(prior_admissions.all_diagnoses.is_in(codelist))
        .count_for_patient()
    )

### OTHER functions
# for BMI calculation, based on https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/dataset_definition.py
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
# DEFINE QUALITY ASSURANCES
#######################################################################################
## Prostate cancer
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

## Pregnancy
dataset.qa_bin_pregnancy = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(pregnancy_snomed_clinical))
        .exists_for_patient()
        )

## Year of birth
dataset.qa_num_birth_year = patients.date_of_birth

## Combined oral contraceptive pill
dataset.qa_bin_combined_oral_contraceptive_pill = (
    medications.where(
        medications.dmd_code.is_in(cocp_dmd))
        .where(medications.date.is_on_or_before(baseline_date))
        .exists_for_patient()
)
## Hormone replacement therapy
dataset.qa_bin_hormone_replacement_therapy = (
    medications.where(
        medications.dmd_code.is_in(hrt_dmd))
        .where(medications.date.is_on_or_before(baseline_date))
        .exists_for_patient()
)

#######################################################################################
# DEMOGRAPHIC variables
#######################################################################################

## sex
dataset.cov_cat_sex = patients.sex.is_in(["female", "male"])

## age
dataset.cov_num_age = patients.age_on(baseline_date)

## ethnicity in 6 categories based on codelists/opensafely-ethnicity.csv only. https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py  
dataset.cov_cat_ethnicity = (
    clinical_events.where(clinical_events.ctv3_code.is_in(ethnicity_codes))
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codes)
)

## Deprivation
imd_rounded = addresses.for_patient_on(baseline_date).imd_rounded
dataset.cov_cat_deprivation = case(
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

## Region
dataset.cov_cat_region = (
    practice_registrations.for_patient_on(baseline_date)
    .practice_nuts1_region_name
)


#######################################################################################
# ELIGIBILITY variables
#######################################################################################

### DIABETES start ---------
# BASED on https://github.com/opensafely/post-covid-diabetes/blob/main/analysis/common_variables.py 
### Type 1 Diabetes
# Date of latest recording
# Primary care
tmp_cov_date_t1dm_ctv3 = prior_event_date_ctv3(diabetes_type1_ctv3_clinical)
# HES APC
tmp_cov_date_t1dm_hes = prior_admission_date(diabetes_type1_icd10)
# Combined
cov_date_t1dm = minimum_of(tmp_cov_date_t1dm_ctv3, tmp_cov_date_t1dm_hes)

# Count of number of records
# Primary care
tmp_cov_count_t1dm_snomed = prior_events_count_ctv3(diabetes_type1_ctv3_clinical) # change name to ctv3
# HES APC
tmp_cov_count_t1dm_hes = prior_admissions_count(diabetes_type1_icd10)

### Type 2 Diabetes
# Date of latest recording
# Primary care
tmp_cov_date_t2dm_ctv3 = prior_event_date_ctv3(diabetes_type2_ctv3_clinical)
# HES APC
tmp_cov_date_t2dm_hes = prior_admission_date(diabetes_type2_icd10)
# Combined
cov_date_t2dm = minimum_of(tmp_cov_date_t2dm_ctv3, tmp_cov_date_t2dm_hes)

# Count of number of records
# Primary care
tmp_cov_count_t2dm_snomed = prior_events_count_ctv3(diabetes_type2_ctv3_clinical) # change name to ctv3
# HES APC
tmp_cov_count_t2dm_hes = prior_admissions_count(diabetes_type2_icd10)

### Diabetes unspecified/other
# Date of latest recording
# Primary care
cov_date_otherdm = prior_event_date_ctv3(diabetes_other_ctv3_clinical)

# Count of number of records
# Primary care
tmp_cov_count_otherdm = prior_events_count_ctv3(diabetes_other_ctv3_clinical) # change name to ctv3

### Gestational diabetes
# Date of latest recording
# Primary care
cov_date_gestationaldm = prior_event_date_ctv3(diabetes_gestational_ctv3_clinical)

### Diabetes diagnostic codes
# Date of latest recording
# Primary care
cov_date_poccdm = prior_event_date_ctv3(diabetes_diagnostic_ctv3_clinical)

# Count of number of records
# Primary care
tmp_cov_count_poccdm_snomed = prior_events_count_ctv3(diabetes_diagnostic_ctv3_clinical) # change name to ctv3

### Other variables needed to define diabetes
# Maximum HbA1c measure (in period before baseline_date)
tmp_cov_num_max_hba1c_mmol_mol = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .numeric_value.maximum_for_patient()
)
# Date of latest maximum HbA1c measure
tmp_cov_num_max_hba1c_date = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(hba1c_new_codes))
        .where(clinical_events.date.is_on_or_before(baseline_date)) # this line of code probably not needed again
        .where(clinical_events.numeric_value == tmp_cov_num_max_hba1c_mmol_mol)
        .sort_by(clinical_events.date)
        .last_for_patient() # translates in cohortextractor to "on_most_recent_day_of_measurement=True"
        .date
)
#  Diabetes drugs
tmp_cov_date_insulin_snomed = (
    medications.where(
        medications.dmd_code.is_in(insulin_snomed_clinical)) # medications. only has dmd_code, no snomed. The codes look the same to me; dmd = snomed?
        .where(medications.date.is_on_or_before(baseline_date))
        .sort_by(medications.date)
        .last_for_patient()
        .date
)
tmp_cov_date_antidiabetic_drugs_snomed = (
    medications.where(
        medications.dmd_code.is_in(antidiabetic_drugs_snomed_clinical))
        .where(medications.date.is_on_or_before(baseline_date))
        .sort_by(medications.date)
        .last_for_patient()
        .date
)   
tmp_cov_date_nonmetform_drugs_snomed = ( ## why is this needed; tmp_cov_date_antidiabetic_drugs_snomed not sufficient?
    medications.where(
        medications.dmd_code.is_in(non_metformin_dmd))
        .where(medications.date.is_on_or_before(baseline_date))
        .sort_by(medications.date)
        .last_for_patient()
        .date
)      

# Generate variable to identify latest date (in period before baseline_date) that any diabetes medication was prescribed
tmp_cov_date_diabetes_medication = maximum_of(
    tmp_cov_date_insulin_snomed, 
    tmp_cov_date_antidiabetic_drugs_snomed) # why excluding tmp_cov_date_nonmetform_drugs_snomed? Is tmp_cov_date_diabetes_medication even needed?

# Generate variable to identify latest date (in period before baseline_date) that any diabetes diagnosis codes were recorded
dataset.tmp_cov_date_first_diabetes_diag = maximum_of( # change name to last
         cov_date_gestationaldm,
         cov_date_otherdm,
         cov_date_t1dm, 
         cov_date_t2dm, 
         cov_date_poccdm,
         tmp_cov_date_diabetes_medication,
         tmp_cov_date_nonmetform_drugs_snomed
)

## Prediabetes, on or before baseline
# Date of preDM code in primary care
tmp_cov_date_prediabetes = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(prediabetes_snomed))
        .where(clinical_events.date.is_on_or_before(baseline_date))
        .sort_by(clinical_events.date)
        .last_for_patient()
        .date
)
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

### DIABETES end ---------


## Hospitalization at baseline (incl. 1 day prior)
dataset.cov_bin_hosp_baseline = (
    hospital_admissions.where(hospital_admissions.admission_date.is_on_or_between(baseline_date - days(1), baseline_date))
    .exists_for_patient()
)

## Metformin use at baseline, defined as receiving a metformin prescription up until 6 months prior to baseline date (assuming half-yearly prescription for stable diabetes across GPs in the UK)
dataset.cov_bin_metfin_before_baseline = (
    medications.where(
        medications.dmd_code.is_in(metformin_codes)) # https://www.opencodelists.org/codelist/user/john-tazare/metformin-dmd/48e43356/
        .where(medications.date.is_on_or_between(baseline_date - days(6 * 30), baseline_date))
        .exists_for_patient()
)

"""
## Known hypersensitivity and / or intolerance to metformin, on or before baseline


"""

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


"""
## Use of the following medications in the last 14 days... 


"""


#######################################################################################
# (Other) Potential CONFOUNDER variables
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
dataset.tmp_most_recent_smoking_cat = tmp_most_recent_smoking_cat

ever_smoked = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(ever_smoking)) ### used a different codelist with ONLY smoking codes
        .where(clinical_events.date.is_on_or_before(baseline_date)) 
        .exists_for_patient()
)
dataset.ever_smoked = ever_smoked

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

## BMI, on or before baseline 
bmi_measurement = most_recent_bmi(
    where=clinical_events.date.is_after(baseline_date - days(5 * 365)),
    minimum_age_at_measurement=16,
)
cov_num_bmi = bmi_measurement.numeric_value
dataset.cov_cat_bmi_groups = case(
    when(cov_num_bmi < 18.5).then("Underweight"),
    when((cov_num_bmi >= 18.5) & (cov_num_bmi < 25.0)).then("Healthy weight (18.5-24.9)"),
    when((cov_num_bmi >= 25.0) & (cov_num_bmi < 30.0)).then("Overweight (25-29.9)"),
    when((cov_num_bmi >= 30.0) & (cov_num_bmi < 70.0)).then("Obese (>30)"),
    default="missing", # assume missing is non-obese
)

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


"""
    ## 2019 consultation rate
        cov_num_consulation_rate=patients.with_gp_consultations(
            between=[days(study_dates["pandemic_start"],-365), days(study_dates["pandemic_start"],-1)],
            returning="number_of_matches_in_period",
            return_expectations={
                "int": {"distribution": "poisson", "mean": 5},
            },
        ),

    ## Healthcare worker    
    cov_bin_healthcare_worker=patients.with_healthcare_worker_flag_on_covid_vaccine_record(
        returning='binary_flag', 
        return_expectations={"incidence": 0.01},
    ),
"""

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

#######################################################################################
# OUTCOME variables
#######################################################################################

#### SARS-CoV-2 --------- based on https://github.com/opensafely/long-covid/blob/main/analysis/codelists.py

## COVID infection
# First COVID-19 code (diagnosis, positive test or sequelae) in primary care, after baseline date
tmp_out_covid19_primary_care_date = (
    primary_care_covid_events.where(clinical_events.date.is_on_or_after(baseline_date)) # details re primary_care_covid_events see # DEFINE the baseline date based on SARS-CoV-2 infection
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
# First positive SARS-COV-2 PCR, after baseline date
tmp_out_covid19_sgss_date = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null()) # double-check with https://docs.opensafely.org/ehrql/reference/schemas/beta.tpp/#sgss_covid_all_tests
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_after(baseline_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)
# First covid-19 related hospital admission, after baseline date
tmp_out_covid19_hes_date = (
    hospital_admissions.where(hospital_admissions.all_diagnoses.is_in(covid_codes_incl_clin_diag)) # includes the only clinically diagnosed cases: https://www.opencodelists.org/codelist/opensafely/covid-identification/2020-06-03/
    .where(hospital_admissions.admission_date.is_on_or_after(baseline_date))
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)
dataset.out_covid19_date = minimum_of(tmp_out_covid19_primary_care_date, tmp_out_covid19_sgss_date, tmp_out_covid19_hes_date)

# Emergency attendance for covid, after baseline date // probably incorporate into above
dataset.out_covid19_emergency_date = (
    emergency_diagnosis_matches(covid_emergency)
    .where(emergency_care_attendances.arrival_date.is_on_or_after(baseline_date))
    .sort_by(emergency_care_attendances.arrival_date)
    .first_for_patient()
    .arrival_date
)

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
dataset.out_long_covid = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_after(baseline_date))
    .exists_for_patient()
)
# First Long COVID code in primary care after baseline date
dataset.out_long_covid_first_date = (
    primary_care_long_covid.where(clinical_events.date.is_on_or_after(baseline_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)
# Any viral fatigue code in primary care after baseline date
dataset.out_viral_fatigue = (
    clinical_events.where(clinical_events.snomedct_code.is_in(post_viral_fatigue_codes))
    .where(clinical_events.date.is_on_or_after(baseline_date))
    .exists_for_patient()
)
# First viral fatigue code in primary care after baseline date
dataset.out_viral_fatigue_first_date = (
    clinical_events.where(clinical_events.snomedct_code.is_in(post_viral_fatigue_codes))
    .where(clinical_events.date.is_on_or_after(baseline_date))
    .sort_by(clinical_events.date)
    .first_for_patient()
    .date
)

## Death
# all-cause death ## death table: I need to search in all cause of death or only underlying_cause_of_death ?
dataset.death_date = ons_deaths.date

# covid-related death (stated anywhere on any of the 15 death certificate options) # https://github.com/opensafely/comparative-booster-spring2023/blob/main/analysis/codelists.py uses a different codelist: codelists/opensafely-covid-identification.csv
dataset.death_cause_covid = cause_of_death_matches(covid_codes_incl_clin_diag)

