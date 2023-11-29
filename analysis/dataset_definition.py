# BASED on https://github.com/opensafely/post-covid-diabetes/blob/main/analysis/common_variables.py 


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
)
## from codelists.py
from codelists import *

## datetime function
from datetime import date


# DEFINE the index date
pandemic_start = "2020-02-01"
vaccine_peak = "2021-06-18"
index_date = pandemic_start


# START the dataset and set the dummy dataset size
dataset = create_dataset()
dataset.configure_dummy_data(population_size=50)


# population variables for dataset definition
is_female_or_male = patients.sex.is_in(["female", "male"]) # only include f, m and no missing values
was_adult = (patients.age_on(index_date) >= 18) & (
    patients.age_on(index_date) <= 110
) # only include adults and no missing values
was_alive = (
    patients.date_of_death.is_after(index_date)
    | patients.date_of_death.is_null()
) # only include if alive 
was_registered = practice_registrations.for_patient_on(
    index_date
).exists_for_patient() # only include if registered on index date

# define/create dataset
dataset.define_population(
    is_female_or_male
    & was_adult
    & was_alive
    & was_registered
) 



# DEMOGRAPHIC variables
## age
dataset.age = patients.age_on(index_date)

## sex
dataset.sex = patients.sex

## ethnicitiy
dataset.ethnicity = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(ethnicity_codes)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codes)
)

## geographic and IMD data (https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019)
address = addresses.for_patient_on(index_date)
dataset.imd = address.imd_rounded
dataset.imd_quintile = case(
    when((address.imd_rounded >=0) & (address.imd_rounded < int(32844 * 1 / 5))).then("1 (most deprived)"),
    when(address.imd_rounded < int(32844 * 2 / 5)).then("2"),
    when(address.imd_rounded < int(32844 * 3 / 5)).then("3"),
    when(address.imd_rounded < int(32844 * 4 / 5)).then("4"),
    when(address.imd_rounded < int(32844 * 5 / 5)).then("5 (least deprived)"),
    default="unknown"
)
dataset.rural_urban_classification = address.rural_urban_classification
#1 - Urban major conurbation
#2 - Urban minor conurbation
#3 - Urban city and town
#4 - Urban city and town in a sparse setting
#5 - Rural town and fringe
#6 - Rural town and fringe in a sparse setting
#7 - Rural village and dispersed
#8 - Rural village and dispersed in a sparse setting
dataset.msoa_code = address.msoa_code

## patient's practice data
registration = practice_registrations.for_patient_on(index_date)
dataset.practice = registration.practice_pseudo_id
dataset.stp = registration.practice_stp
dataset.region = registration.practice_nuts1_region_name


# EXPOSURE variables ------------------------------------------------------

### 1) Who are the people who take Metfin (index_date onwards) in terms of underlying disease?
#### T2DM, pre(T2)DM, PCO, HbA1c high (date of first ever recording): Primary care & HES APC
### 2) COVID-19 infection
#### "Generate variable to identify first date of confirmed COVID"
### How many were started on Metfin at/shortly after COVID-19 infection?

## Metformin: https://www.opencodelists.org/codelist/user/john-tazare/metformin-dmd/48e43356/
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

## DIABETES exposure -------------------

### Type 1 Diabetes
## Date of first ever recording
# Primary care
tmp_exp_date_t1dm_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_type1_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)
# HES APC
tmp_exp_date_t1dm_hes = (
    hospital_admissions.where(
        hospital_admissions.all_diagnoses.is_in(diabetes_type1_icd10))
        .where(hospital_admissions.admission_date.is_on_or_after("1990-01-01"))
        .sort_by(hospital_admissions.admission_date)
        .first_for_patient()
        .admission_date
)
# Combined
exp_date_t1dm = minimum_of(tmp_exp_date_t1dm_snomed, tmp_exp_date_t1dm_hes)

## Count of number of records
# Primary care
tmp_exp_count_t1dm_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_type1_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        ).count_for_patient()
# HES APC
tmp_exp_count_t1dm_hes = (
    hospital_admissions.where(
        hospital_admissions.all_diagnoses.is_in(diabetes_type1_icd10))
        .where(hospital_admissions.admission_date.is_on_or_after("1990-01-01"))
        ).count_for_patient()


### Type 2 Diabetes
## Date of first ever recording
# Primary care
tmp_exp_date_t2dm_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_type2_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)
# HES APC
tmp_exp_date_t2dm_hes = (
    hospital_admissions.where(
        hospital_admissions.all_diagnoses.is_in(diabetes_type2_icd10))
        .where(hospital_admissions.admission_date.is_on_or_after("1990-01-01"))
        .sort_by(hospital_admissions.admission_date)
        .first_for_patient()
        .admission_date
)  
# Combined
exp_date_t2dm = minimum_of(tmp_exp_date_t2dm_snomed, tmp_exp_date_t2dm_hes)

## Count of number of records
# Primary care
tmp_exp_count_t2dm_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_type2_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        ).count_for_patient()
# HES APC
tmp_exp_count_t2dm_hes = (
    hospital_admissions.where(
        hospital_admissions.all_diagnoses.is_in(diabetes_type2_icd10))
        .where(hospital_admissions.admission_date.is_on_or_after("1990-01-01"))
        ).count_for_patient()


### Diabetes unspecified
## Date of first ever recording
# Primary care
exp_date_otherdm = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_other_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)

## Count of number of records
# Primary care
tmp_exp_count_otherdm = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_other_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        ).count_for_patient()


### Gestational diabetes
## Date of first ever recording
# Primary care
exp_date_gestationaldm = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_gestational_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)


### Diabetes diagnostic codes
## Date of first ever recording
# Primary care
exp_date_poccdm = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_diagnostic_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)

## Count of number of records
# Primary care
tmp_exp_count_poccdm_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(diabetes_diagnostic_snomed_clinical))
        .where(clinical_events.date.is_on_or_after("1990-01-01"))
        ).count_for_patient()


### Variables needed to define diabetes
### Maximum latest HbA1c measure



        

## SARS-CoV-2 pos / COVID-19 diagnosis --> take it from the long covid repo
## Date of positive SARS-COV-2 PCR antigen test / define a time period or just on_or_after?
dataset.tmp_exp_date_covid19_confirmed_sgss = (
    sgss_covid_all_tests.where(
        sgss_covid_all_tests.is_positive.is_not_null())
        .where(sgss_covid_all_tests.lab_report_date.is_on_or_after(index_date))
        .sort_by(sgss_covid_all_tests.lab_report_date)
        .first_for_patient()
        .lab_report_date
)
## First COVID-19 code (diagnosis, positive test or sequalae) in primary care
dataset.tmp_exp_date_covid19_confirmed_snomed = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(covid_primary_care_positive_test) | clinical_events.ctv3_code.is_in(covid_primary_care_code) | clinical_events.ctv3_code.is_in(covid_primary_care_sequalae))
        .where(clinical_events.date.is_on_or_after(index_date))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)
## Start date of episode with confirmed diagnosis in any position

# OUTCOME variables
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

## hospital admission
dataset.date_of_first_admission = (
    hospital_admissions.where(
        hospital_admissions.admission_date.is_after(
            index_date
        )
    )
    .sort_by(hospital_admissions.admission_date)
    .first_for_patient()
    .admission_date
)

## death
dataset.date_of_death_ons = ons_deaths.date
dataset.place_of_death_ons = ons_deaths.place
dataset.cause_of_death_ons = ons_deaths.cause_of_death_01