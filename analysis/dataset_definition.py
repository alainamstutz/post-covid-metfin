from ehrql import (
    case,
    codelist_from_csv,
    create_dataset,
    days,
    when,
    weeks
)
from ehrql.tables.beta.tpp import (
    addresses,
    clinical_events,
    hospital_admissions,
    medications,
    patients,
    practice_registrations,
    ons_deaths,
)

# from cohortextractor import combine_codelists ## would be nice to 

# define important dates
pandemic_start = "2020-02-01"
index_date = "2021-12-16" ### Omicron BA.1 dominance in UK

# start the dataset and set the dummy dataset size
dataset = create_dataset()
dataset.configure_dummy_data(population_size=50)

# import codelists from codelists.py
from codelists import *

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
age = patients.age_on(index_date)
dataset.age = age
dataset.age_band = case(
        when(age < 20).then("0-19"),
        when(age < 40).then("20-39"),
        when(age < 60).then("40-59"),
        when(age < 80).then("60-79"),
        when(age >= 80).then("80+"),
        default="missing",
)
dataset.date_of_birth = patients.date_of_birth

## sex
dataset.sex = patients.sex

## ethnicitiy
dataset.ethnicity = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(ethnicity_codelist)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .ctv3_code.to_category(ethnicity_codelist)
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



# EXPOSURE variables
dataset.first_metfin_date = (
    medications.where(
        medications.dmd_code.is_in(metformin_codelist))
        .where(medications.date.is_on_or_after(index_date))
        .sort_by(medications.date)
        .first_for_patient()
        .date
)

dataset.num_metfin_prescriptions_within_1y = (
    medications.where(
        medications.dmd_code.is_in(metformin_codelist))
        .where(medications.date.is_on_or_between(index_date, "2023-01-01"))
        .count_for_patient()
)

dataset.t2dm = (
    clinical_events.where(
        clinical_events.ctv3_code.is_in(t2dm_codelist))
        #.where(clinical_events.date.is_on_or_after(pandemic_start))
        .sort_by(clinical_events.date)
        .first_for_patient()
        .date
)

dataset.num_asthma_inhaler_medications = medications.where(
    medications.dmd_code.is_in(asthma_inhaler_codelist)
    & medications.date.is_on_or_between(
        index_date - days(30), index_date
    )
).count_for_patient()



# OUTCOME variables
## long/post covid
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