from ehrql import (
    case,
    codelist_from_csv,
    create_dataset,
    days,
    when,
)
#from ehrql.tables.beta.core import clinical_events, patients

## demographics / ethnicitiy
ethnicity_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity.csv",
    column="Code",
    category_column="Grouping_6",
)

## acute covid
covid_codes = codelist_from_csv("codelists/opensafely-covid-identification.csv", column="icd10_code")
covid_primary_care_positive_test = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-positive-test.csv", column="CTV3ID")
covid_primary_care_code = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-clinical-code.csv", column="CTV3ID")
covid_primary_care_sequalae = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-sequelae.csv", column="CTV3ID")

## long covid
long_covid_diagnostic_codes = codelist_from_csv("codelists/opensafely-nice-managing-the-long-term-effects-of-covid-19.csv",column="code")
long_covid_referral_codes = codelist_from_csv("codelists/opensafely-referral-and-signposting-for-long-covid.csv",column="code")
long_covid_assessment_codes = codelist_from_csv("codelists/opensafely-assessment-instruments-and-outcome-measures-for-long-covid.csv",column="code")
post_viral_fatigue_codes = codelist_from_csv("codelists/user-alex-walker-post-viral-syndrome.csv",column="code")

## diabetes / T2DM
diabetes_codes = codelist_from_csv( "codelists/opensafely-diabetes.csv", column="CTV3ID")
t2dm_codes = codelist_from_csv("codelists/opensafely-type-2-diabetes.csv", column="CTV3ID")
prediabetes_codes = codelist_from_csv("codelists/opensafely-prediabetes-snomed.csv", column="code")

## metformin
metformin_codes = codelist_from_csv("codelists/user-john-tazare-metformin-dmd.csv",column="code")

## other
asthma_inhaler_codes = codelist_from_csv("codelists/opensafely-asthma-inhaler-salbutamol-medication.csv",column="code")

#dataset = create_dataset()
#dataset.define_population(patients.exists_for_patient())
