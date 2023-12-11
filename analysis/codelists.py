from ehrql import (
    case,
    codelist_from_csv,
    create_dataset,
    days,
    when,
)

# ethnicity
ethnicity_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity.csv",
    column="Code",
    category_column="Grouping_6",
)
primis_covid19_vacc_update_ethnicity = codelist_from_csv(
    "codelists/primis-covid19-vacc-uptake-eth2001.csv",
    column="code",
    category_column="grouping_6_id",
)

## covid
covid_primary_care_positive_test = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-positive-test.csv", column="CTV3ID")
covid_primary_care_code = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-clinical-code.csv", column="CTV3ID")
covid_primary_care_sequelae = codelist_from_csv("codelists/opensafely-covid-identification-in-primary-care-probable-covid-sequelae.csv", column="CTV3ID")
covid_codes = codelist_from_csv("codelists/user-RochelleKnight-confirmed-hospitalised-covid-19.csv", column="code")

## diabetes
# T1DM
diabetes_type1_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-type-1-diabetes.csv",column="code")
# T2DM
diabetes_type2_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-type-2-diabetes.csv",column="code")
# Other or non-specific diabetes
diabetes_other_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-other-or-nonspecific-diabetes.csv",column="code")
# Gestational diabetes
diabetes_gestational_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-gestational-diabetes.csv",column="code")
# Type 1 diabetes secondary care
diabetes_type1_icd10 = codelist_from_csv("codelists/opensafely-type-1-diabetes-secondary-care.csv",column="icd10_code")
# Type 2 diabetes secondary care
diabetes_type2_icd10 = codelist_from_csv("codelists/user-r_denholm-type-2-diabetes-secondary-care-bristol.csv",column="code")
# Non-diagnostic diabetes codes
diabetes_diagnostic_ctv3_clinical = codelist_from_csv("codelists/user-hjforbes-nondiagnostic-diabetes-codes.csv",column="code")

# HbA1c
hba1c_new_codes = codelist_from_csv("codelists/user-alainamstutz-hba1c-bristol.csv",column="code")
# Antidiabetic drugs
insulin_snomed_clinical = codelist_from_csv("codelists/opensafely-insulin-medication.csv",column="id")
antidiabetic_drugs_snomed_clinical = codelist_from_csv("codelists/opensafely-antidiabetic-drugs.csv",column="id")
non_metformin_dmd = codelist_from_csv("codelists/user-r_denholm-non-metformin-antidiabetic-drugs_bristol.csv",column="id")

# Prediabetes
prediabetes_snomed = codelist_from_csv("codelists/opensafely-prediabetes-snomed.csv",column="code")

## metformin
metformin_codes = codelist_from_csv("codelists/user-john-tazare-metformin-dmd.csv",column="code")

# kidney disease
ckd_snomed_clinical_45 = codelist_from_csv("codelists/nhsd-primary-care-domain-refsets-ckdatrisk1_cod.csv",column="code")

ckd_snomed_clinical = codelist_from_csv("codelists/user-elsie_horne-ckd_snomed.csv",column="code")
ckd_icd10 = codelist_from_csv("codelists/user-elsie_horne-ckd_icd10.csv",column="code")

# liver disease
liver_disease_snomed_clinical = codelist_from_csv("codelists/user-elsie_horne-liver_disease_snomed.csv",column="code")
liver_disease_icd10 = codelist_from_csv("codelists/user-elsie_horne-liver_disease_icd10.csv",column="code")

# heart failure
hf_snomed_clinical = codelist_from_csv("codelists/user-elsie_horne-hf_snomed.csv",column="code")
hf_icd10 = codelist_from_csv("codelists/user-RochelleKnight-hf_icd10.csv",column="code")

# smoking
smoking_clear = codelist_from_csv("codelists/opensafely-smoking-clear.csv",
    column="CTV3Code",
    category_column="Category",
)

smoking_unclear = codelist_from_csv("codelists/opensafely-smoking-unclear.csv",
    column="CTV3Code",
    category_column="Category",
)

# Patients in long-stay nursing and residential care
carehome = codelist_from_csv("codelists/primis-covid19-vacc-uptake-longres.csv",column="code")

# obesity
bmi_obesity_snomed_clinical = codelist_from_csv("codelists/user-elsie_horne-bmi_obesity_snomed.csv",column="code")
bmi_obesity_icd10 = codelist_from_csv("codelists/user-elsie_horne-bmi_obesity_icd10.csv",column="code")

# acute myocardial infarction
ami_snomed_clinical = codelist_from_csv("codelists/user-elsie_horne-ami_snomed.csv",column="code")
ami_icd10 = codelist_from_csv("codelists/user-RochelleKnight-ami_icd10.csv",column="code")
ami_prior_icd10 = codelist_from_csv("codelists/user-elsie_horne-ami_prior_icd10.csv",column="code")






## long covid
long_covid_diagnostic_codes = codelist_from_csv("codelists/opensafely-nice-managing-the-long-term-effects-of-covid-19.csv",column="code")
long_covid_referral_codes = codelist_from_csv("codelists/opensafely-referral-and-signposting-for-long-covid.csv",column="code")
long_covid_assessment_codes = codelist_from_csv("codelists/opensafely-assessment-instruments-and-outcome-measures-for-long-covid.csv",column="code")
post_viral_fatigue_codes = codelist_from_csv("codelists/user-alex-walker-post-viral-syndrome.csv",column="code")