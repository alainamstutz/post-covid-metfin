calc_n_excluded_contraindicated <- function(data_processed){
    n_before_exclusion_contraindications <- 
    data_processed %>%
    nrow()
  # liver disease
  n_cirrhosis_snomed <-
    data_processed %>%
    filter(advanced_decompensated_cirrhosis == TRUE) %>%
    nrow()
  n_cirrhosis_icd10 <- 
    data_processed %>%
    filter(decompensated_cirrhosis_icd10 == TRUE) %>%
    nrow()
  n_cirrhosis_icd10_prim_diag <-
    data_processed %>%
    filter(decompensated_cirrhosis_icd10_prim_diag == TRUE) %>%
    nrow()
  n_ascitic_drainage <-
    data_processed %>%
    filter(ascitic_drainage_snomed == TRUE) %>%
    nrow()
  # solid organ transplant
  n_solid_organ_highrisk <-
    data_processed %>%
    filter(solid_organ_transplant_nhsd_new == TRUE) %>%
    nrow()
  n_solid_organ_snomed <-
    data_processed %>%
    filter(solid_organ_transplant_snomed == TRUE) %>%
    nrow()
  # renal disease
  n_ckd5_nhsd <-
    data_processed %>%
    filter(ckd_stage_5_nhsd == TRUE) %>%
    nrow()
  n_ckd3_primis <-
    data_processed %>%
    filter(ckd_primis_stage == "3") %>%
    nrow()
  n_ckd45_primis <-
    data_processed %>%
    filter(ckd_primis_stage %in% c("4", "5")) %>%
    nrow()
  n_ckd3_icd10 <-
    data_processed %>%
    filter(ckd3_icd10 == TRUE) %>%
    nrow()
  n_ckd45_icd10 <-
    data_processed %>%
    filter(ckd4_icd10 == TRUE | ckd5_icd10 == TRUE) %>%
    nrow()
  n_dialysis <-
    data_processed %>%
    filter(dialysis == TRUE | dialysis_icd10 == TRUE | dialysis_procedure == TRUE) %>%
    nrow()
  n_kidney_transplant <-
    data_processed %>%
    filter(kidney_transplant == TRUE | kidney_transplant_icd10 == TRUE | kidney_transplant_procedure == TRUE) %>%
    nrow()
  n_egfr_30_59 <-
    data_processed %>%
    filter((!is.na(eGFR_record) & (eGFR_record >= 30 & eGFR_record < 60)) | 
             (!is.na(eGFR_short_record) & (eGFR_short_record >= 30 & eGFR_short_record < 60))) %>%
    nrow()
  n_egfr_below30 <-
    data_processed %>%
    filter((!is.na(eGFR_record) & eGFR_record < 30) | 
             (!is.na(eGFR_short_record) & eGFR_short_record < 30)) %>%
    nrow()
  n_egfr_creat_30_59 <-
    data_processed %>%
    filter((!is.na(egfr_ctv3) & (egfr_ctv3 >= 30 & egfr_ctv3 < 60)) | 
             (!is.na(egfr_snomed) & (egfr_snomed >= 30 & egfr_snomed < 60)) |
             (!is.na(egfr_short_snomed) & (egfr_short_snomed >= 30 & egfr_short_snomed < 60))) %>%
    nrow()
  n_egfr_creat_below30 <-
    data_processed %>%
    filter((!is.na(egfr_ctv3) & egfr_ctv3 < 30) | 
             (!is.na(egfr_snomed) & egfr_snomed < 30) |
             (!is.na(egfr_short_snomed) & egfr_short_snomed < 30)) %>%
    nrow()
  # drugs do not use 
  n_drugs_do_not_use <-
    data_processed %>%
    filter(drugs_do_not_use == TRUE) %>%
    nrow()
  # drugs do not use absolute
  n_drugs_do_not_use_absolute <-
    data_processed %>%
    filter(drugs_do_not_use_absolute == TRUE) %>%
    nrow()
  # drugs caution (not excluded!)
  n_drugs_caution <-
    data_processed %>%
    filter(drugs_consider_risk == TRUE) %>%
    nrow()
  n_after_exclusion_contraindications <- 
    data_processed %>% 
    filter(advanced_decompensated_cirrhosis == FALSE & decompensated_cirrhosis_icd10_prim_diag == FALSE) %>%
    filter(ascitic_drainage_snomed == FALSE) %>%
    filter(solid_organ_transplant_nhsd_new == FALSE) %>%
    filter(solid_organ_transplant_snomed == FALSE) %>%
    filter(ckd_stage_5_nhsd == FALSE) %>% 
    filter(!(ckd_primis_stage %in% c("4", "5"))) %>% #FIXME: two codes missing see above
    filter(ckd4_icd10 == FALSE & ckd5_icd10 == FALSE) %>%
    filter(dialysis == FALSE & dialysis_icd10 == FALSE & dialysis_procedure == FALSE) %>%
    filter(kidney_transplant == FALSE & kidney_transplant_icd10 == FALSE & kidney_transplant_procedure == FALSE) %>%
    filter((is.na(eGFR_record) | eGFR_record >= 30) & 
             (is.na(eGFR_short_record) | eGFR_short_record >= 30) & 
             (is.na(egfr_ctv3) | egfr_ctv3 >= 30) &
             (is.na(egfr_snomed) | egfr_snomed >= 30) &
             (is.na(egfr_short_snomed) | egfr_short_snomed >= 30)
    ) %>%
    filter(drugs_do_not_use_absolute == FALSE) %>%
    nrow()
  n_after_exclusion_contraindications_strict <- 
    data_processed %>% 
    filter(advanced_decompensated_cirrhosis == FALSE & decompensated_cirrhosis_icd10 == FALSE & decompensated_cirrhosis_icd10_prim_diag == FALSE) %>%
    filter(ascitic_drainage_snomed == FALSE) %>%
    filter(solid_organ_transplant_nhsd_new == FALSE) %>%
    filter(solid_organ_transplant_snomed == FALSE) %>%
    filter(ckd_stage_5_nhsd == FALSE) %>%
    filter(!(ckd_primis_stage %in% c("3", "4", "5"))) %>%
    filter(ckd3_icd10 == FALSE & ckd4_icd10 == FALSE & ckd5_icd10 == FALSE) %>%
    filter(dialysis == FALSE & dialysis_icd10 == FALSE & dialysis_procedure == FALSE) %>%
    filter(kidney_transplant == FALSE & kidney_transplant_icd10 == FALSE & kidney_transplant_procedure == FALSE) %>%
    filter((is.na(eGFR_record) | eGFR_record >= 60) & 
             (is.na(eGFR_short_record) | eGFR_short_record >= 60) & 
             (is.na(egfr_ctv3) | egfr_ctv3 >= 60) &
             (is.na(egfr_snomed) | egfr_snomed >= 60) &
             (is.na(egfr_short_snomed) | egfr_short_snomed >= 60)
           ) %>%
    filter(drugs_do_not_use == FALSE) %>%
    nrow()
  out <- tibble(n_before_exclusion_contraindications,
                n_cirrhosis_snomed,
                n_cirrhosis_icd10,
                n_cirrhosis_icd10_prim_diag,
                n_ascitic_drainage,
                n_solid_organ_highrisk,
                n_solid_organ_snomed,
                n_ckd5_nhsd,
                n_ckd3_primis,
                n_ckd45_primis,
                n_ckd3_icd10,
                n_ckd45_icd10,
                n_dialysis,
                n_kidney_transplant,
                n_egfr_30_59,
                n_egfr_below30,
                n_egfr_creat_30_59,
                n_egfr_creat_below30,
                n_drugs_do_not_use,
                n_drugs_do_not_use_absolute,
                n_drugs_caution,
                n_after_exclusion_contraindications,
                n_after_exclusion_contraindications_strict)
}
