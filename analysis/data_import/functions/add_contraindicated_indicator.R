add_contraindicated_indicator <- function(data_processed) {
  data_processed <-
    data_processed %>% 
    mutate(
      ci_cirrhosis_snomed = 
        if_else(advanced_decompensated_cirrhosis, TRUE, FALSE),
      ci_cirrhosis_icd10 =
        if_else(decompensated_cirrhosis_icd10, TRUE, FALSE), # strict ci
      ci_cirrhosis_icd10_prim_diag =
        if_else(decompensated_cirrhosis_icd10_prim_diag, TRUE, FALSE),
      ci_ascitic_drainage =
        if_else(ascitic_drainage_snomed, TRUE, FALSE),
      ci_solid_organ_highrisk =
        if_else(solid_organ_transplant_nhsd_new, TRUE, FALSE),
      ci_solid_organ_snomed =
        if_else(solid_organ_transplant_snomed, TRUE, FALSE),
      ci_ckd5_nhsd = 
        if_else(ckd_stage_5_nhsd, TRUE, FALSE),
      ci_ckd3_primis =
        if_else(!is.na(ckd_primis_stage) & ckd_primis_stage == "3", TRUE, FALSE),
      ci_ckd45_primis =
        if_else(!is.na(ckd_primis_stage) & ckd_primis_stage %in% c("4", "5"), TRUE, FALSE),
      ci_ckd3_icd10 = 
        if_else(ckd3_icd10, TRUE, FALSE),
      ci_ckd45_icd10 =
        if_else(ckd4_icd10 | ckd5_icd10, TRUE, FALSE),
      ci_dialysis =
        if_else(dialysis | dialysis_icd10 | dialysis_procedure, TRUE, FALSE),
      ci_kidney_transplant =
        if_else(kidney_transplant | kidney_transplant_icd10 | kidney_transplant_procedure, TRUE , FALSE),
      ci_egfr_30_59 = 
        if_else((!is.na(eGFR_record) & (eGFR_record >= 30 & eGFR_record < 60)) | 
                  (!is.na(eGFR_short_record) & (eGFR_short_record >= 30 & eGFR_short_record < 60)), TRUE, FALSE),
      ci_egfr_below30 = 
        if_else((!is.na(eGFR_record) & eGFR_record < 30) | 
                  (!is.na(eGFR_short_record) & eGFR_short_record < 30), TRUE, FALSE),
      ci_egfr_creat_30_59 =
        if_else((!is.na(egfr_ctv3) & (egfr_ctv3 >= 30 & egfr_ctv3 < 60)) | 
                  (!is.na(egfr_snomed) & (egfr_snomed >= 30 & egfr_snomed < 60)) |
                  (!is.na(egfr_short_snomed) & (egfr_short_snomed >= 30 & egfr_short_snomed < 60)), TRUE, FALSE),
      ci_egfr_creat_below30 =
        if_else((!is.na(egfr_ctv3) & egfr_ctv3 < 30) | 
                  (!is.na(egfr_snomed) & egfr_snomed < 30) |
                  (!is.na(egfr_short_snomed) & egfr_short_snomed < 30), TRUE, FALSE),
      ci_drugs_do_not_use =
        if_else(drugs_do_not_use, TRUE, FALSE),
      ci_drugs_do_not_use_absolute =
        if_else(drugs_do_not_use_absolute, TRUE, FALSE),
      # aggregated
      ci_liver_disease = 
        if_else(ci_cirrhosis_snomed | ci_cirrhosis_icd10_prim_diag | ci_ascitic_drainage, TRUE, FALSE),
      ci_solid_organ_transplant =
        if_else(ci_solid_organ_highrisk | ci_solid_organ_snomed, TRUE, FALSE),
      ci_renal_disease =
        if_else(ci_ckd5_nhsd | ci_ckd45_primis | ci_ckd45_icd10 |
                  ci_dialysis | ci_kidney_transplant | ci_egfr_below30 |
                  ci_egfr_creat_below30, TRUE, FALSE), # not excluding ckd stage3
      contraindicated =
        if_else(ci_liver_disease | ci_solid_organ_transplant |
                  ci_renal_disease | ci_drugs_do_not_use_absolute, TRUE, FALSE),
      contraindicated_strict =
        if_else(ci_liver_disease | ci_cirrhosis_icd10 | ci_solid_organ_transplant | 
                  ci_renal_disease | ci_ckd3_primis | ci_ckd3_icd10 |
                  ci_egfr_30_59 | ci_egfr_creat_30_59 |
                  ci_drugs_do_not_use, TRUE, FALSE)
    )
}