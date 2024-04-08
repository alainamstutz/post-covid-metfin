construct_trial2 <- function(data, period, trial_no, censor = FALSE){
  trial <-
    data %>%
    dplyr::filter(period == !!period,
                  tstart >= !!trial_no) %>%
    dplyr::mutate(trial = !!trial_no) %>%
    dplyr::group_by(patient_id) %>%
    dplyr::mutate(arm = dplyr::first(treatment_seq),
                  treatment_seq_lag1_baseline = dplyr::first(treatment_seq_lag1),
                  tstart = tstart - !!trial_no,
                  tend = tend - !!trial_no) %>%
    dplyr::filter(treatment_seq_lag1_baseline == 0, #restrict to those not previously treated at the start of the trial
                  dplyr::first(treatment_seq_sotmol) == 0) %>% # restrict to those not treated with sot/mol in the first interval
    dplyr::ungroup() %>%
    dplyr::select(- c(treatment_seq_lag1_baseline, treatment_seq_lag1)) %>%
    dplyr::relocate(patient_id, starts_with("period_"), trial, tstart, tend, arm)
  if (censor == TRUE) {
    trial %<>%
      dplyr::group_by(patient_id, trial) %>%
      dplyr::filter(!(arm == 0 & treatment_seq == 1)) %>%
      dplyr::ungroup()
  }
  trial
}
