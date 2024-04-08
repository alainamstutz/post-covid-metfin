construct_trial_no <- function(data, trial_no){
  data %>%
    dplyr::filter(tstart >= trial_no) %>% # restrict baseline of k-th trial to k-th tstart (i.e. time after pos test, with initiation on day of pos test = trial 1)
    dplyr::mutate(trial = trial_no) %>% # the k-th trial
    dplyr::group_by(patient_id) %>%
    dplyr::mutate(arm = dplyr::first(treatment_seq), # at baseline of k-th trial
                  treatment_seq_lag1_baseline = dplyr::first(treatment_seq_lag1), # at baseline of k-th trial => participants with no treatment at all during follow-up (data$tb_postest_treat_seq == NA) this returns NA => excluded below, line 10
                  tstart = tstart - trial_no,
                  tend = tend - trial_no) %>%
    dplyr::filter(treatment_seq_lag1_baseline == 0, #restrict to those not previously treated at the start of the trial (not eligible anymore), but what about those with treatment_seq_lag1_baseline == NA?
                  # dplyr::first(treatment_seq_sotmol) == 0 # restrict to those not treated with sot/mol in the first interval
                  ) %>%
    dplyr::ungroup() %>%
    dplyr::select(- c(treatment_seq_lag1_baseline)) %>%
    dplyr::relocate(patient_id, starts_with("period_"), trial, tstart, tend, arm)
}
