construct_trials <- function(data, period, treat_window, censor = FALSE, construct_trial_no){
  data %<>%
    dplyr::filter(period == !!period)
  trials <-
    lapply(X = 0:{treat_window - 1},
           FUN = construct_trial_no, data = data) %>% bind_rows()
  if (censor == TRUE) {
    trials %<>%
      dplyr::group_by(patient_id, trial) %>%
      dplyr::filter(!(arm == 0 & treatment_seq == 1)) %>%
      dplyr::ungroup()
  }
  trials
}
