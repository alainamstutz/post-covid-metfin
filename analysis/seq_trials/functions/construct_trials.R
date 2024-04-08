construct_trials <- function(data, period, treat_window, censor = FALSE, construct_trial_no){
  data %<>%
    dplyr::filter(period == !!period) # The !! operator is used for unquoting period, allowing its value to be used as a variable rather than a literal. => only keep data with period equal to any value in cuts, i.e. periods with no participants are not selected
    trials <- lapply(X = 0:{treat_window - 1}, # trial_no
                    FUN = construct_trial_no, data = data) %>%
    bind_rows()
  if (censor == TRUE) {
    trials %<>%
      dplyr::group_by(patient_id, trial) %>%
      dplyr::filter(!(arm == 0 & treatment_seq == 1)) %>%
      dplyr::ungroup()
  }
  trials
}
