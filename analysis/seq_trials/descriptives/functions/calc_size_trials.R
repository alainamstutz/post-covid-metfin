calc_size_trials <- function(trials){
  trials %>%
    group_by(period, trial, arm) %>%
    summarise(n = length(unique(patient_id)), .groups = "keep") %>%
    mutate(period = as.integer(period),
           trial = trial) %>%
    ungroup() %>%
    pivot_wider(
      names_from = arm,
      values_from = n, 
      names_prefix = "n_") %>%
    relocate(n_1, .before = n_0) %>%
    mutate(across(c("n_0", "n_1"), .f = ~ if_else(is.na(.x), 0L, .x))) %>%
    rename(n_treated = n_1, n_untreated = n_0)
}