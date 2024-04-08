add_ipacw <- function(trials, treatment_window = 5, covars){
  trials_4 <-
    trials %>%
    filter(trial == 4) %>% # no individuals starting treatment by design
    mutate(w = 1)
  trials_ipacw_added <-
    map(.x = 0:3,
        .f = ~ add_ipacw_trial_no(trials, treatment_window, .x, covars)) %>% 
    bind_rows() %>%
    bind_rows(., trials_4)
}
add_ipacw_trial_no <- function(trials, treatment_window, trial_no, covars){
  tend_max <- treatment_window - trial_no - 1
  trials_no <-
    trials %>%
    filter(trial == {{trial_no}}, arm == 0, tend <= {tend_max + 1}) %>%
    group_by(patient_id) %>%
    mutate(treatment_seq_lead1 = lead(treatment_seq, n = 1L, default = NA), 
           treatment_seq_lead1_equal_to_arm = if_else(treatment_seq_lead1 == arm, 1L, 0L)) %>%
    filter(treatment_seq != 1 & tend <= tend_max) %>% # censor and only select intervals where there is a chance someone starts treatment
    ungroup()
  #formula_num <- "treatment_seq_lead1_equal_to_arm ~ 1" %>% as.formula()
  if (tend_max > 1){
    # formula_denom <- paste0("treatment_seq_lead1_equal_to_arm ~ ",
    #                         paste0(c("factor(tend)"),
    #                                collapse = " + ")) %>% as.formula()
    formula_denom <- paste0("treatment_seq_lead1_equal_to_arm ~ ",
                            paste0(c("factor(tend) + ns(covid_test_positive_date, 3) + period", covars),
                                   collapse = " + ")) %>% as.formula()
  } else if (tend_max == 1){
    # formula_denom <- paste0("treatment_seq_lead1_equal_to_arm ~ 1") %>% as.formula()
    formula_denom <- paste0("treatment_seq_lead1_equal_to_arm ~ ",
                            paste0(c("ns(covid_test_positive_date, 3) + period", covars),
                                   collapse = " + ")) %>% as.formula()
  }
  # fit_num <- glm(formula_num,
  #                family = binomial(link = "logit"),
  #                data = trials_no)
  fit_denom <- glm(formula_denom,
                   family = binomial(link = "logit"),
                   data = trials_no)
  trials_no <-
    trials_no %>%
    mutate(#num = predict(fit_num, newdata = ., type = "response"),  
           denom = predict(fit_denom, newdata =., type = "response"),
           #ipacw = num / denom
           ipacw = 1 / denom) %>%
    select(patient_id, trial, tstart, tend, ipacw)
  trials %>%
    filter(trial == {{trial_no}} & !(arm == 0 & treatment_seq == 1)) %>%
    left_join(trials_no, by = c("patient_id", "trial", "tstart", "tend")) %>%
    mutate(ipacw = if_else(is.na(ipacw), 1, ipacw)) %>%
    group_by(patient_id) %>%
    mutate(ipacw_lag = lag(ipacw, n = 1, default = 1L),
           ipacw_lag_cumprod = cumprod(ipacw_lag),
           w = ipacw_lag_cumprod) %>%
    ungroup()
}
