## function for marginalisation ----
create_survcurve <-  function(
    data_counterfact,
    plrmod,
    vcov, 
    cuminc_variance, # function to estimate variance cuminc
    id,
    trial,
    time,
    weights){
  
  # g-formula to get marginalised/adjusted incidence / survival curves
  
  data_counterfact %>%
    mutate(
      prob = predict(plrmod, newdata = ., type = "response")
    ) %>%
    # marginalise over all patients
    group_by(tend) %>%
    summarise(
      prob = weighted.mean(prob, .data[[weights]]),
    ) %>%
    ungroup() %>%
    mutate(
      survival = cumprod(1 - prob),
      survival_se = cuminc_variance(plrmod, 
                                    vcov,
                                    data_counterfact, 
                                    id,
                                    trial,
                                    time, 
                                    weights) %>% sqrt()
    ) %>%
    select(-prob) %>%
    mutate(
      survival_ll = pmax(0, survival + qnorm(0.025) * survival_se) - pmax(0, survival + qnorm(0.975) * survival_se - 1),
      survival_ul = pmin(1, survival + qnorm(0.975) * survival_se) + pmin(0, survival + qnorm(0.025) * survival_se),
      haz = (lag(survival, n = 1, default = 1) - survival) / survival
    ) %>%
    add_row(
      tend = 0,
      survival_se = 0,
      survival = 1,
      survival_ll = 1,
      survival_ul = 1,
      haz = 0,
      .before = 1
    ) %>%
    mutate(
      lead_tend = lead(tend),
      .after = tend
    )
}
