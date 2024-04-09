calc_n_excluded_in_each_trial <- function(data){
 data %>%
    group_by(period) %>%
    summarise(n_total = n(),
              n_total_pax = sum(treatment_strategy_cat_prim == "Paxlovid"),
              n_total_sotmol = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir")),
              n_sotmol0 = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir") & tb_postest_treat == 0),
              n_sotmol1 = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir") & tb_postest_treat == 1),
              n_sotmol2 = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir") & tb_postest_treat == 2),
              n_sotmol3 = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir") & tb_postest_treat == 3),
              n_sotmol4 = sum(treatment_strategy_cat_prim %in% c("Sotrovimab", "Molnupiravir") & tb_postest_treat == 4),
              n_event0 = sum(status_primary == "covid_hosp_death" & fu_primary == 0), # should be 0
              n_event1 = sum(status_primary == "covid_hosp_death" & fu_primary == 1 & 
                               !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 1)),
              n_event2 = sum(status_primary == "covid_hosp_death" & fu_primary == 2 &
                               !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 2)),
              n_event3 = sum(status_primary == "covid_hosp_death" & fu_primary == 3 & 
                               !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 3)),
              n_event4 = sum(status_primary == "covid_hosp_death" & fu_primary == 4 & 
                               !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 4)),
              n_censor0 = sum(status_primary != "covid_hosp_death" & fu_primary == 0),
              n_censor1 = sum(status_primary!= "covid_hosp_death" & fu_primary == 1 & 
                                !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 1)),
              n_censor2 = sum(status_primary != "covid_hosp_death" & fu_primary == 2 & 
                                !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 2)),
              n_censor3 = sum(status_primary != "covid_hosp_death" & fu_primary == 3 & 
                                !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 3)),
              n_censor4 = sum(status_primary != "covid_hosp_death" & fu_primary == 4 & 
                                !(treatment_strategy_cat_prim != c("Untreated") & tb_postest_treat < 4)),
    ) %>%
    ungroup() %>%
    pivot_longer(cols = starts_with(c("n_sotmol", "n_event", "n_censor")),
                 names_to = c(".value", "trial"),
                 names_pattern = "(n_.*)(.)") %>% 
    relocate(trial, .after = period) %>%
    mutate(period = period %>% as.integer(),
           trial = trial %>% as.integer())
}