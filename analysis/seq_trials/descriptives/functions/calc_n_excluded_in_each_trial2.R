calc_n_excluded_in_each_trial2 <- function(data){
  data %>%
    group_by(period) %>%
    summarise(n_total = n(),
              n_total_pax = sum(treatment_seq),
              # n_total_sotmol = sum(treatment_seq_sotmol),
              # n_sotmol0 = sum(treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol == 0),
              # n_sotmol1 = sum(treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol == 1),
              # n_sotmol2 = sum(treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol == 2),
              # n_sotmol3 = sum(treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol == 3),
              # n_sotmol4 = sum(treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol == 4),
              n_event0 = sum(status_seq == 1 & fup_seq == 0), # should be 0
              n_event1 = sum(status_seq == 1 & fup_seq == 1 &
                               !((treatment_seq == 1 & tb_postest_treat_seq < 1) |
                                   (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 1))),
              n_event2 = sum(status_seq == 1 & fup_seq == 2 &
                               !((treatment_seq == 1 & tb_postest_treat_seq < 2) |
                                   (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 2))),
              n_event3 = sum(status_seq == 1 & fup_seq == 3 &
                               !((treatment_seq == 1 & tb_postest_treat_seq < 3) |
                                   (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 3))),
              n_event4 = sum(status_seq == 1 & fup_seq == 4 &
                               !((treatment_seq == 1 & tb_postest_treat_seq < 4) |
                                   (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 4))),
              n_censor0 = sum(status_seq != 1 & fup_seq == 0),
              n_censor1 = sum(status_seq != 1 & fup_seq == 1 &
                                !((treatment_seq == 1 & tb_postest_treat_seq < 1) |
                                    (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 1))),
              n_censor2 = sum(status_seq != 1 & fup_seq == 2 &
                                !((treatment_seq == 1 & tb_postest_treat_seq < 2) |
                                    (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 2))),
              n_censor3 = sum(status_seq != 1 & fup_seq == 3 &
                                !((treatment_seq == 1 & tb_postest_treat_seq < 3) |
                                    (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 3))),
              n_censor4 = sum(status_seq != 1 & fup_seq == 4 &
                                !((treatment_seq == 1 & tb_postest_treat_seq < 4) |
                                    (treatment_seq_sotmol == 1 & tb_postest_treat_seq_sotmol < 4))),
    ) %>%
    ungroup() %>%
    pivot_longer(cols = starts_with(c("n_sotmol", "n_event", "n_censor")),
                 names_to = c(".value", "trial"),
                 names_pattern = "(n_.*)(.)") %>%
    relocate(trial, .after = period) %>%
    mutate(period = period %>% as.integer(),
           trial = factor(trial, levels = c("0", "1", "2", "3", "4")))
}
