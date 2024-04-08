split_data <- function(data){
  data %>%
    tidyr::uncount(fup_seq, .remove = FALSE) %>%
    dplyr::mutate(tend = dplyr::row_number(),
                  tstart = tend - 1L,
                  status_seq = dplyr::if_else(tend == fup_seq, status_seq, 0L),
                  treatment_seq = dplyr::if_else(tstart == tb_postest_treat_seq | tstart > tb_postest_treat_seq, treatment_seq, 0L),
                  # treatment_seq_sotmol = dplyr::if_else(tstart == tb_postest_treat_seq_sotmol | tstart > tb_postest_treat_seq_sotmol, treatment_seq_sotmol, 0L)
                  ) %>%
    dplyr::select(- c(fup_seq, tb_postest_treat_seq,
                      # tb_postest_treat_seq_sotmol
                      )) %>%
    dplyr::relocate(patient_id, tstart, tend)
}
