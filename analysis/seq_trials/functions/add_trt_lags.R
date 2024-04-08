add_trt_lags <- function(data){
  data %>%
    mutate(treatment_seq_lag1 = lag(treatment_seq, n = 1, default = 0),
           treatment_seq_lag2 = lag(treatment_seq, n = 2, default = 0),
           treatment_seq_lag3 = lag(treatment_seq, n = 3, default = 0),
           treatment_seq_lag4 = lag(treatment_seq, n = 4, default = 0),
           treatment_seq_lag5 = lag(treatment_seq, n = 5, default = 0),
           treatment_seq_lag6 = lag(treatment_seq, n = 6, default = 0),
           treatment_seq_lag7 = lag(treatment_seq, n = 7, default = 0),
           # treatment_seq_sotmol_lag1 = lag(treatment_seq_sotmol, n = 1, default = 0),
           # treatment_seq_sotmol_lag2 = lag(treatment_seq_sotmol, n = 2, default = 0),
           # treatment_seq_sotmol_lag3 = lag(treatment_seq_sotmol, n = 3, default = 0),
           # treatment_seq_sotmol_lag4 = lag(treatment_seq_sotmol, n = 4, default = 0),
           # treatment_seq_sotmol_lag5 = lag(treatment_seq_sotmol, n = 5, default = 0)
           )
}
