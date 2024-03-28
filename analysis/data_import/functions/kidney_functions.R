## ###########################################################

##  This script:
## - Contains a function that is used to calculate egfr from creatinine levels
## - Contains a function that is used to categories patients to ckd/rrt

## linda.nab@thedatalab.com - 20220527
## ###########################################################

# Load libraries & functions ---
## function 'add_min_creatinine'
## Arguments:
## data: extracted data, with columns:
## SCR_adj: numeric, serum creatinine levels in mg/dl
## sex: factor ("F" or "M")
## Output:
## minimum of SCR_adj / k and 1 to the power of l (with k and l different for
## females and males, see function)
## note that equation for males is used if sex is missing, which is in this 
## study pop never the case (only people with non missing sex are included)
add_min_creatinine <- function(data,
                               codelist){
  SCR_var <- paste0("SCR_adj_", codelist)
  data <- 
    data %>%
    mutate("min_creat_{codelist}" := if_else(sex == "M" | is.na(sex),
                                             pmin(.data[[SCR_var]] / 0.9, 1) ^ -0.411,
                                             pmin(.data[[SCR_var]] / 0.7, 1) ^ -0.329))
}
## function 'add_max_creatinine'
## Arguments:
## data: extracted data, with columns:
## SCR_adj: numeric, serum creatinine levels in mg/dl
## sex: factor ("F" or "M")
## Output:
## maximum of SCR_adj / k and 1 to the power of -1.209 (with k different for
## males and females)
## note that equation for males is used if sex is missing, which is in this 
## study pop never the case (only people with non missing sex are included)
add_max_creatinine <- function(data,
                               codelist){
  SCR_var <- paste0("SCR_adj_", codelist)
  data <- 
    data %>%
    mutate("max_creat_{codelist}" := if_else(sex == "M" | is.na(sex),
                                             pmax(.data[[SCR_var]] / 0.9, 1) ^ -1.209,
                                             pmax(.data[[SCR_var]] / 0.7, 1) ^ -1.209))
}

# Function ---
## Function 'add_egfr' calculates estimated Glomerular Filtration Rate
## based on the ckd-epi formula
## see https://docs.google.com/document/d/1hi_lMyuAa23u1xXLULLMdAiymiPopPZrAtQCDzYtjtE/edit
## for logic
## Arguments:
## data: extracted_data, with columns:
## creatinine: numeric with creatinine level
## creatinine_operator: character with operator (None, >, <, >=, <=, ~)
## sex: factor ("F" or "M")
## creatinine_age: numeric, age at measurement of creatinine
## Output:
## egfr based on creatinine level
add_egfr <- function(data,
                     codelist = "ctv3"){
  creatinine_var <- paste0("creatinine_", codelist)
  SCR_var <- paste0("SCR_adj_", codelist)
  min_creat_var <- paste0("min_creat_", codelist)
  max_creat_var <- paste0("max_creat_", codelist)
  egfr_var <- paste0("egfr_", codelist)
  creatinine_operator <- paste0("creatinine_operator_", codelist)
  creatinine_age <- paste0("age_creatinine_", codelist)
  data <-
    data %>%
    mutate("egfr_{codelist}" := case_when(
      is.na(.data[[creatinine_var]]) ~ NA_real_,
      is.na(.data[[creatinine_var]]) ~ NA_real_,
      (!is.na(.data[[creatinine_operator]]) & .data[[creatinine_operator]] != "=") ~ NA_real_,
      .data[[creatinine_var]] < 20 | .data[[creatinine_var]] > 3000 ~ NA_real_,
      TRUE ~ (.data[[min_creat_var]] * .data[[max_creat_var]] * 141) * (0.993 ^ .data[[creatinine_age]])),
      "egfr_{codelist}" := if_else(!is.na(sex) & sex == "F",
                     1.018 * .data[[egfr_var]],
                     .data[[egfr_var]]))
}
## Function 'categorise_ckd_rrt' categorises an individual into one of the 
## following categories:
## No CKD or RRT; RRT (dialysis); RRT (transplant); CKD Stage 5; 
## CKD Stage 4; CKD Stage 3b; CKD Stage 3a
## first is checked if someone is on dialysis or has a kidney transplant (rrt),
## if not rrt, and egfr is missing --> No CKD or RRT
## if not rrt and egfr is not missing --> classify into stage 
## 5/4/3b/3a/No CKD or RRT
## Arguments:
## data: extracted_data, with columns:
## - egfr: numeric containing estimated Glomerular Filtration Rate
## - rrt_cat: numeric ("0"; "1"; "2")
## Output:
## character of one of the above described categories
# categorise_ckd_rrt <- function(data){
#   data <-
#     data %>% 
#     mutate(ckd_rrt = case_when(
#       rrt_cat == "1" ~ "RRT (dialysis)",
#       rrt_cat == "2" ~ "RRT (transplant)",
#       (is.na(egfr) & rrt_cat == "0") ~ "No CKD or RRT",
#       (egfr >= 0 & egfr < 15) ~ "Stage 5",
#       (egfr >= 15 & egfr < 30) ~ "Stage 4",
#       (egfr >= 30 & egfr < 45) ~ "Stage 3b",
#       (egfr >= 45 & egfr < 60) ~ "Stage 3a",
#       (egfr >= 60) ~ "No CKD or RRT")
#     )
# }