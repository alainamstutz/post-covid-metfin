## ###########################################################

##  This script:
## - Contains a function that adds vars 'egfr' and 'ckd_rrt' to the extracted
## data.frame

## linda.nab@thedatalab.com - 20220527
## ###########################################################

# Load libraries & functions ---
library(dplyr)
source(here("analysis", "data_import", "functions", "kidney_functions.R"))

# Function --
## Arguments:
## data_extracted: data.frame with columns creatinine, creatinine_operator,
## sex and creatinine_age
## Output:
## data_extracted with 1 extra column:
add_kidney_vars_to_data <- function(data_extracted){
  data_extracted <- data_extracted %>%
    mutate(SCR_adj_ctv3 = creatinine_ctv3 / 88.4,
           SCR_adj_snomed = creatinine_snomed / 88.4,
           SCR_adj_short_snomed = creatinine_short_snomed / 88.4) %>% # divide by 88.4 (to convert umol/l to mg/dl))
    add_min_creatinine("ctv3") %>%
    add_max_creatinine("ctv3") %>%
    add_egfr("ctv3") %>%
    add_min_creatinine("snomed") %>%
    add_max_creatinine("snomed") %>%
    add_egfr("snomed") %>%
    add_min_creatinine("short_snomed") %>%
    add_max_creatinine("short_snomed") %>%
    add_egfr("short_snomed")
}
