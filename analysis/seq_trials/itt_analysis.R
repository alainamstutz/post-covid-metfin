################################################################################
#
# INTENTION TO TREAT ANALYSIS
# 
# 
# The output of this script is:
# csv file ./output/seq_trials/itt/itt_fit_*.csv
# where * is simple or interaction_*
################################################################################

################################################################################
# 0.0 Import libraries + functions
################################################################################
library(magrittr)
library(dplyr)
library(tidyr)
library(fs)
library(here)
library(purrr)
library(splines)
library(broom)
library(sandwich)
library(lmtest)
library(data.table)
library(optparse)
library(tictoc)
library(parglm)
library(glue)
source(here::here("lib", "design", "covars_seq_trials.R"))
source(here::here("analysis", "seq_trials", "functions", "glance_plr.R"))
source(here::here("analysis", "seq_trials", "functions", "tidy_plr.R"))
source(here::here("analysis", "seq_trials", "functions", "plr_process.R"))

################################################################################
# 0.1 Create directories for output
################################################################################
output_dir <- here::here("output", "seq_trials", "itt")
fs::dir_create(output_dir)
log_dir <- here::here("output", "seq_trials", "itt", "log")
fs::dir_create(log_dir)

################################################################################
# 0.2 Import command-line arguments
################################################################################
args <- commandArgs(trailingOnly=TRUE)
if(length(args)==0){
  # use for interactive testing
  model <- "simple"
} else {
  
  option_list <- list(
    make_option("--model", type = "character", default = "simple",
                help = "Outcome model fitted. Choice between simple (main effects arm, period and trial), interaction_period (introducing interaction between arm and period), interaction_trial (introducing interaction between arm and trial), interaction_all (introducing interaction between arm and period; arm and trial), crude, crude_period and crude_trial [default %simple]. ",
                metavar = "model")
  )
  
  opt_parser <- OptionParser(usage = "prepare_data:[version] [options]", option_list = option_list)
  opt <- parse_args(opt_parser)
  
  model <- opt$model
}
## create special log file ----
cat(glue("## script info for the itt analysis, using model: {model} ##"), 
    "  \n", 
    file = fs::path(log_dir, glue("itt_log_{model}.txt")), append = FALSE)
## function to pass additional log text
logoutput <- function(...){
  cat(..., file = fs::path(log_dir, glue("itt_log_{model}.txt")), sep = "\n  ", append = TRUE)
}

################################################################################
# 0.3 Import data
################################################################################
trials <- arrow::read_feather(here("output", "data", "data_seq_trials_monthly.feather"))

################################################################################
# 1.0 Outcome model
################################################################################
if (model == "simple"){
  f <- 
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + period + trial", covars),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "interaction_period"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + period + trial + arm:period", covars),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "interaction_trial"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + period + trial + arm:trial", covars),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "interaction_all"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + period + trial + arm:period + arm:trial", covars),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "crude"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4)"),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "crude_period"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + period"),
                  collapse = " + ")) %>%  
    as.formula()
} else if (model == "crude_trial"){
  f <-
    paste0("status_seq ~ ",
           paste0(c("arm + ns(tend, 4) + trial"),
                  collapse = " + ")) %>%  
    as.formula()
}
# Settings for parglm fitting
parglm_control <-
  parglm.control(maxit = 40,
                 nthreads = 4)

print("Fit outcome model")
tic()
om_fit <-
  parglm(f, 
         family = binomial(link = "logit"),
         data = trials,
         control = parglm_control,
         na.action = "na.fail",
         model = FALSE)
om_fit$data <- NULL
toc()
print("Process outcome model")
tic()
om_processed <-
  plr_process(
    plrmod = om_fit,
    model = model,
    cluster = ~ patient_id:tend + patient_id:trial,
    glance_plr,
    tidy_plr
  )
toc()

################################################################################
# 2.0 Save output
################################################################################
print("Write output")
tic()
saveRDS(
  om_fit,
  fs::path(output_dir, paste0("itt_fit_", model, ".rds"))
)
saveRDS(
  om_processed$vcov,
  fs::path(output_dir, paste0("itt_vcov_", model, ".rds"))
)
data.table::fwrite(
  om_processed$tidy,
  fs::path(output_dir, paste0("itt_fit_", model, ".csv"))
)
data.table::fwrite(
  om_processed$glance,
  fs::path(output_dir, paste0("itt_glance_", model, ".csv"))
)
toc()
