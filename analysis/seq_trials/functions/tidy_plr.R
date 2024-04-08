tidy_plr <- function(plrmod, 
                     vcov,
                     conf.int = TRUE,
                     conf.level = 0.95,
                     exponentiate = FALSE,
                     cluster){
  
  # create tidy dataframe for coefficients of pooled logistic regression
  # using robust standard errors
  robust <- 
    lmtest::coeftest(plrmod, vcov. = vcov) %>% 
    broom::tidy(conf.int = TRUE,
                conf.level = conf.level,
                exponentiate = exponentiate)
  
  robust %>%
    mutate(
      or = exp(estimate),
      or.ll = exp(conf.low),
      or.ul = exp(conf.high),
    )
}