glance_plr <- function(plrmod){
  tibble(
    AIC = plrmod$aic,
    df.null = plrmod$df.null,
    df.residual = plrmod$df.residual,
    deviance = plrmod$deviance,
    null.deviance = plrmod$null.deviance,
    nobs = length(plrmod$y)
  )
}