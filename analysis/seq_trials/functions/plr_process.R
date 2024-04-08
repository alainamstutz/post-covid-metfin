plr_process <- function(plrmod,
                        model,
                        cluster,
                        glance_plr,
                        tidy_plr){
  
  print(warnings())
  logoutput(
    glue("model{model} data size = ", plrmod$n),
    glue("model{model} memory usage = ", format(object.size(plrmod), units="GB", standard="SI", digits=3L)),
    glue("convergence status: ", plrmod$converged)
  )

  # cluster vcov
  vcov <- vcovCL(plrmod, cluster = cluster, fix = TRUE, type = "HC0")
  
  # one row glance of model
  glance <-
    glance_plr(plrmod) %>%
    tibble::add_column(
      model = model,
      convergence = plrmod$converged,
      ram = format(object.size(plrmod), units="GB", standard="SI", digits=3L),
      .before=1
    )
  
  # model coefficients in tibble
  tidy <- 
    broom.helpers::tidy_and_attach(
      plrmod,
      tidy_fun = tidy_plr,
      exponentiate = FALSE,
      cluster = cluster,
      vcov = vcov
    ) %>%
    tibble::add_column(
      model = model,
      .before = 1
    )
  
  tibble::lst(glance, tidy, vcov)
}