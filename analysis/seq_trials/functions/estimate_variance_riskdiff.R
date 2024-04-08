riskdiff_variance <- function(plrmod, vcov, newdata0, newdata1, id, trial, time, weights){
  
  # calculate variance of adjusted survival / adjusted cumulative incidence
  # needs plrmod object, cluster-vcov, input data, plrmod weights, and indices for patient and time
  
  tt <- terms(plrmod) # this helpfully grabs the correct spline basis from the plrmod, rather than recalculating based on `newdata`
  newdata0 <- newdata0 %>% arrange(.data[[id]], .data[[time]])
  newdata1 <- newdata1 %>% arrange(.data[[id]], .data[[time]])
  m.mat0 <- model.matrix(tt, data = newdata0)
  m.mat1 <- model.matrix(tt, data = newdata1)
  m.coef <- plrmod$coef
  
  N <- nrow(m.mat0)
  K <- length(m.coef)
  
  # log-odds, nu_t, at time t
  nu0 <- m.coef %*% t(m.mat0) # t_i x 1
  nu1 <- m.coef %*% t(m.mat1) # t_i x 1
  # part of partial derivative
  pdc0 <- (exp(nu0) / ((1 + exp(nu0))^2)) # t_i x 1
  pdc1 <- (exp(nu1) / ((1 + exp(nu1))^2)) # t_i x 1
  # summand for partial derivative of P_t(theta_t | X_t), for each time t and term k
  
  #summand <- crossprod(diag(as.vector(pdc0)), m.mat0)    # t_i  x k
  summand0 <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    summand0[,k] <- m.mat0[,k] * as.vector(pdc0)
  }
  summand1 <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    summand1[,k] <- m.mat1[,k] * as.vector(pdc1)
  }
  
  # cumulative sum of summand, by patient_id  # t_i x k
  cmlsum <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    cmlsum[,k] <- ave(summand1[,k] - summand0[,k], newdata0[[id]], newdata0[[trial]], FUN = cumsum)
  }
  
  ## multiply by plrmod weights (weights are normalised here so we can use `sum` later, not `weighted.mean`)
  normweights <- newdata0[[weights]] / ave(newdata0[[weights]], newdata0[[time]], FUN = sum) # t_i x 1
  
  #wgtcmlsum <- crossprod(diag(normweights), cmlsum ) # t_i x k
  wgtcmlsum <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    wgtcmlsum[,k] <- cmlsum[,k] * normweights
  }
  
  # partial derivative of cumulative incidence at t
  partial_derivative <- rowsum(wgtcmlsum, newdata0[[time]])
  
  variance <- rowSums(crossprod(t(partial_derivative), vcov) * partial_derivative) # t x 1
  
  variance
}