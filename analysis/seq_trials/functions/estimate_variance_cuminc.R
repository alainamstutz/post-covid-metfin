cuminc_variance <- function(plrmod, vcov, newdata, id, trial, time, weights){
  
  # calculate variance of adjusted survival / adjusted cumulative incidence
  # needs plrmod object, cluster-vcov, input data, plrmod weights, and indices for patient and time
  tt <- terms(plrmod) # this helpfully grabs the correct spline basis from the plrmod, rather than recalculating based on `newdata`
  #Terms <- delete.response(tt) # not sure if needed?
  newdata <- newdata %>% arrange(.data[[id]], .data[[time]])
  m.mat <- model.matrix(tt, data = newdata)
  m.coef <- plrmod$coef
  
  N <- nrow(m.mat)
  K <- length(m.coef)
  
  # log-odds, nu_t, at time t
  nu <- m.coef %*% t(m.mat) # t_i x 1
  # part of partial derivative
  pdc <- (exp(nu) / ((1 + exp(nu))^2)) # t_i x 1
  # summand for partial derivative of P_t(theta_t | X_t), for each time t and term k
  
  #summand <- crossprod(diag(as.vector(pdc)), m.mat)    # t_i  x k
  summand <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    summand[,k] <- m.mat[,k] * as.vector(pdc)
  }
  
  # cumulative sum of summand, by patient_id  # t_i x k
  cmlsum <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    cmlsum[,k] <- ave(summand[,k], 
                      newdata[[id]], 
                      newdata[[trial]], 
                      FUN = cumsum)
  }
  
  ## multiply by plrmod weights (weights are normalised here so we can use `sum` later, not `weighted.mean`)
  normweights <- newdata[[weights]] / ave(newdata[[weights]], newdata[[time]], FUN = sum) # t_i x 1
  
  # wgtcmlsum <- crossprod(diag(normweights), cmlsum ) # t_i x k
  wgtcmlsum <- matrix(0, nrow = N, ncol = K)
  for (k in seq_len(K)){
    wgtcmlsum[,k] <- cmlsum[,k] * normweights
  }
  
  # partial derivative of cumulative incidence at t
  
  partial_derivative <- rowsum(wgtcmlsum, newdata[[time]])
  
  variance <- rowSums(crossprod(t(partial_derivative), vcov) * partial_derivative) # t x 1
  
  variance
}