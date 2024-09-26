# post-covid-metfin

This is an old repo, superseded by this one: https://github.com/opensafely/metformin_covid !

[View on OpenSAFELY](https://jobs.opensafely.org/repo/https%253A%252F%252Fgithub.com%252Fopensafely%252Fpost-covid-metfin)

Recent evidence from a placebo-controlled randomised clinical trial (COVID-OUT: https://doi.org/10.1016/S1473-3099(23)00299-2) suggested that administering metformin during the acute stage of COVID-19 infection lowers the likelihood of developing Long COVID. Treatment with metformin reduced long COVID incidence by about 41%, with an absolute reduction of 4,1%, compared to placebo. This interesting finding warrants further investigation, and to guide treatment policy, effectiveness studies, especially from routine care data, are needed. 
The aim of this study is to, first, understand the prescription pattern and timing of metformin among people diagnosed with type 2 Diabetes Mellitus (T2DM) and pre-T2DM in OpenSAFELY. Second, to assess how many who are diagnosed with T2DM also have a recent COVID-19 infection. Third, to conceptualise and conduct a target trial emulation estimating the incidence of Long COVID among (at-risk of) T2DM patients treated with metformin vs no metformin.

Details of the purpose and any published outputs from this project can be found at the link above.

The contents of this repository MUST NOT be considered an accurate or valid representation of the study or its purpose. 
This repository may reflect an incomplete or incorrect analysis with no further ongoing work.
The content has ONLY been made public to support the OpenSAFELY [open science and transparency principles](https://www.opensafely.org/about/#contributing-to-best-practice-around-open-science) and to support the sharing of re-usable code for other subsequent users.
No clinical, policy or safety conclusions must be drawn from the contents of this repository.

# Data flow
1. Script data_process.R

* Import/Extract feather dataset from OpenSAFELY, function *extract_data*

* Process data, function *process_data*, incl. introducing the grace period, the period cuts, treatment and outcome (=status) variables

* Apply the quality assurance criteria, function *quality_assurance*

* Apply the eligibility criteria, function *calc_n_excluded*


2. Script select_and_simplify_data.R

a) Reduce/simplify to most important variables, function *simplify_data*:

* status_seq: outcome reached 

* treatment_seq: treated within grace period

* tb_postest_treat_seq: time between baseline and treatment (within grace period)

* fup_seq: time until outcome, or dereg, or max fup time (what if competing event? Or I want to censoring weights for dereg?)


3. Script prepare_data.R // use https://github.com/LindaNab/seqtrial/blob/main/vignettes/sequential-trial.Rmd instead!

* Split the data into regular interval within follow-up (tstart - tend intervals), function *split_data*, for each individual

* Add treatment lag variable, function *add_trt_lags*

* Construct trials, using function *construct_trials* and function *construct_trial_no*, according to grace period


4. Script describe_size_trials.R and all other descriptive and analyses scripts



# About the OpenSAFELY framework

The OpenSAFELY framework is a Trusted Research Environment (TRE) for electronic
health records research in the NHS, with a focus on public accountability and
research quality.

Read more at [OpenSAFELY.org](https://opensafely.org).

# Licences
As standard, research projects have a MIT license. 
