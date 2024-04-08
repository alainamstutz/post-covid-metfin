# post-covid-metfin

[View on OpenSAFELY](https://jobs.opensafely.org/repo/https%253A%252F%252Fgithub.com%252Fopensafely%252Fpost-covid-metfin)

Recent evidence from a placebo-controlled randomised clinical trial (COVID-OUT: https://doi.org/10.1016/S1473-3099(23)00299-2) suggested that administering metformin during the acute stage of COVID-19 infection lowers the likelihood of developing Long COVID. Treatment with metformin reduced long COVID incidence by about 41%, with an absolute reduction of 4,1%, compared to placebo. This interesting finding warrants further investigation, and to guide treatment policy, effectiveness studies, especially from routine care data, are needed. 
The aim of this study is to, first, understand the prescription pattern and timing of metformin among people diagnosed with type 2 Diabetes Mellitus (T2DM) and pre-T2DM in OpenSAFELY. Second, to assess how many who are diagnosed with T2DM also have a recent COVID-19 infection. Third, to conceptualise and conduct a target trial emulation estimating the incidence of Long COVID among (at-risk of) T2DM patients treated with metformin vs no metformin.

Details of the purpose and any published outputs from this project can be found at the link above.

The contents of this repository MUST NOT be considered an accurate or valid representation of the study or its purpose. 
This repository may reflect an incomplete or incorrect analysis with no further ongoing work.
The content has ONLY been made public to support the OpenSAFELY [open science and transparency principles](https://www.opensafely.org/about/#contributing-to-best-practice-around-open-science) and to support the sharing of re-usable code for other subsequent users.
No clinical, policy or safety conclusions must be drawn from the contents of this repository.

# Data flow
1. data_process
\begin{itemize}
\item[(a)] Import/Extract feather dataset from OpenSAFELY, function \texttt{extract_data}
\item[(b)] Process data, function \texttt{process_data}, incl. introducing the grace period, the period cuts, treatment and outcome (=status) variables
\item[(c)] Apply the quality assurance criteria, function \texttt{quality_assurance}
\item[(d)] Apply the eligibility criteria, function \texttt{calc_n_excluded}
\end{itemize}

2. select_and_simplify_data
\begin{itemize}
\item[(a)] Reduce/simplify to most important variables, function \texttt{simplify_data}:
\item[(b)] status_seq: outcome reached (or max fup)
\item[(c)] treatment_seq: treated within grace period
\item[(d)] tb_postest_treat_seq: time between baseline and treatment (within grace period)
\item[(e)] fup_seq: time until outcome within max fup time
\end{itemize}

3. prepare_data
\begin{itemize}
\item[(a)] Split the data into regular interval within follow-up (tstart - tend intervals), function \texttt{split_data}, for each individual
\item[(b)] Add treatment lag variable, function \texttt{add_trt_lags}
\item[(c)] Construct trials, using function \texttt{construct_trials} and function \texttt{construct_trial_no}, according to grace period
\end{itemize}

# About the OpenSAFELY framework

The OpenSAFELY framework is a Trusted Research Environment (TRE) for electronic
health records research in the NHS, with a focus on public accountability and
research quality.

Read more at [OpenSAFELY.org](https://opensafely.org).

# Licences
As standard, research projects have a MIT license. 
