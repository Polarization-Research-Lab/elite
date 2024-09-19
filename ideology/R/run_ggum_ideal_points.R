# Problems with this script (and setup more broadly):
# - dont save filenames with information that changes. For example, H118 refers to the 118th congress; everytime congress changes, we have to update all of these filenames
# - dont use RDS when all you're saving is a single dataframe; use csv so it can be accessed by other software
# - the mix of global and local functions is not ideal
# - some files are saved despite not being used at any other point. Id imagine this was for debugging purposes; but should be commented out or specified


##### Setup -----
## Load required packages
library(bggum)     ## For GGUM-MC3 sampler and related tools
library(lubridate)
library(MCMCpack)  ## For the CJR model sampler
library(wnominate) ## To estimate W-NOMINATE
library(oc)        ## For Poole's Optimal Classification
library(pROC)      ## For getting AUC (for appendix fit stats)
library(dplyr)     ## For data manipulation and summarization
library(tidyr)     ## For data reshaping
library(tibble)    ## For extra data manipulation functionality not in dplyr
library(parallel)  ## For running chains in parallel
# library(devtools)
## Source helper functions (documented in code/util.R)
source("functions.R")


## Ensure required directories exist


##### Prepare data -----
member_data = read.csv('../.tmp/voteview.csv')
house_members <- member_data[member_data$chamber == "House", ]
senate_members <- member_data[member_data$chamber == "Senate", ]

vote_data =  read.csv('../.tmp/votes.csv')
colnames(vote_data)[colnames(vote_data) == "X"] <- "id"
house_data <- vote_data[vote_data$chamber == "House", ]
senate_data <- vote_data[vote_data$chamber == "House", ]

#### HOUSE ####
## Create a dichotomous response matrix from the data on members' votes
responses   <- house_data %>%        ## First we dichotomize responses,
  mutate(                         ## with a "yea" vote being a "1",
    response = case_when(       ## "nay" being a "0", & everything
      cast_code == 1 ~ 1L,    ## else counted as missing. In theory
      cast_code == 6 ~ 0L,    ## we could leverage GGUM's polytomous
      TRUE ~ NA_integer_      ## nature & treat abstention, etc., as
    )                           ## substantively interesting responses,
  ) %>%                           ## but we leave that for the future.
  select(                         ## Now we need to move the data to "wide"
    icpsr, rollnumber, response ## form, where the rows represent MCs
  ) %>%                           ## and the columns represent roll call
  pivot_wider(                    ## votes. After eliminating the now
    names_from = "rollnumber",  ## superfluous columns of house_data,
    values_from = "response"    ## we do this via the relatively new
  ) %>%                           ## pivot_wider() function (see its
  column_to_rownames("icpsr") %>% ## vignette for details). Finally we fix
  as.matrix()                     ## row names and convert to matrix.
## Eliminate Amash
## Eliminate unanimous and lopsided votes and legislators with little data
lopsided  <- which(apply(responses, 2, is_lopsided))   ## find lopsided votes
if (length(lopsided) > 0) {
  responses <- responses[, -lopsided]  ## remove lopsided columns
}
few_votes <- which(apply(responses, 1, has_few_votes)) ## find MCs with > 90% missing votes
if (length(few_votes) > 0) {
  responses <- responses[-few_votes,] ## remove rows with more than 90% missing votes
}
unanimous <- which(apply(responses, 2, is_unanimous))  ## find unanimous votes
if (length(unanimous) > 0) {
  responses <- responses[, -unanimous]  ## remove unanimous columns
}

##### Obtain and summarize MC3-GGUM posterior samples -----
## Before any sampling, we define our prior over alpha (the descrimination
## parameter); while the default settings should work just fine for delta
## (the location parameters) and tau (the option threshold parameters),
## as there are some sharp cutpoints on these bills we could have censoring
## without a wider prior on alpha; we double the default range.
alpha_prior <- c(1.5, 1.5, 0.25, 8.00)
## Set seed for reproducibility
set.seed(42)
## Tune proposal densities
sds <- tune_proposals(responses, 5000, alpha_prior_params = alpha_prior)
## Tune temperature schedule
temps <- tune_temperatures(responses, 6, proposal_sds = sds,
                           alpha_prior_params = alpha_prior)
## Get posterior samples; we want at least two chains to assess convergence,
## so we'll obtain those chains in parallel to reduce computation time.
ggum_chains <- ggumMC3(data = responses,
          sample_iterations = 100000,
          burn_iterations = 10000,
          # sample_iterations = 1,
          # burn_iterations = 1,
          proposal_sds = sds,
          temps = temps,
          alpha_prior_params = alpha_prior)

saveRDS(ggum_chains, file = "../.tmp/output/H118-chains-house.rds")
## Post process to deal with reflection
aic <- which(rownames(responses) == "21726") ## Use Jayapal to identify
processed_ggum_chains <-  post_process(sample = ggum_chains,
                                       constraint = aic,
                                       expected_sign = "-")

## Summarise posterior
ggum_posterior_summary <- summary(processed_ggum_chains)
saveRDS(ggum_posterior_summary, file = "../.tmp/output/H118-ggum-post-summary-house.rds")

# retrieve estimates
house <- readRDS(file = "../.tmp/output/H118-ggum-post-summary-house.rds")

write.csv(house_members, 'HOUSESUMMARY.csv', row.names = FALSE)
write.csv(house_members, 'HOUSEMEMBERSTEXT.csv', row.names = FALSE)

house_members$ggum_dim1 <- house[["estimates"]][["theta"]]

saveRDS(house_members, file = "../.tmp/output/H118-estimates-house.rds")


#### SENATE ####
##### Prepare data -----
## Create a dichotomous response matrix from the data on members' votes
responses   <- senate_data %>%        ## First we dichotomize responses,
  mutate(                         ## with a "yea" vote being a "1",
    response = case_when(       ## "nay" being a "0", & everything
      cast_code == 1 ~ 1L,    ## else counted as missing. In theory
      cast_code == 6 ~ 0L,    ## we could leverage GGUM's polytomous
      TRUE ~ NA_integer_      ## nature & treat abstention, etc., as
    )                           ## substantively interesting responses,
  ) %>%                           ## but we leave that for the future.
  select(                         ## Now we need to move the data to "wide"
    icpsr, rollnumber, response ## form, where the rows represent MCs
  ) %>%                           ## and the columns represent roll call
  pivot_wider(                    ## votes. After eliminating the now
    names_from = "rollnumber",  ## superfluous columns of senate_data,
    values_from = "response"    ## we do this via the relatively new
  ) %>%                           ## pivot_wider() function (see its
  column_to_rownames("icpsr") %>% ## vignette for details). Finally we fix
  as.matrix()                     ## row names and convert to matrix.
## Eliminate Amash
## Eliminate unanimous and lopsided votes and legislators with little data
lopsided  <- which(apply(responses, 2, is_lopsided))   ## find lopsided votes
if (length(lopsided) > 0) {
  responses <- responses[, -lopsided]  ## remove lopsided columns
}
few_votes <- which(apply(responses, 1, has_few_votes)) ## find MCs with > 90% missing votes
if (length(few_votes) > 0) {
  responses <- responses[-few_votes,] ## remove rows with more than 90% missing votes
}
unanimous <- which(apply(responses, 2, is_unanimous))  ## find unanimous votes
if (length(unanimous) > 0) {
  responses <- responses[, -unanimous]  ## remove unanimous columns
}

##### Obtain and summarize MC3-GGUM posterior samples -----
## Before any sampling, we define our prior over alpha (the descrimination
## parameter); while the default settings should work just fine for delta
## (the location parameters) and tau (the option threshold parameters),
## as there are some sharp cutpoints on these bills we could have censoring
## without a wider prior on alpha; we double the default range.
alpha_prior <- c(1.5, 1.5, 0.25, 8.00)
## Set seed for reproducibility
set.seed(42)
## Tune proposal densities
sds <- tune_proposals(responses, 5000, alpha_prior_params = alpha_prior)
## Tune temperature schedule
temps <- tune_temperatures(responses, 6, proposal_sds = sds,
                           alpha_prior_params = alpha_prior)
## Get posterior samples; we want at least two chains to assess convergence,
## so we'll obtain those chains in parallel to reduce computation time.
ggum_chains <- ggumMC3(data = responses,
                       sample_iterations = 100000,
                       burn_iterations = 10000,
                       proposal_sds = sds,
                       temps = temps,
                       alpha_prior_params = alpha_prior)

saveRDS(ggum_chains, file = "../.tmp/output/H118-chains-senate.rds")
## Post process to deal with reflection
aic <- which(rownames(responses) == "41301") ## Use Warren to identify
processed_ggum_chains <-  post_process(sample = ggum_chains,
                                       constraint = aic,
                                       expected_sign = "-")

## Summarise posterior
ggum_posterior_summary <- summary(processed_ggum_chains)
saveRDS(ggum_posterior_summary, file = "../.tmp/output/H118-ggum-post-summary-senate.rds")

# retrieve estimates
senate <- readRDS(file = "../.tmp/output/H118-ggum-post-summary-senate.rds")
senate_members$ggum_dim1 <- senate[["estimates"]][["theta"]]

saveRDS(senate_members, file = "../.tmp/output/H118-estimates-senate.rds")
