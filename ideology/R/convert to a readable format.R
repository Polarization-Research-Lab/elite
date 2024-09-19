# For some annoying reason, RDS was chosen as the save format when the .rds files are just dataframes. It should have be saved as a CSV, so literally any other tool can access it.

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

house_members = readRDS(file = "../.tmp/output/H118-estimates-house.rds")
senate_members = readRDS(file = "../.tmp/output/H118-estimates-senate.rds")

write.csv(house_members, file = "../.tmp/output/house-ideology-estimates.csv")
write.csv(senate_members, file = "../.tmp/output/senate-ideology-estimates.csv")




