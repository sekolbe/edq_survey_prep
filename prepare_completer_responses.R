####################################################################################
# This script takes the raw completer survey data, prepares it to be 
# loaded to the data warehouse, and saves it as 'staging data.'
#
# S. Kolbe
####################################################################################
# 
####################################################################################
# Instructions
####################################################################################
#
# To run this script:
# 
# 1.  Download the raw completer survey responses from the completer survey admin portal (https://www.csuexitsurvey.org/campusadmin/) from the 'System Exports' tab
#
# 2.  Create the staging completer survey response files:
#     a. Enter the desired values for start date, end date, and year in the script below based on the interval of interest.
#     b. Run the script. Read through the QA checks in the ouput and resolve any issues.
#     c. Load the staging data to the staging server (pwbmwsqldb02) using SSMS.
#
# 3. Work with CSU IT to load the data to the production server.
#
####################################################################################
# Set up
####################################################################################

library(dplyr)
library(magrittr)
library(data.table)

# In the year variable, "1" designates the first upload of the calendar year (usually for responses from January 1 through June 30 of that year),
# and "2" designates the second upload of the year (usually July 1 through Dec 31).
start_date <- as.Date("2021-07-01")
end_date <- as.Date("2021-12-31")
date_range <- paste0(start_date, " to ", end_date)
year <- "2021_2"
comp_base_name <- "CSU_SYSTEMWIDE"

options(stringsAsFactors = FALSE)

####################################################################################
# Load data to R
####################################################################################

# Survey responses
comp <- as.data.frame(read.csv(paste0("survey_data_raw/", comp_base_name, "_", year, ".csv")))

# Survey metadata
survey_items <- as.data.frame(read.delim("../Specs and Structure/SURVEY_ITEMS.txt"))
survey_form_item <- as.data.frame(read.delim("../Specs and Structure/SURVEY_FORM_ITEM_ASSOCIATION.txt"))
survey_form_item <- merge(survey_form_item, survey_items, by="ITEM_ID")
survey_item_code_crosswalk <- select(survey_items, ITEM_CODE, VENDOR_CODE)

rm(comp_base_name)

####################################################################################
# Rename fields for consistency with the survey metadata
####################################################################################

# Check to make sure all field names in the results file have a corresponding code in the survey item table, then filter to only those survey items
setdiff(names(comp),survey_item_code_crosswalk$VENDOR_CODE)
survey_item_code_crosswalk <- filter(survey_item_code_crosswalk, VENDOR_CODE %in% names(comp))

# Rename fields. A departure from tidyverse to allow for renaming by vectors of names from the survey metadata since there are 100+ field names
setnames(comp, old = survey_item_code_crosswalk$VENDOR_CODE, new = survey_item_code_crosswalk$ITEM_CODE)

####################################################################################
# Select records from the specified time frame
####################################################################################

comp %<>% filter(., ENTRY_DATE >= start_date & ENTRY_DATE <= end_date)

rm(start_date, end_date)

####################################################################################
# Data cleaning
# Most data cleaning will be done in the ETL process. A few small items are covered here
####################################################################################

# Remove tabs, returns, and other extra whitespace from open response fields
clean_open_response <- function(text_field){
  out_field <- gsub("\t", " ", text_field) %>%
    gsub("\r\n", " ", .) %>%
    gsub("&", "and", .) %>%
    gsub('\"\"', "'", .) %>%
    gsub('"', "'", .) %>%
    gsub("n/a", "", .) %>%
    gsub("N/A", "", .) %>%
    gsub("\\[", "(", .) %>%
    gsub("\\]", ")", .) %>%
    gsub("\\*", "-", .)
  return(out_field)
}
comp$COMP_MOST_VALUABLE %<>% clean_open_response(.)
comp$COMP_SUGGESTED_CHANGE %<>% clean_open_response(.)

rm(clean_open_response)

# Recode yes/no variables to 1/0
recode_yesno <- function(text_vector){
  print(sort(table(text_vector)))
  if (length(setdiff(unique(text_vector),(c(0,1)))) > 0){
    text_vector[text_vector == "Yes"] <- 1
    text_vector[text_vector == "No"] <- 0
    print(sort(table(text_vector)))
  }
  return(text_vector)
}

comp$COMP_SALARY %<>% recode_yesno(.)
comp$COMP_HISP_LAT_STATUS %<>% recode_yesno(.)

rm(recode_yesno)

# Recode text binary variables
# In the vendor file, the values for each race/eth field are blanks or the name of the group (e.g. "Black" and blank for the field for black race/eth). 
# Recoded here to 1 or 0 for each field with this source data issue.
recode_binary <- function(text_vector){
  print(sort(table(text_vector)))
  if (length(setdiff(unique(text_vector),(c(0,1)))) > 0){
    text_vector[nchar(text_vector) > 0] <- 1
    text_vector[nchar(text_vector) == 0] <- 0
    text_vector[is.na(text_vector)] <- 0
    text_vector <- as.numeric(text_vector)
    print(sort(table(text_vector)))
  }
  return(text_vector)
}

comp$COMP_RACE_ETH_AMER_INDIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_CHINESE %<>% recode_binary(.)
comp$COMP_RACE_ETH_JAPANESE %<>% recode_binary(.)
comp$COMP_RACE_ETH_KOREAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_VIETNAMESE %<>% recode_binary(.)
comp$COMP_RACE_ETH_ASIAN_INDIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_LAOTIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_CAMBODIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_FILIPINO %<>% recode_binary(.)
comp$COMP_RACE_ETH_HMONG %<>% recode_binary(.)
comp$COMP_RACE_ETH_OTH_ASIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_BLACK %<>% recode_binary(.)
comp$COMP_RACE_ETH_HAWAIIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_GUAMANIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_SAMOAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_TAHITIAN %<>% recode_binary(.)
comp$COMP_RACE_ETH_OTH_PACIFIC_ISLE %<>% recode_binary(.)
comp$COMP_RACE_ETH_WHITE %<>% recode_binary(.)

comp$COMP_CRED_MS %<>% recode_binary(.)
comp$COMP_CRED_SS %<>% recode_binary(.)
comp$COMP_CRED_ES %<>% recode_binary(.)
comp$COMP_CRED_SS_ENG %<>% recode_binary(.)
comp$COMP_CRED_SS_LOTE %<>% recode_binary(.)
comp$COMP_CRED_SS_ELD %<>% recode_binary(.)
comp$COMP_CRED_SS_MATH %<>% recode_binary(.)
comp$COMP_CRED_SS_MUSIC %<>% recode_binary(.)
comp$COMP_CRED_SS_ARTS %<>% recode_binary(.)
comp$COMP_CRED_SS_PE %<>% recode_binary(.)
comp$COMP_CRED_SS_SCI_FDN %<>% recode_binary(.)
comp$COMP_CRED_SS_MATH_FDN %<>% recode_binary(.)
comp$COMP_CRED_SS_SCI_BIO %<>% recode_binary(.)
comp$COMP_CRED_SS_SCI_PHYS %<>% recode_binary(.)
comp$COMP_CRED_SS_SCI_CHEM %<>% recode_binary(.)
comp$COMP_CRED_SS_SCI_GEO %<>% recode_binary(.)
comp$COMP_CRED_SS_HEALTH %<>% recode_binary(.)
comp$COMP_CRED_SS_SOC %<>% recode_binary(.)
comp$COMP_CRED_SS_AGRICULTURE %<>% recode_binary(.)
comp$COMP_CRED_SS_BUSINESS %<>% recode_binary(.)
comp$COMP_CRED_SS_HOME_ECON %<>% recode_binary(.)
comp$COMP_CRED_SS_TECH %<>% recode_binary(.)
comp$COMP_CRED_ES_MMD %<>% recode_binary(.)
comp$COMP_CRED_ES_MSD %<>% recode_binary(.)
comp$COMP_CRED_ES_DHH %<>% recode_binary(.)
comp$COMP_CRED_ES_PHI %<>% recode_binary(.)
comp$COMP_CRED_ES_VI %<>% recode_binary(.)
comp$COMP_CRED_ES_ECSE %<>% recode_binary(.)
comp$COMP_CRED_ES_LAD %<>% recode_binary(.)

comp$COMP_ACAD_CC %<>% recode_binary(.)
comp$COMP_ACAD_CSU %<>% recode_binary(.)
comp$COMP_ACAD_NON_CSU %<>% recode_binary(.)

comp$COMP_COOP_TEACH_OBS %<>% recode_binary(.)
comp$COMP_COOP_TEACH_ROLE_MODEL %<>% recode_binary(.)
comp$COMP_COOP_TEACH_EFFECTIVE %<>% recode_binary(.)
comp$COMP_COOP_TEACH_PLAN %<>% recode_binary(.)
comp$COMP_COOP_TEACH_FEEDBACK %<>% recode_binary(.)
comp$COMP_COOP_TEACH_REFLECT %<>% recode_binary(.)
comp$COMP_COOP_TEACH_PROBLEM_SOLVING %<>% recode_binary(.)
comp$COMP_COOP_TEACH_KNOWLEDGE %<>% recode_binary(.)

rm(recode_binary)

####################################################################################
# Create indicators for single-subject modules
# This should follow the same format as the main form indicators (COMP_CRED_ES, COMP_CRED_SS, COMP_CRED_MS)
####################################################################################

table(comp$COMP_CRED_ES)
table(comp$COMP_CRED_SS)
table(comp$COMP_CRED_MS)

comp$COMP_SSCRED_ENG <- 0
comp$COMP_SSCRED_MATH <- 0
comp$COMP_SSCRED_SCI <- 0
comp$COMP_SSCRED_SOC <- 0
comp$COMP_SSCRED_OTH <- 0

comp$COMP_SSCRED_ENG[comp$COMP_CRED_SS_ENG == 1] <- 1
table(comp$COMP_SSCRED_ENG)

comp$COMP_SSCRED_MATH[comp$COMP_CRED_SS_MATH == 1 | comp$COMP_CRED_SS_MATH_FDN == 1] <- 1
table(comp$COMP_SSCRED_MATH)

comp$COMP_SSCRED_SCI[ comp$COMP_CRED_SS_SCI_FDN == 1 | comp$COMP_CRED_SS_SCI_BIO == 1 | comp$COMP_CRED_SS_SCI_CHEM == 1 | 
                      comp$COMP_CRED_SS_SCI_GEO == 1 | comp$COMP_CRED_SS_SCI_PHYS == 1] <- 1
table(comp$COMP_SSCRED_SCI)

comp$COMP_SSCRED_SOC[comp$COMP_CRED_SS_SOC == 1] <- 1
table(comp$COMP_SSCRED_SOC)

comp$COMP_SSCRED_OTH[ comp$COMP_CRED_SS_LOTE==1 | comp$COMP_CRED_SS_ELD==1 |                                         
                      comp$COMP_CRED_SS_MUSIC==1 | comp$COMP_CRED_SS_ARTS==1 | comp$COMP_CRED_SS_PE==1 |                      
                      comp$COMP_CRED_SS_HEALTH==1 | comp$COMP_CRED_SS_AGRICULTURE==1 |             
                      comp$COMP_CRED_SS_BUSINESS==1 | comp$COMP_CRED_SS_HOME_ECON==1 | omp$COMP_CRED_SS_TECH==1] <- 1
table(comp$COMP_SSCRED_OTH)

####################################################################################
# Create a separate record for each survey form the respondent takes:
# There are separate completer survey forms for each credential (MS, ES, SS by subject).
# The survey is set up to allow respondents to answer all questions relevant to them if they
# completed more than one credential, and their responses are all combined into a single
# record in the file we receive from the vendor. 
# For the purposes of the dashboard, and the ETL logic that only retains questions relevant
# to a particular survey form, it's better to have a separate record for each survey form,
# with only the fields that are relevant to that survey form.
# This section splits the combined record into separate records for each survey form. Fields
# that are relevant to multiple survey forms appear in each record, and responses for fields 
# that are not relevant to a particular form are removed.
####################################################################################

msCred <- filter(comp, COMP_CRED_MS == 1) %>% mutate(COMP_SURVEY_CODE = "msCred")
esCred <- filter(comp, COMP_CRED_ES == 1) %>% mutate(COMP_SURVEY_CODE = "spedCred")
ssCred_eng <- filter(comp, COMP_CRED_SS == 1 & COMP_SSCRED_ENG == 1) %>% mutate(COMP_SURVEY_CODE = "ssCred-eng")
ssCred_math <- filter(comp, COMP_CRED_SS == 1 & COMP_SSCRED_MATH == 1) %>% mutate(COMP_SURVEY_CODE = "ssCred-math")
ssCred_sci <- filter(comp, COMP_CRED_SS == 1 & COMP_SSCRED_SCI == 1) %>% mutate(COMP_SURVEY_CODE = "ssCred-sci")
ssCred_soc <- filter(comp, COMP_CRED_SS == 1 & COMP_SSCRED_SOC == 1) %>% mutate(COMP_SURVEY_CODE = "ssCred-soc")
ssCred_oth <- filter(comp, COMP_CRED_SS == 1 & COMP_SSCRED_OTH == 1) %>% mutate(COMP_SURVEY_CODE = "ssCred-oth")

# Adjust each record so that only data for the questions that appeared on that record's survey are retained
adjust_records <- function(data){
  temp_form <- unique(data$COMP_SURVEY_CODE)
  temp_form_item <- filter(survey_form_item, FORM_ID %in% c(temp_form))
  data[,setdiff(names(data), c(temp_form_item$ITEM_CODE,"COMP_SURVEY_CODE"))] <- NA
  return(data)
}

msCred %<>% adjust_records(.)
esCred %<>% adjust_records(.)
ssCred_eng %<>% adjust_records(.)
ssCred_math %<>% adjust_records(.)
ssCred_sci %<>% adjust_records(.)
ssCred_soc %<>% adjust_records(.)
ssCred_oth %<>% adjust_records(.)
rm(adjust_records)

comp <- rbind(msCred, esCred, ssCred_eng, ssCred_math, ssCred_sci, ssCred_soc, ssCred_oth)

rm(msCred, esCred, ssCred_eng, ssCred_math, ssCred_sci, ssCred_soc, ssCred_oth)

####################################################################################
# Check max number of characters in each field. Note fields with more than 50 
# characters - these field require an extra step in SSMS during upload to the CTQ database
#
# The default field width is 50. If there are wider values, the import will fail with
# an error message.
####################################################################################

find_max_char <- function(vec){
  max(nchar(vec),na.rm=TRUE)
}
sort(apply(comp, 2, find_max_char))

rm(find_max_char)

####################################################################################
# Write files
####################################################################################

write.table(comp, paste0("temp-staging_data/COMPLETER_RESPONSES_", year, ".txt"), row.names=FALSE, sep = "\t", quote=FALSE)

####################################################################################
# Clean up
####################################################################################

rm(comp)
rm(year, date_range, survey_item_code_crosswalk)
rm(survey_form_item, survey_items)
