############################################################
# This script tests the load of survey response tables
# to the CTQ database on pwbmwsqldb01 for the completer survey
#
# S. Kolbe
############################################################

############################################################
# Set up
############################################################

library(dplyr)
library(tidyr)
library(RODBC)

channel1 <- odbcDriverConnect("Driver={SQL Server};Server=pwbmwsqldb01;Trusted_Connection=Yes;uid=skolbe;Database=CTQ")

options(stringsAsFactors = FALSE)

years_in_range <- c("2014-2015","2015-2016","2016-2017","2017-2018", "2018-2019", "2019-2020", "2020-2021")

############################################################
# Load data
############################################################

counts <- sqlQuery(channel1, "select
                      COMPLETION_YEAR,
                      TARGET_GROUP,
                      CREDENTIAL_TYPE,
                      CREDENTIAL,
                      FORMS,
                      CTQ_CAMPUS_NM,
                      count(distinct SURVEY_RES_DTL_ID) as COUNT
                      from dbo.SURVEY_DASHBOARD_vw
                      group by COMPLETION_YEAR, TARGET_GROUP, CREDENTIAL_TYPE, CREDENTIAL, FORMS, CTQ_CAMPUS_NM") %>%
  arrange(., COMPLETION_YEAR, TARGET_GROUP, CTQ_CAMPUS_NM, CREDENTIAL_TYPE, CREDENTIAL) %>%
  filter(., !is.na(COMPLETION_YEAR)) %>%
  filter(., COMPLETION_YEAR %in% years_in_range) %>%
  reshape(data = ., direction = "wide", v.names = "COUNT", timevar = "COMPLETION_YEAR", 
          idvar = c("TARGET_GROUP","CREDENTIAL_TYPE","CREDENTIAL","FORMS","CTQ_CAMPUS_NM")) %>%
  arrange(., TARGET_GROUP, CTQ_CAMPUS_NM, CREDENTIAL_TYPE, CREDENTIAL) 

write.csv(counts, "QA/output/response counts by survey by year.csv", row.names = FALSE)

############################################################
# Test values
############################################################

sdata <- sqlQuery(channel1, "select *
                 from dbo.SURVEY_DASHBOARD_vw
                 where TARGET_GROUP = 'Completer'")

names(sdata)

check_data <- function(data, field){
  output <- data %>%
    group_by_(., "COMPLETION_YEAR", field) %>%
    summarise(., 
              COUNT = length(unique(SURVEY_RES_DTL_ID))) %>%
    pivot_wider(data = ., names_from = COMPLETION_YEAR, values_from = COUNT) %>%
    as.data.frame(.)
  return(output)
}

check_data(sdata, "TARGET_GROUP")
check_data(sdata, "FORMS")
check_data(sdata, "QUESTION_TEXT")
check_data(sdata, "ITEMS")
check_data(sdata, "ITEM_RESPONSE_NUM")
check_data(sdata, "RESPONSE_TYPE")
check_data(sdata, "FRAMEWORK_NAME")
check_data(sdata, "STANDARDS")
check_data(sdata, "CTQ_CAMPUS_NM")
check_data(sdata, "RACE")
check_data(sdata, "GENDER")
check_data(sdata, "CREDENTIAL_TYPE")
check_data(sdata, "CREDENTIAL")
check_data(sdata, "CRED_PROG_TYPE")
check_data(sdata, "COMP_PROGRAM_NAME")
check_data(sdata, "DISTRICT_NM")
check_data(sdata, "DOC_TYPE")

####################################################################################
# Clean up
####################################################################################

close(channel1)
rm(check_data, sdata, counts, years_in_range, channel1)
