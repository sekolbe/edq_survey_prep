####################################################################################
# This script takes the raw graduate and supervisor survey data, prepares it to be 
# loaded to the staging database server, and saves it as 'staging data.'
#
# S. Kolbe
####################################################################################
#
# 2.  Create the staging graduate and supervisor survey response files:
#     a. Enter the setup values for date and file name, and whether you're running prep for graduate or supervisor responses.
#     b. Run the script. Read through the QA checks in the ouput and resolve any issues.
#     c. Load the staging data to the staging server (pwbmwsqldb02) using SSMS.
#     d. Ask CSUCO ITS to convert SSNs to calstateedupersonuids.
#
# 3. Work with CSU IT to load the data to the production server.
#
####################################################################################
# Set up
####################################################################################

library(dplyr)
library(magrittr)
library(RODBC)
library(stringr)
library(data.table)

# Update year and file base names annually
# Here, the year corresponds the survey administration school year, not completion cohort year
year <- "2020-2021"
target_group <- "graduate" # options are "graduate" or "supervisor"
raw_grad_base_name <- "2021_TeacherFullExport"
raw_sup_base_name <- "2021_SupervisorFullExport"

options(stringsAsFactors = FALSE)

####################################################################################
# Load data
####################################################################################

if (target_group == "graduate"){
  responses <- as.data.frame(read.csv(paste0("raw_data/", raw_grad_base_name, ".csv"), 
                                     colClasses = c("ssn"="character", 
                                                    "CSU.Other"="character", 
                                                    "gep.se.3"="character", 
                                                    "Ell.DK"="character")))
}

if (target_group == "supervisor"){
  responses <- as.data.frame(read.csv(paste0("raw_data/", raw_sup_base_name, ".csv"),
                                    colClasses = c("ssn"="character")))
}

rm(raw_grad_base_name, raw_sup_base_name)

####################################################################################
# Load survey metadata
####################################################################################

survey_items <- as.data.frame(read.delim("../Specs and Structure - do not touch/SURVEY_ITEMS.txt"))
survey_item_code_crosswalk <- select(survey_items, ITEM_CODE, VENDOR_CODE)

rm(survey_items)

####################################################################################
# Rename fields for consistency with the survey metadata
####################################################################################

# Check to make sure all field names in the results file have a corresponding code in the survey item table, then filter to only those survey items
setdiff(names(comp),survey_item_code_crosswalk$VENDOR_CODE)
survey_item_code_crosswalk <- filter(survey_item_code_crosswalk, VENDOR_CODE %in% names(responses))

# Rename fields. A departure from tidyverse to allow for renaming by vectors of names from the survey metadata since there are 100+ field names
setnames(responses, old = survey_item_code_crosswalk$VENDOR_CODE, new = survey_item_code_crosswalk$ITEM_CODE)

####################################################################################
# Data cleaning
# Most data cleaning will be done in the ETL process. A few small items are covered here
####################################################################################

# Remove all non-numeric characters from SSN fields
# Leave fields in character format to preserve leading zeros
responses$SSN_GRAD <-   responses$SSN_GRAD %>%
    gsub("[^0-9]", "", .) %>%
    str_pad(., 9, pad = "0")

# Make case consistent in CRED_EXAM field
responses$CRED_EXAM %<>% toupper(.)

# Remove tabs, returns, and other unexpected characters from open response fields
clean_open_response <- function(text_field){
  out_field <- gsub("\t", " ", text_field) %>%
    gsub("\r\n", " ", .) %>%
    gsub("\n", " ", .) %>%
    gsub("&", "and", .) %>%
    gsub('\"\"', "'", .) %>%
    gsub('"', "'", .) %>%
    gsub("n/a", "", .) %>%
    gsub("N/A", "", .) %>%
    gsub("\\[", "(", .) %>%
    gsub("\\]", ")", .) %>%
    gsub("\t", " ", .) %>%
    #gsub("?", "c", .) %>%
    #gsub("?", "e", .) %>%
    gsub("\\*", "-", .)
  return(out_field)
}


responses$MOST_EFFECTIVE_SKILL %<>% clean_open_response(.)
responses$MOST_SERIOUS_GAP %<>% clean_open_response(.)
responses$POSN_OTH %<>% clean_open_response(.)
responses$SUPERVISOR_POSN_OTH %<>% clean_open_response(.)
responses$MOST_VALUABLE %<>% clean_open_response(.)
responses$UGGESTED_CHANGE %<>% clean_open_response(.)
responses$GRAD_CAMPUS_OTH %<>% clean_open_response(.)

rm(clean_open_response)

# Recode CONTENT_DESC
recode_content_desc <- function(code_vector){
  desc_vector <- recode(code_vector,
                        `ENGL` = "English",
                        `SBS` = "Biology",
                        `GS` = "General Subjects",
                        `SIF` = "Foundational-Level General Science",
                        `SS` = "Social Science",
                        `FLS` = "Spanish",
                        `MATH` = "Math",
                        `SC` = "Chemistry",
                        `HS` = "Health Science",
                        `FM` = "Foundations of Math",
                        `MUSI` = "Music",
                        `SG` = "Geoscience",
                        `PE` = "Physical Education",
                        `ART` = "Art",
                        `AGRI` = "Agriculture",
                        `ECSE` = "Early Childhood Special Education",
                        `FLF` = "French",
                        `PHS` = "Physics Specialized",
                        `CHS` = "Chemistry Specialized",
                        `SP` = "Physics",
                        `FLM` = "Mandarin",
                        `FLJ` = "Japanese",
                        `BSS` = "Biology Specialized",
                        `GES` = "Geoscience Specialized",
                        `ITE` = "Industrial & Tech Ed.",
                        `SG SBS` = "Geoscience, Biology",
                        `SC SG` = "Chemistry, Geoscience",
                        `FLS FM` = "Spanish, Foundations of Math",
                        `FLAS` = "American Sign Language",
                        `OT` = "Other"
  )
}

responses$CONTENT_DESC <- recode_content_desc(responses$CONTENT_CODE)

rm(recode_content_desc)

####################################################################################
# Remove records for Fullerton advanced completers, if present in file. These don't get loaded to the data warehouse per Paul 9/13/20.
####################################################################################

responses <- filter(responses, CAMPUS_ID != 50)

####################################################################################
# Check max number of characters in each field. Note fields with more than 50 
# characters - these field require an extra step during upload to the CTQ database
####################################################################################

find_max_char <- function(vec){
  max(nchar(vec),na.rm=TRUE)
}

sort(apply(responses, 2, find_max_char))

rm(find_max_char)

####################################################################################
# Write files
####################################################################################

if (target_group == "graduate"){
  write.table(responses, paste0("temp-staging_data/GRADUATE_RESPONSES_", substr(year,6,9), ".txt"), row.names=FALSE, sep = "\t", quote=FALSE)
}
if (target_group == "supervisor"){
  write.table(responses, paste0("temp-staging_data/SUPERVISOR_RESPONSES_", substr(year,6,9), ".txt"), row.names=FALSE, sep = "\t", quote=FALSE)
}

####################################################################################
# Clean up
####################################################################################

rm(year, target_group, survey_item_code_crosswalk)
