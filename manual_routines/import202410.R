#Relational database import from cleaned Qualtrics export, 2024-10-03
#Focus on including the follow-up data, but may also go through baseline data yet again.
#Johan Zvrskovec


#libraries
library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)
library(phenodbr)
library(data.table)



#settings and configuration
qCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/latest_freeze",mustWork = T)

cognitronCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/cognitron",mustWork = T)

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ADDITIONAL_202401")
folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")

followupCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/joined",mustWork = T)

cohortSetting <- "covidcns"
cohortInstanceSetting <- "2023"

cinfo<-c()
cinfo$pw <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")
cinfo$host<-"localhost"
cinfo$dbname<-"phenodb"
cinfo$user<-"tng2101"
cinfo$port<-65432 #65432 for remote


dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
#dbutil$setSearchPath.standard()

#Test of overlap between the baseline cleaned and the joined cleaned data
# dem1<-readRDS(file.path(qCleanedFolderPath,"baseline","dem_covidcns_clean.rds"))
# nrow(dem1)
# dem2<-readRDS(file.path(followupCleanedFolderPath,"covidcns_baseline.rds"))
# nrow(dem2)
# dJoined<-readRDS(file.path(followupCleanedFolderPath,"covidcns_data_joined.rds"))
# nrow(dJoined)

#Follow-up data


allFollowup1<-as.data.frame(readRDS(file.path(followupCleanedFolderPath,"covidcns_followup1_joined.rds")))
allFollowup2<-as.data.frame(readRDS(file.path(followupCleanedFolderPath,"covidcns_followup2_joined.rds")))


#covidcnsals
##followup1
sCols<-c("ID","startDate","endDate",grep(pattern = "alsfrs.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "alsfrs\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = F,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsals",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("handwritingnoticedchange","handwritingchangenoticed")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "alsfrs\\.",suffixesToExcludeRegex = "_followup1", deitemise = T, parseAndBusiness = F)
##followup2
sCols<-c("ID","startDate","endDate",grep(pattern = "alsfrs.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "alsfrs\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = F,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsals",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("handwritingnoticedchange","handwritingchangenoticed")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "alsfrs\\.",suffixesToExcludeRegex = "_followup2", deitemise = T, parseAndBusiness = F)
##check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsals",assessmentVersionCode = "1")


#


# #Baseline
#
# ##als_covidcns_clean
# dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","als_covidcns_clean.rds"))
# head(dbutil$importDataDf)
# colnames(dbutil$importDataDf)
# dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = "als\\.", deitemise = T)
# dim(dbutil$importDataDf)
