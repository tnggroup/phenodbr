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
#allJoined<-as.data.frame(readRDS(file.path(followupCleanedFolderPath,"covidcns_data_joined.rds")))

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
###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsals",assessmentVersionCode = "1", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$importDataDf)
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
###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsals",assessmentVersionCode = "1", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)


#import comments
#handwritingchangenoticed has changed to handwritingnoticedchange in variable naming


#cfq11
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "cfs.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "followup1",  prefixesToExcludeRegex = "cfs\\.",suffixesToExcludeRegex = "_followup1", deitemise = T, prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "cfq11",assessmentVersionCode = "covidcns")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataMeta())
#View(dbutil$selectImportDataAssessmentItemVariableStats())
dbutil$renameAnnotatedVariableColumn("doyouhaveproblemswithtiredness","tirednessproblemsmonth")
dbutil$renameAnnotatedVariableColumn("doyouneedtorestmore","restmonth")
dbutil$renameAnnotatedVariableColumn("doyoufeelsleepyordrowsy","feelsleepydrowsymonth")
dbutil$renameAnnotatedVariableColumn("oyouhaveproblemsstartingthings","problemsstartingthingsmonth")
dbutil$renameAnnotatedVariableColumn("doyoulackenergy","lackenergymonth")
dbutil$renameAnnotatedVariableColumn("strengthmuscles","musclesstrengthmonth")
dbutil$renameAnnotatedVariableColumn("doyoufeelweak","feelweakweek")
dbutil$renameAnnotatedVariableColumn("youhavedifficultyconcentrating","difficultyconcentratingmonth")
dbutil$renameAnnotatedVariableColumn("makeslipstonguespeaking","slipsspeakingtonguemonth")
dbutil$renameAnnotatedVariableColumn("correctwordfinddifficult","correctworddifficultfind")
dbutil$renameAnnotatedVariableColumn("howisyourmemory","memorymonth")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "followup1",  prefixesToExcludeRegex = "cfs\\.",suffixesToExcludeRegex = "_followup1", deitemise = T, parseAndBusiness = F, doAnnotate = F)
###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "cfq11",assessmentVersionCode = "covidcns", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$importDataDf)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "cfs.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "followup2",  prefixesToExcludeRegex = "cfs\\.",suffixesToExcludeRegex = "_followup2", deitemise = T, prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "cfq11",assessmentVersionCode = "covidcns")
dbutil$compareColumnFormatWithSelectedExportData()
cat("Total number of columns: ",ncol(dbutil$importDataDf))
cat("Number of annotated columns:",sum(dbutil$selectImportDataAssessmentItemVariableStats()$annotated))
#View(dbutil$selectImportDataMeta())
#View(dbutil$selectImportDataAssessmentItemVariableStats())
dbutil$renameAnnotatedVariableColumn("doyouhaveproblemswithtiredness","tirednessproblemsmonth")
dbutil$renameAnnotatedVariableColumn("doyouneedtorestmore","restmonth")
dbutil$renameAnnotatedVariableColumn("doyoufeelsleepyordrowsy","feelsleepydrowsymonth")
dbutil$renameAnnotatedVariableColumn("oyouhaveproblemsstartingthings","problemsstartingthingsmonth")
dbutil$renameAnnotatedVariableColumn("doyoulackenergy","lackenergymonth")
dbutil$renameAnnotatedVariableColumn("strengthmuscles","musclesstrengthmonth")
dbutil$renameAnnotatedVariableColumn("doyoufeelweak","feelweakweek")
dbutil$renameAnnotatedVariableColumn("youhavedifficultyconcentrating","difficultyconcentratingmonth")
dbutil$renameAnnotatedVariableColumn("makeslipstonguespeaking","slipsspeakingtonguemonth")
dbutil$renameAnnotatedVariableColumn("correctwordfinddifficult","correctworddifficultfind")
dbutil$renameAnnotatedVariableColumn("howisyourmemory","memorymonth")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "followup2",  prefixesToExcludeRegex = "cfs\\.",suffixesToExcludeRegex = "_followup2", deitemise = T, parseAndBusiness = F, doAnnotate = F)
##check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "cfq11",assessmentVersionCode = "covidcns", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)

#import comments
# COGTEST-participants imported from this data (for example COGTEST1)
# ALL variable/column names had changed names in the follow-up data


#csri, not documented what this instrument is.
#mixed integer and text variables
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "csri.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "csri", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "csri\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
#set correct types
#View(dbutil$variableAnnotationDf)
dbutil$variableAnnotationDf[!dbutil$variableAnnotationDf$column_name %in% c("startdate","enddate","howmanytimes"),]$variable_data_type<-NA #i.e. string
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "csri", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "csri.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "csri", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "csri\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "csri",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
#set correct types
#View(dbutil$variableAnnotationDf)
dbutil$variableAnnotationDf[!dbutil$variableAnnotationDf$column_name %in% c("startdate","enddate","howmanytimes"),]$variable_data_type<-NA #i.e. string
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "csri", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "csri",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary
# most variables have text entered mixed with numbers



#dem
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "dem.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "dem\\.",suffixesToExcludeRegex = "_followup1", deitemise = T, prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = T)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsdem",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "dem\\.",suffixesToExcludeRegex = "_followup1", deitemise = T, parseAndBusiness = F, doAnnotate = T)
###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsdem",assessmentVersionCode = "1", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$importDataDf)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "dem.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "dem\\.",suffixesToExcludeRegex = "_followup2", deitemise = T, prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = T)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsdem",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "dem\\.",suffixesToExcludeRegex = "_followup2", deitemise = T, parseAndBusiness = F, doAnnotate = T)
###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsdem",assessmentVersionCode = "1", joinSec = T)
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)

#import comments
# only has the item whereareyoucurrentlyliving


#gad7
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "gad7.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "gad7", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "gad7\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "gad7",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
dbutil$renameAnnotatedVariableColumn("feelingnervousanxiousoronedge","tfeelingnervousanxiousoronedge",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("controlworryingstop","ingabletostoporcontrolworrying",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("ingtoomuchaboutdifferentthings","ingtoomuchaboutdifferentthings",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("troublerelaxing","tstatementspasttroublerelaxing",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("sithardrestless","restlessthatitishardtositstill",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("comingeasilyannoyedorirritable","comingeasilyannoyedorirritable",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("feelingafraidawfulhappen","dasifsomethingawfulmighthappen",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$createBasicItemAnnotationFromVariableAnnotation()
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "gad7", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "gad7.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "gad7", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "gad7\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "gad7",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
dbutil$renameAnnotatedVariableColumn("feelingnervousanxiousoronedge","tfeelingnervousanxiousoronedge",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("controlworryingstop","ingabletostoporcontrolworrying",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("ingtoomuchaboutdifferentthings","ingtoomuchaboutdifferentthings",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("troublerelaxing","tstatementspasttroublerelaxing",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("sithardrestless","restlessthatitishardtositstill",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("comingeasilyannoyedorirritable","comingeasilyannoyedorirritable",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$renameAnnotatedVariableColumn("feelingafraidawfulhappen","dasifsomethingawfulmighthappen",forceItem = "past2weeksoftenbotheredfollowi")
dbutil$createBasicItemAnnotationFromVariableAnnotation()
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "gad7", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "gad7",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# COGTEST-participants imported from this data (for example COGTEST1)
# ALL variable/column names had changed names in the follow-up data


#lang - not documented
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "lang.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "lang", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "lang\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "lang", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "lang.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "lang", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "lang\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "lang", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "lang",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary


#menopause - not documented
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "menopause.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "menopause", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "menopause\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "menopause", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "menopause.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "menopause", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "menopause\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "menopause", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "menopause",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary


#covidcnsmigraine
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "migraine.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsmigraine", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "migraine\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsmigraine",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("sufferedheadachespastmonths","headachessufferedpastmonth",forceItem = "headachessufferedpastmonth")
dbutil$variableAnnotationDf[dbutil$variableAnnotationDf$column_name=="missworkschoolheadaches",c("variable_data_type")]<-"integer"
dbutil$variableAnnotationDf[dbutil$variableAnnotationDf$column_name=="schoolreducedhalfproductivity",c("variable_data_type")]<-"integer"
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsmigraine", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "migraine.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsmigraine", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "migraine\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsmigraine",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("sufferedheadachespastmonths","headachessufferedpastmonth",forceItem = "headachessufferedpastmonth")
dbutil$variableAnnotationDf[dbutil$variableAnnotationDf$column_name=="missworkschoolheadaches",c("variable_data_type")]<-"integer"
dbutil$variableAnnotationDf[dbutil$variableAnnotationDf$column_name=="schoolreducedhalfproductivity",c("variable_data_type")]<-"integer"
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsmigraine", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnsmigraine",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# only one variable maps on to the baseline questionnaire


#nle
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "nle.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "nle", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "nle\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "nle", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "nle.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "nle", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "nle\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "nle", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "nle",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary


#pad
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "pad.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pad", assessmentVersionCode = "covidcns", stageCode = "followup1",  prefixesToExcludeRegex = "pad\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pad", assessmentVersionCode = "covidcns", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "pad.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pad", assessmentVersionCode = "covidcns", stageCode = "followup2",  prefixesToExcludeRegex = "pad\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
# View(dbutil$selectImportDataAssessmentItemVariableStats())
# View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pad", assessmentVersionCode = "covidcns", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "pad",assessmentVersionCode = "covidcns", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary

#pcl
##pcl5_covidcns_clean - let's re-import the baseline data as the item mapping was not the best
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","pcl5_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("pcl5\\.","pcl5\\.\\."), deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F,parseItemsFromVariableLabelText = F)
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("pcl5\\.","pcl5\\.\\."), parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
dim(dbutil$importDataDf)
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "pcl.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "pcl\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "pcl5",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
dbutil$renameAnnotatedVariableColumn("imagespastrepeateddisturbing","moriesofthestressfulexperience",forceItem = "moriesofthestressfulexperience")
dbutil$renameAnnotatedVariableColumn("upsetremindedfeelingpast","dedyouofthestressfulexperie001",forceItem = "dedyouofthestressfulexperie001")
dbutil$renameAnnotatedVariableColumn("ngactivitiesremindedsituations","indersofthestressfulexperience",forceItem = "indersofthestressfulexperience") #documentation updated to reflect the question formulation for the follow-up questions
dbutil$renameAnnotatedVariableColumn("cutpeoplefeelingdistant","antorcutofffromfromotherpeople",forceItem = "antorcutofffromfromotherpeople")
dbutil$renameAnnotatedVariableColumn("rritableorhavingangryoutbursts","ryoutburstoractingaggressively",forceItem = "ryoutburstoractingaggressively")
dbutil$renameAnnotatedVariableColumn("difficultyconcentrating","dhavingdifficultyconcentrating",forceItem = "dhavingdifficultyconcentrating")
dbutil$createBasicItemAnnotationFromVariableAnnotation()
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "pcl.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "pcl\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "pcl5",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
dbutil$renameAnnotatedVariableColumn("imagespastrepeateddisturbing","moriesofthestressfulexperience",forceItem = "moriesofthestressfulexperience")
dbutil$renameAnnotatedVariableColumn("upsetremindedfeelingpast","dedyouofthestressfulexperie001",forceItem = "dedyouofthestressfulexperie001")
dbutil$renameAnnotatedVariableColumn("ngactivitiesremindedsituations","indersofthestressfulexperience",forceItem = "indersofthestressfulexperience") #documentation updated to reflect the question formulation for the follow-up questions
dbutil$renameAnnotatedVariableColumn("cutpeoplefeelingdistant","antorcutofffromfromotherpeople",forceItem = "antorcutofffromfromotherpeople")
dbutil$renameAnnotatedVariableColumn("rritableorhavingangryoutbursts","ryoutburstoractingaggressively",forceItem = "ryoutburstoractingaggressively")
dbutil$renameAnnotatedVariableColumn("difficultyconcentrating","dhavingdifficultyconcentrating",forceItem = "dhavingdifficultyconcentrating")
dbutil$createBasicItemAnnotationFromVariableAnnotation()
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "pcl5",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# baseline was re-imported again for better item mapping
# SOME variable/column names in the follow up data were mapped onto the baseline items and had changed names in the follow-up data
# Texts were different in the follow-up data for some items. When this was extreme, it was commented on in the item documentation field.


#psm
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "psm.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psm", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "psm\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psm", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)
##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "psm.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psm", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "psm\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psm", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "psm",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary


#psq
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "psq.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psq", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "psq\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "psq",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("breakendhappydays","breakendhappyfelt",forceItem = "breakendhappyfelt")
#dbutil$renameAnnotatedVariableColumn("wasthereanobviousreasonforthis","wasthereanobviousreasonforthis",forceItem = "wasthereanobviousreasonforthis")
#dbutil$renameAnnotatedVariableColumn("complainstrangepeople","complainstrangepeople",forceItem = "complainstrangepeople")
dbutil$renameAnnotatedVariableColumn("forcepersoncontrolledthoughts","controlledforcepersonthoughts",forceItem = "controlledforcepersonthoughts")
#dbutil$renameAnnotatedVariableColumn("findhardinstancetelepathy","findhardinstancetelepathy",forceItem = "findhardinstancetelepathy")
dbutil$renameAnnotatedVariableColumn("feltpeopletimespast","felttimespeoplepast",forceItem = "felttimespeoplepast")
dbutil$renameAnnotatedVariableColumn("harminterestsfeltpeople","interestsharmfelttimes",forceItem = "interestsharmfelttimes")

#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psq", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "psq.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psq", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "psq\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "psq",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("breakendhappydays","breakendhappyfelt",forceItem = "breakendhappyfelt")
#dbutil$renameAnnotatedVariableColumn("wasthereanobviousreasonforthis","wasthereanobviousreasonforthis",forceItem = "wasthereanobviousreasonforthis")
#dbutil$renameAnnotatedVariableColumn("complainstrangepeople","complainstrangepeople",forceItem = "complainstrangepeople")
dbutil$renameAnnotatedVariableColumn("forcepersoncontrolledthoughts","controlledforcepersonthoughts",forceItem = "controlledforcepersonthoughts")
#dbutil$renameAnnotatedVariableColumn("findhardinstancetelepathy","findhardinstancetelepathy",forceItem = "findhardinstancetelepathy")
dbutil$renameAnnotatedVariableColumn("feltpeopletimespast","felttimespeoplepast",forceItem = "felttimespeoplepast")
dbutil$renameAnnotatedVariableColumn("harminterestsfeltpeople","interestsharmfelttimes",forceItem = "interestsharmfelttimes")
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "psq", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "psq",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# MOST variable/column names in the follow up data were mapped onto the baseline items and SOME had changed names in the follow-up data
#there was one new variable compared to baseline that did not map on to a baseline variable


#ses
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "ses.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "ses", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "ses\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "ses", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "ses.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "ses", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "ses\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F, parseItemsFromVariableLabelText = F)
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "ses", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "ses",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# this instrument was not documented or part of any extraction data dictionary


#smell
##followup1
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "smell.",x = colnames(allFollowup1), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup1[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnssmelltaste", assessmentVersionCode = "1", stageCode = "followup1",  prefixesToExcludeRegex = "smell\\.",suffixesToExcludeRegex = "_followup1", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnssmelltaste",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("tastesmelllosssevere","howsevereiswastheloss",forceItem = "howsevereiswastheloss")
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnssmelltaste", assessmentVersionCode = "1", stageCode = "followup1", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

##followup2
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
sCols<-c("ID","startDate","endDate",grep(pattern = "smell.",x = colnames(allFollowup2), fixed = T,value = T))
dbutil$readImportData(dataframe = allFollowup2[,sCols])
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnssmelltaste", assessmentVersionCode = "1", stageCode = "followup2",  prefixesToExcludeRegex = "smell\\.",suffixesToExcludeRegex = "_followup2", deitemise = T,prepare = T,import = F,doAnnotate = F,doInsert = F,addIndividuals = F)
###check concordance with old column names
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnssmelltaste",assessmentVersionCode = "1")
dbutil$compareColumnFormatWithSelectedExportData()
dbutil$renameAnnotatedVariableColumn("tastesmelllosssevere","howsevereiswastheloss",forceItem = "howsevereiswastheloss")
#View(dbutil$selectImportDataAssessmentItemVariableStats())
#View(dbutil$variableMetaDataDf)
# View(dbutil$variableAnnotationDf)
# View(dbutil$itemAnnotationDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnssmelltaste", assessmentVersionCode = "1", stageCode = "followup2", parseAndBusiness = F, prepare = T,import = T,doAnnotate = T,doInsert = T,addIndividuals = T)

###check
dbutil$selectExportData(cohortCode = cohortSetting,instanceCode = cohortInstanceSetting,assessmentCode = "covidcnssmelltaste",assessmentVersionCode = "1", joinSec = T)
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup1",])
# View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
# View(dbutil$importDataDf)

#import comments
# tastesmelllosssevere (fu) -> howsevereiswastheloss (bl)


