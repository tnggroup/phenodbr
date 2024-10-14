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
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)


#import comments
#prefix alsfrs rather than als
#limbweaknessnoticedpast is entered without other answers for the other variables in the follow-up data, while it is always entered together with data in the other variables in BL entries.
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
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)

#import comments
# COGTEST-participants imported from this data (for example COGTEST1)
# ALL variable/column names had changed names in the follow-up data



#import comments
#csri, not documented what this instrument is.
#mixed integer and text variables
#NOT IMPORTED YET

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
#View(dbutil$exportDataDf[dbutil$exportDataDf$`_stage_code`=="followup2",])
#View(dbutil$importDataDf)


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


# #Baseline
#
# ##als_covidcns_clean
# dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","als_covidcns_clean.rds"))
# head(dbutil$importDataDf)
# colnames(dbutil$importDataDf)
# dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = "als\\.", deitemise = T)
# dim(dbutil$importDataDf)
