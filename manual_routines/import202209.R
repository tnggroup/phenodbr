#Relational database import from cleaned Qualtrics export, 2022-09-06
#Johan Zvrskovec

#libraries
library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)
library(phenodbr)
library(data.table)

#settings and configuration
qCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/MT-BioResource - Covid-CNS Data Extraction - Covid-CNS Data Extraction/Extraction/data/latest_freeze",mustWork = T)

cognitronCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/MT-BioResource - Covid-CNS Data Extraction - Covid-CNS Data Extraction/Data_intergration",mustWork = T)

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/TRAVEL_processed")
folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")


cinfo<-c()
cinfo$pw <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")
cinfo$host<-"localhost"
cinfo$dbname<-"phenodb"
cinfo$user<-"tng2101"
cinfo$port<-5432 #65432 for remote



dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
#dbutil$setSearchPath.standard()
#Demographics

##sex_and_gender_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","sex_and_gender_clean.rds"))
# dbutil$parseVariableLabels()
# dbutil$formatImportColumnNames(prefixesToExcludeRegex = ".+\\.", deitemise = T)
# dbutil$synchroniseVariableLabelTextForValueColumns()
# dbutil$fixIdColumn()
# dbutil$parseVariableValueLabels()
# #dbutil$interpretAndParseBooleanDataTypes()
# dbutil$filterColumnsOnFormatting()
# dbutil$createVariableAnnotation()
# dbutil$amendVariableAnnotationFromVariableLabelText()
# dbutil$importDataAsTables(temporary = T)
# dbutil$prepareImport(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", cohortIdColumn = "id")
# dbutil$selectImportDataAssessmentVariableAnnotation()
# dbutil$selectImportDataMeta()
# dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", doAnnotate = T, addIndividuals = T, doInsert = T)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = ".+\\.", deitemise = T)

#sexual_orientation_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","sexual_orientation_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = ".+\\.", deitemise = T)
dbutil$cleanup()

#marital_status_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","marital_status_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = ".+\\.", deitemise = T)
dbutil$cleanup()

#language_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","language_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

# dbutil$parseVariableLabels()
# dbutil$formatImportColumnNames(prefixesToExcludeRegex = list("dem\\.","dem_1\\."), deitemise = T)
# dbutil$synchroniseVariableLabelTextForValueColumns()
# dbutil$fixIdColumn(cohortIdColumn = "externaldatareference")
# dbutil$parseVariableValueLabels()
# #dbutil$interpretAndParseBooleanDataTypes()
# dbutil$filterColumnsOnFormatting()
# dbutil$createVariableAnnotation()
# dbutil$amendVariableAnnotationFromVariableLabelText()
# dbutil$importDataAsTables(temporary = F)
# dbutil$prepareImport(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", cohortIdColumn = "id")
# dbutil$selectImportDataAssessmentVariableAnnotation()
# dbutil$selectImportDataMeta()
# dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", doAnnotate = T, addIndividuals = T, doInsert = T)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("dem\\.","dem_1\\."), deitemise = T, cohortIdColumn = "externaldatareference")
dbutil$cleanup()


dbutil$readImportData(dataframe = covidcnsReadIDP(folderpathIDP = folderpathIDP, folderpathIDPMeta = folderpathIDPMeta)$importDataDf)
dbutil$parseVariableLabels()
dbutil$formatImportColumnNames(deitemise = T) #this fails because it will try to set colnames on the data
#HERE!!!

# dbutil$synchroniseVariableLabelTextForValueColumns()
# dbutil$fixIdColumn(cohortIdColumn = "externaldatareference")
# dbutil$parseVariableValueLabels()
# #dbutil$interpretAndParseBooleanDataTypes()
# dbutil$filterColumnsOnFormatting()
# dbutil$createVariableAnnotation()
# dbutil$amendVariableAnnotationFromVariableLabelText()
# dbutil$importDataAsTables(temporary = F)
# dbutil$prepareImport(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", cohortIdColumn = "id")
# dbutil$selectImportDataAssessmentVariableAnnotation()
# dbutil$selectImportDataMeta()
# dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", doAnnotate = T, addIndividuals = T, doInsert = T)

