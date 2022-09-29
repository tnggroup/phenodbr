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
qCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/latest_freeze",mustWork = T)

cognitronCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/cognitron",mustWork = T)

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/TRAVEL_processed")
folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")


cinfo<-c()
cinfo$pw <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")
cinfo$host<-"localhost"
cinfo$dbname<-"phenodb"
cinfo$user<-"tng2101"
cinfo$port<-65432 #65432 for remote



dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)
#dbutil$setSearchPath.standard()



#Baseline

##als_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","als_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsals", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = "als\\.", deitemise = T)
dim(dbutil$importDataDf)

##cage_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","cage_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cage", assessmentVersionCode = "covidcns", stageCode = "bl",  prefixesToExcludeRegex = list("cage\\.","cage\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##catatonia_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","catatonia_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "rogers", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("catatonia\\.","catatonia\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##cfs_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","cfs_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "bl",  prefixesToExcludeRegex = list("cfs\\.","cfs\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##dem_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","dem_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("dem\\.","dem\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##facial_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","facial_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsfacial", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("facial\\.","facial\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##fam_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","fam_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsfam", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("fam\\.","fam\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##gad7_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","gad7_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "gad7", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("gad7\\.","gad7\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##impact_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","impact_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsimpact", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("impact\\.","impact\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##mhd_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","mhd_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsmhd", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("mhd\\.","mhd\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##nonmotor_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","nonmotor_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsnonmotor", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("nonmotor\\.","nonmotor\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##pcl5_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","pcl5_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "pcl5", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("pcl5\\.","pcl5\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##phq9_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","phq9_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "phq9", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("phq9\\.","phq9\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##smelltaste_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","smelltaste_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnssmelltaste", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("smelltaste\\.","smelltaste\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##sub_covidcns_clean - second part of the demographics questionnaire
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","sub_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("sub\\.","sub\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##trauma_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","trauma_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnstrauma", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("trauma\\.","trauma\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##updrs_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","updrs_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "updrs", assessmentVersionCode = "covidcns", stageCode = "bl",  prefixesToExcludeRegex = list("updrs\\.","updrs\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##vaccine_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","vaccine_covidcns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsvaccine", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("vaccine\\.","vaccine\\.\\."), deitemise = T)
dim(dbutil$importDataDf)


#--- old
##sexual_orientation_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","sexual_orientation_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = ".+\\.", deitemise = T)
dbutil$cleanup()

##marital_status_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","marital_status_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = ".+\\.", deitemise = T)
dbutil$cleanup()

##language_clean
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


##highest_education_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","highest_education_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("dem\\.","dem_1\\."), deitemise = T, cohortIdColumn = "id")
dbutil$cleanup()


##ethnicity_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"demographics","ethnicity_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("dem\\.","dem_1\\."), deitemise = T, cohortIdColumn = "externaldatareference")
dbutil$cleanup()

#Baseline

##covid19_covid_cns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"baseline","covid19_covid_cns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnscovid19", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("covid19\\."), deitemise = T, cohortIdColumn = "id")
dbutil$cleanup()

#dbutil$importDataAsTables(temporary = F)


#Core neuro

##gcs_outpatient_covid_cns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"core_neuro","gcs_outpatient_covid_cns_clean.rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "gcs", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("covid19\\."), deitemise = T, cohortIdColumn = "id", doAnnotate = T, addIndividuals = F, doInsert = F )
dbutil$itemAnnotationDf$assessment_type<-"interview"
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "gcs", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("covid19\\."), deitemise = T, cohortIdColumn = "id", doAnnotate = F, addIndividuals = T, doInsert = T)
dbutil$cleanup()

#extraction test - this should generate a dataframe!
dbutil$selectExportData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsdem",
  assessmentVersionCode = "1",
  #assessmentItemCodeList = c("dateofbirth","dobage","ethnocorigin")
  )
View(dbutil$exportDataDf)


#Cognitron
dbutil$readImportData(dataframe = fread(file = file.path(cognitronCleanedFolderPath,"Cognitron_data_09.07.2021_CCNS_CNS01040.csv"), na.strings =c(".",NA,"NA",""), encoding = "UTF-8",check.names = T, fill = T, blank.lines.skip = T, data.table = T, nThread = 5, showProgress = F))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)



#IDP's - NOT FINISHED!!
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

