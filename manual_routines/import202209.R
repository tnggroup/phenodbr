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

#clin_neuro

##clinical_na_inp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"clin_neuro","clinical_na_inp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^clinical_na_inp\\.(.+)"),replacement = "clinical_na\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsna", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("clinical_na\\.","clinical_na\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##clinical_na_outp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"clin_neuro","clinical_na_outp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^clinical_na_outp\\.(.+)"),replacement = "clinical_na\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsna", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("clinical_na\\.","clinical_na\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

#Warning!! there may be text columns erroneously interpreted as float from the previous insert here which will make the routine crash until they have been corrected in teh database before the next import.


##core_neuro


##fourat_inp_covidcns_clean - special treatment as inpatients and outpatients share instrument

readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","fourat_inp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^fourat_inp\\.(.+)"),replacement = "fourat\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "4at", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("fourat\\.","fourat\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##fourat_outp_covidcns_clean - special treatment as inpatients and outpatients share instrument

readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","fourat_outp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^fourat_outp\\.(.+)"),replacement = "fourat\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "4at", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("fourat\\.","fourat\\.\\."), deitemise = T)
dim(dbutil$importDataDf)


##gcs_inp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","gcs_inp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^gcs_inp\\.(.+)"),replacement = "gcs\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "gcs", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("gcs\\.","gcs\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##gcs_outp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","gcs_outp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^gcs_outp\\.(.+)"),replacement = "gcs\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "gcs", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("gcs\\.","gcs\\.\\."), deitemise = T)
dim(dbutil$importDataDf)


##neuro_add_inp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","neuro_add_inp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^neuro_add_inp\\.(.+)"),replacement = "neuro_add\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsneuroadd", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("neuro_add\\.","neuro_add\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##neuro_add_outp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","neuro_add_outp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^neuro_add_outp\\.(.+)"),replacement = "neuro_add\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsneuroadd", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("neuro_add\\.","neuro_add\\.\\."), deitemise = T)
dim(dbutil$importDataDf)


##nis_inp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","nis_inp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^nis_inp\\.(.+)"),replacement = "nis\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsnis", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("nis\\.","nis\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##nis_outp_covidcns_clean - special treatment as inpatients and outpatients share instrument
readImportData<-readRDS(file = file.path(qCleanedFolderPath,"core_neuro","nis_outp_covidcns_clean.rds"))
columnNames<-colnames(readImportData)

#Edit colnames to correspond to a shared instrument for inpatients and outpatients
for(iCol in 1:length(columnNames)){
  #iCol<-4
  cName<-columnNames[iCol]
  cName<-gsub(pattern = paste0("^nis_outp\\.(.+)"),replacement = "nis\\.\\1", x = cName)
  columnNames[iCol]<-cName
}
colnames(readImportData)<-columnNames
dbutil$readImportData(dataframe = readImportData)
#head(dbutil$importDataDf)
colnames(dbutil$importDataDf)

dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsnis", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("nis\\.","nis\\.\\."), deitemise = T)
dim(dbutil$importDataDf)



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

