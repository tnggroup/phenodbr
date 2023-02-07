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

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ALL_202301")
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


#core_neuro

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


#fbc

##fbc_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"fbc","fbc_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cbc", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("fbc\\.","fbc\\.\\."), deitemise = T)
dim(dbutil$importDataDf)


#mh_case_report

##moca_inp_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"mh_case_report","moca_inp_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "moca", assessmentVersionCode = "1", stageCode = "blinp",  prefixesToExcludeRegex = list("moca_inp\\.","moca_inp\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##psy_neuro_scr_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"mh_case_report","psy_neuro_scr_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnspsyneuroscr", assessmentVersionCode = "1", stageCode = "bl",  prefixesToExcludeRegex = list("psy_neuro_scr\\.","psy_neuro_scr\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

#moca

##cognitron_outp_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"moca","cognitron_outp_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cognitron", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("cognitron_outp\\.","cognitron_outp\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##moca_outp_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"moca","moca_outp_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "moca", assessmentVersionCode = "1", stageCode = "bloutp",  prefixesToExcludeRegex = list("moca_outp\\.","moca_outp\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

#neuro_case_report

##ncrf1_admission_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_admission_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_admission\\.","ncrf1_admission\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_care_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_care_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_care\\.","ncrf1_care\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_cic_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_cic_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_cic\\.","ncrf1_cic\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_comorbid_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_comorbid_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_comorbid\\.","ncrf1_comorbid\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_dem_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_dem_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_dem\\.","ncrf1_dem\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_lab_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_lab_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_lab\\.","ncrf1_lab\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_newsymp_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_newsymp_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_newsymp\\.","ncrf1_newsymp\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_pre_med_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_pre_med_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_pre_med\\.","ncrf1_pre_med\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf1_vital_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf1_vital_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m1", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf1_vital\\.","ncrf1_vital\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf2_care_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf2_care_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m2", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf2_care\\.","ncrf2_care\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf2_lab_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf2_lab_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m2", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf2_lab\\.","ncrf2_lab\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf2_med_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf2_med_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m2", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf2_med\\.","ncrf2_med\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf2_newsymp_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf2_newsymp_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m2", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf2_newsymp\\.","ncrf2_newsymp\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf2_vital_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf2_vital_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m2", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf2_vital\\.","ncrf2_vital\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf3_compli_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf3_compli_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m3", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf3_compli\\.","ncrf3_compli\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

##ncrf3_diag_covidcns_clean
dbutil$readImportData(filepath.rds = file.path(qCleanedFolderPath,"neuro_case_report","ncrf3_diag_covidcns_clean.Rds"))
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsncrf", assessmentVersionCode = "m3", stageCode = "bl",  prefixesToExcludeRegex = list("ncrf3_diag\\.","ncrf3_diag\\.\\."), deitemise = T)
dim(dbutil$importDataDf)

#extraction meta test - this should generate a dataframe!
View(dbutil$selectVariableMetaData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsna",
  assessmentVersionCode = "1"
))

#extraction test - this should generate a dataframe!

dbutil$selectExportData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsna",
  assessmentVersionCode = "1"
  #assessmentItemCodeList = c("dateofbirth","dobage","ethnocorigin")
  )
View(dbutil$exportDataDf)
View(dbutil$metaDataDf)


#Export of ncrf for Naomi - NOT INCLUDING STUDY ID!
dbutil$selectExportData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsncrf",
  assessmentVersionCode = "m1"
)
fwrite(x = dbutil$exportDataDf,file = paste0("COVIDCNS_NCRF_M1.tsv"),append = F,quote = T,sep = "\t",col.names = T)
fwrite(x = dbutil$metaDataDf,file = paste0("COVIDCNS_NCRF_M1_DICT.tsv"),append = F,quote = T,sep = "\t",col.names = T)

dbutil$selectExportData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsncrf",
  assessmentVersionCode = "m2"
)
fwrite(x = dbutil$exportDataDf,file = paste0("COVIDCNS_NCRF_M2.tsv"),append = F,quote = T,sep = "\t",col.names = T)
fwrite(x = dbutil$metaDataDf,file = paste0("COVIDCNS_NCRF_M2_DICT.tsv"),append = F,quote = T,sep = "\t",col.names = T)

dbutil$selectExportData(
  cohortCode = "covidcns",
  instanceCode = "2022",
  assessmentCode = "covidcnsncrf",
  assessmentVersionCode = "m3"
)
fwrite(x = dbutil$exportDataDf,file = paste0("COVIDCNS_NCRF_M3.tsv"),append = F,quote = T,sep = "\t",col.names = T)
fwrite(x = dbutil$metaDataDf,file = paste0("COVIDCNS_NCRF_M3_DICT.tsv"),append = F,quote = T,sep = "\t",col.names = T)




#connect again
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)


#IDPs - NOT FINISHED!!

#read and parse IDPs
idpData<-covidcnsReadIDP(folderpathIDP = folderpathIDP, folderpathIDPMeta = folderpathIDPMeta)
idpData$dfIDP$ID<-covidcnsParseIDColumn(IDs = idpData$dfIDP$ID)
idpData$dfFSIDP$ID<-covidcnsParseIDColumn(IDs = idpData$dfFSIDP$ID)

if(any(nchar(idpData$dfIDP$ID)!=8)) warning("There are IDs with length != 8 (IDP)")
if(any(nchar(idpData$dfFSIDP$ID)!=8)) warning("There are IDs with length != 8 (FSIDP)")


#Standard IDPs
dbutil$readImportData(dataframe = idpData$dfIDP)


#does not work for some reason
# dbutil$formatImportColumnNames(deitemise = T,
#                                prefixesToItemiseRegex = c(
#                                  "QC_",
#                                  "IDP_T1_SIENAX_",
#                                  "IDP_T1_FIRST_",
#                                  "IDP_T1_FAST_ROIs_",
#                                  "IDP_T2_FLAIR_BIANCA_",
#                                  "IDP_SWI_T2star_",
#                                  "IDP_tfMRI_",
#                                  "IDP_dMRI_TBSS_FA_",
#                                  "IDP_dMRI_TBSS_MD_",
#                                  "IDP_dMRI_TBSS_MO_",
#                                  "IDP_dMRI_TBSS_L1_",
#                                  "IDP_dMRI_TBSS_L2_",
#                                  "IDP_dMRI_TBSS_L3_",
#                                  "IDP_dMRI_TBSS_ICVF_",
#                                  "IDP_dMRI_TBSS_OD_",
#                                  "IDP_dMRI_TBSS_ISOVF_",
#                                  "IDP_dMRI_ProbtrackX_FA_",
#                                  "IDP_dMRI_ProbtrackX_MD_",
#                                  "IDP_dMRI_ProbtrackX_MO_",
#                                  "IDP_dMRI_ProbtrackX_L1_",
#                                  "IDP_dMRI_ProbtrackX_L2_",
#                                  "IDP_dMRI_ProbtrackX_L3_",
#                                  "IDP_dMRI_ProbtrackX_ICVF_",
#                                  "IDP_dMRI_ProbtrackX_OD_",
#                                  "IDP_dMRI_ProbtrackX_ISOVF_"
#                                )
#                                )


nformat <- formatStdColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToItemiseRegex = c(
  "QC_",
  "IDP_T1_SIENAX_",
  "IDP_T1_FIRST_",
  "IDP_T1_FAST_ROIs_",
  "IDP_T2_FLAIR_BIANCA_",
  "IDP_SWI_T2star_",
  "IDP_tfMRI_",
  "IDP_dMRI_TBSS_FA_",
  "IDP_dMRI_TBSS_MD_",
  "IDP_dMRI_TBSS_MO_",
  "IDP_dMRI_TBSS_L1_",
  "IDP_dMRI_TBSS_L2_",
  "IDP_dMRI_TBSS_L3_",
  "IDP_dMRI_TBSS_ICVF_",
  "IDP_dMRI_TBSS_OD_",
  "IDP_dMRI_TBSS_ISOVF_",
  "IDP_dMRI_ProbtrackX_FA_",
  "IDP_dMRI_ProbtrackX_MD_",
  "IDP_dMRI_ProbtrackX_MO_",
  "IDP_dMRI_ProbtrackX_L1_",
  "IDP_dMRI_ProbtrackX_L2_",
  "IDP_dMRI_ProbtrackX_L3_",
  "IDP_dMRI_ProbtrackX_ICVF_",
  "IDP_dMRI_ProbtrackX_OD_",
  "IDP_dMRI_ProbtrackX_ISOVF_"
), deitemise = T)
dbutil$columnFormat<-nformat
colnames(dbutil$importDataDf)<-dbutil$columnFormat$names.new

#fix ID
dbutil$fixIdColumn(cohortIdColumn = "id")

#dbutil$parseVariableValueLabels() #not needed

#select actual columns to import
#filterColumnsOnFormatting() #not needed

#annotation tables
dbutil$createVariableAnnotation(parseItems = T)

nVarAnnotation <- merge(dbutil$variableAnnotationDf,idpData$dfIDPmeta,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
setDT(nVarAnnotation)
nVarAnnotation$variable_documentation<-nVarAnnotation$description
nVarAnnotation$variable_unit<-nVarAnnotation$unit
nVarAnnotation$variable_data_type<-trimws(nVarAnnotation$datatype)
nVarAnnotation[variable_data_type=="int",variable_data_type:="integer"]
nVarAnnotation[variable_data_type=="float",variable_data_type:="double precision"]
dbutil$variableAnnotationDf<-unique(as.data.frame(nVarAnnotation[,c("column_name","variable_code","variable_original_descriptor","variable_label","index","item_code","variable_documentation","variable_unit","variable_data_type")]))

dbutil$castVariablesAsAnnotated()

dbutil$itemAnnotationDf <- data.frame(
  item_code=unique(dbutil$variableAnnotationDf[order(dbutil$variableAnnotationDf$index),]$item_code)
)
  #merge(dbutil$variableAnnotationDf,idpData$dfIDP,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
#dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[order(dbutil$itemAnnotationDf$order),]
dbutil$itemAnnotationDf$item_text<-""
dbutil$itemAnnotationDf$assessment_type<-"imaging"
dbutil$itemAnnotationDf$assessment_item_type_code<-"idp"
#dbutil$itemAnnotationDf$item_index<-dbutil$itemAnnotationDf$order
dbutil$itemAnnotationDf$item_documentation<-""
dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[,c("item_code","item_text","assessment_type","assessment_item_type_code","item_documentation")]
dbutil$itemAnnotationDf <- unique(dbutil$itemAnnotationDf)
dbutil$itemAnnotationDf$item_index <- 1:nrow(dbutil$itemAnnotationDf)

#check single variable items
grep(pattern = "_",x = dbutil$variableAnnotationDf$column_name, value = T, fixed = T, invert = T)

#import all tables
dbutil$importDataAsTables(temporary = T)

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'idpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'idpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)


#FS IDPs
dbutil$readImportData(dataframe = idpData$dfFSIDP)

nformat <- formatStdColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToItemiseRegex = c(
  "aseg_global_intensity_",
  "aseg_global_volume_",
  "aseg_global_volume-ratio_",
  "aseg_lh_intensity_",
  "aseg_lh_number_",
  "aseg_lh_volume_",
  "aseg_rh_intensity_",
  "aseg_rh_number_",
  "aseg_rh_volume",
  "AmygNuclei_lh_",
  "AmygNuclei_rh_",
  "HippSubfield_lh_",
  "HippSubfield_rh_",
  "ThalamNuclei_lh",
  "ThalamNuclei_rh",
  "Brainstem_global_volume_",
  "aparc-Desikan_lh_area_",
  "aparc-Desikan_lh_thickness_",
  "aparc-Desikan_lh_volume_",
  "aparc-Desikan_rh_area_",
  "aparc-Desikan_rh_thickness_",
  "aparc-Desikan_rh_volume_",
  "aparc-pial_lh_area_",
  "aparc-pial_rh_area_",
  "wg_lh_intensity-contrast_",
  "wg_rh_intensity-contrast_",
  "BA-exvivo_lh_area_",
  "BA-exvivo_lh_thickness_",
  "BA-exvivo_lh_volume_",
  "BA-exvivo_rh_area_",
  "BA-exvivo_rh_thickness_",
  "BA-exvivo_rh_volume_",
  "aparc-DKTatlas_lh_area_",
  "aparc-DKTatlas_lh_thickness_",
  "aparc-DKTatlas_lh_volume_",
  "aparc-DKTatlas_rh_area_",
  "aparc-DKTatlas_rh_thickness_",
  "aparc-DKTatlas_rh_volume_",
  "aparc-a2009s_lh_area_",
  "aparc-a2009s_lh_thickness_",
  "aparc-a2009s_lh_volume_",
  "aparc-a2009s_rh_area_",
  "aparc-a2009s_rh_thickness_",
  "aparc-a2009s_rh_volume_"
), deitemise = T)
dbutil$columnFormat<-nformat
colnames(dbutil$importDataDf)<-dbutil$columnFormat$names.new

#fix ID
dbutil$fixIdColumn(cohortIdColumn = "id")

#annotation tables
dbutil$createVariableAnnotation(parseItems = T)

nVarAnnotation <- merge(dbutil$variableAnnotationDf,idpData$dfIDPmeta,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
setDT(nVarAnnotation)
nVarAnnotation$variable_documentation<-nVarAnnotation$description
nVarAnnotation$variable_unit<-nVarAnnotation$unit
nVarAnnotation$variable_data_type<-trimws(nVarAnnotation$datatype)
nVarAnnotation[variable_data_type=="int",variable_data_type:="integer"]
nVarAnnotation[variable_data_type=="float",variable_data_type:="double precision"]
dbutil$variableAnnotationDf<-unique(as.data.frame(nVarAnnotation[,c("column_name","variable_code","variable_original_descriptor","variable_label","index","item_code","variable_documentation","variable_unit","variable_data_type")]))

dbutil$castVariablesAsAnnotated()

dbutil$itemAnnotationDf <- data.frame(
  item_code=unique(dbutil$variableAnnotationDf[order(dbutil$variableAnnotationDf$index),]$item_code)
)
#merge(dbutil$variableAnnotationDf,idpData$dfIDP,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
#dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[order(dbutil$itemAnnotationDf$order),]
dbutil$itemAnnotationDf$item_text<-""
dbutil$itemAnnotationDf$assessment_type<-"imaging"
dbutil$itemAnnotationDf$assessment_item_type_code<-"idp"
#dbutil$itemAnnotationDf$item_index<-dbutil$itemAnnotationDf$order
dbutil$itemAnnotationDf$item_documentation<-""
dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[,c("item_code","item_text","assessment_type","assessment_item_type_code","item_documentation")]
dbutil$itemAnnotationDf <- unique(dbutil$itemAnnotationDf)
dbutil$itemAnnotationDf$item_index <- 1:nrow(dbutil$itemAnnotationDf)

#check single variable items
grep(pattern = "_",x = dbutil$variableAnnotationDf$column_name, value = T, fixed = T, invert = T)

#test
# name<-"_import_data_df"
# if(dbExistsTable(conn = dbutil$connection, name = name)) dbRemoveTable(conn = dbutil$connection, name = name, temporary= F)
# excludeColIndexFrom <- 2
# excludeColIndexTo <- 500
#
# if(!is.null(excludeColIndexFrom)){
#   if(is.null(excludeColIndexTo)) excludeColIndexTo=ncol(importDataDf)
#   colIndices <- 1:(excludeColIndexFrom-1)
#   if(excludeColIndexTo < ncol(dbutil$importDataDf)) colIndices <- c(colIndices,excludeColIndexTo:ncol(dbutil$importDataDf))
#   tempImport<-dbutil$importDataDf[,colIndices]
#   dbCreateTable(conn = dbutil$connection, name = name, fields = tempImport, temporary = F)
#   dbAppendTable(conn = dbutil$connection, name = name, value = tempImport)
# } else {
#   dbCreateTable(conn = dbutil$connection, name = name, fields = importDataDf, temporary = F)
#   dbAppendTable(conn = dbutil$connection, name = name, value = importDataDf)
# }


#import all tables  -should be 1275 columns
dbutil$importDataAsTables(temporary = T,excludeColIndexFrom = 501) #Import too big for one table import

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)

#import all tables again for the rest of the columns - This seems to work now!
dbutil$importDataAsTables(temporary = T,excludeColIndexFrom = 2, excludeColIndexTo = 500) #Import too big for one table import

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = 'covidcns', instanceCode = '2022', assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)

#OLD STUFF BELOW!
# #Cognitron
# dbutil$readImportData(dataframe = fread(file = file.path(cognitronCleanedFolderPath,"Cognitron_data_09.07.2021_CCNS_CNS01040.csv"), na.strings =c(".",NA,"NA",""), encoding = "UTF-8",check.names = T, fill = T, blank.lines.skip = T, data.table = T, nThread = 5, showProgress = F))
# head(dbutil$importDataDf)
# colnames(dbutil$importDataDf)





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

