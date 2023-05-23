#Demo 2 of the COVID-CNS PhenoDB

#libraries
#library(RPostgres)
#library(DBI)
#devtools::install_github("tnggroup/phenodbr")
library(phenodbr)

#settings and configuration
qCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/latest_freeze",mustWork = T)

cognitronCleanedFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/TNG-Public - ilovecovidcns - ilovecovidcns/data/cognitron",mustWork = T)

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ALL_202301")
folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")

cohortSetting <- "covidcns"
cohortInstanceSetting <- "2023"

cinfo<-c()
cinfo$pw <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")
cinfo$host<-"localhost"
cinfo$dbname<-"phenodb"
cinfo$user<-"tng2101"
cinfo$port<-65432 #65432 for remote


dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)


#Demo

#Select variable data dictionary
selectedCovidcnsdem <- dbutil$selectVariableMetaData(
  cohortCode = cohortSetting,
  instanceCode = cohortInstanceSetting,
  assessmentCode = "covidcnsdem",
  assessmentVersionCode = "1",
  assessmentItemCodeList = list('dateofbirth','dobage','ethnicorigin')
)
View(selectedCovidcnsdem)

selectedCovidcnsdem <- dbutil$selectItemMetaData(
  cohortCode = cohortSetting,
  instanceCode = cohortInstanceSetting,
  assessmentCode = "covidcnsdem",
  assessmentVersionCode = "1",
  assessmentItemCodeList = list('dateofbirth','dobage','ethnicorigin')
)
View(selectedCovidcnsdem)

selectedCovidcnsdem <- dbutil$selectItemMetaData(
  cohortCode = cohortSetting,
  instanceCode = cohortInstanceSetting,
  assessmentCode = NULL,
  assessmentVersionCode = NULL
)
View(selectedCovidcnsdem)

#Extract data
dbutil$selectExportData(
  cohortCode = cohortSetting,
  instanceCode = cohortInstanceSetting,
  assessmentCode = "covidcnsdem",
  assessmentVersionCode = "1"
  #assessmentItemCodeList = c("dateofbirth","dobage","ethnocorigin")
)
View(dbutil$exportDataDf)
View(dbutil$itemMetaDataDf)
View(dbutil$variableMetaDataDf)

dbutil$selectExportData(
  cohortCode = cohortSetting,
  instanceCode = cohortInstanceSetting,
  assessmentCode = "idpukbb",
  assessmentVersionCode = "2022"
  #assessmentItemCodeList = c('visit','qc','idpt1sienax','idp1first','idp1fastrois')
)
View(dbutil$exportDataDf)
View(dbutil$itemMetaDataDf)
View(dbutil$variableMetaDataDf)


