#install.packages("optparse")
#install.packages('RPostgres')
library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)

#working directory is expected to be the location of the script!
source("pg_database_utility.R")
source("xnat_http_rest_driver.R")

clParser <- OptionParser()
clParser <- add_option(clParser, c("-t", "--task"), type="character",
                       help="Code of the task to run [default %default]")
clParser <- add_option(clParser, c("-l", "--location"), type="character",
                       help="The place where the code is run [local,server] [default %default]")
clParser <- add_option(clParser, c("-a", "--task_argument"), type="character",
                       help="General purpose argument for tasks [default %default]")
p<-c()
p$clArguments<-parse_args(clParser)
p$filepath.config<-file.path("automated_db_routine.conf.tsv")
p$settings<-as.data.frame(matrix(data = NA, nrow = 0, ncol = 1)) #template
colnames(p$settings)<-c("value")

#settings defaults

##command line defaults
p$settings["task",1]<-"default"
p$settings["location",1]<-"local"
p$settings["task_argument",1]<-"default"

##project defaults
p$settings["xnat.url",1]<-"https://xnat01.wbic.cam.ac.uk/"
p$settings["xnat.project-id",1]<-"P00557" #the xnat01_cambridge covidcns project-id

p$settings["q.import_path",1]<-"/Users/jakz/King's College London/MT-BioResource - Covid-CNS Data Extraction - Covid-CNS Data Extraction/Extraction/data_raw/2021-07-12"


if(file.exists(p$filepath.config)) {
  p$settingsConfig<-read.table(p$filepath.config, header=F, sep = "")
  p$settings[,p$settingsConfig$V1]<-p$settingsConfig$V2
} else {
    #write template config
    p$settingsTemplate<-p$settings
    p$settingsTemplate$key<-rownames(p$settingsTemplate)
    write.table(x = p$settingsTemplate[,c("key","value")], file = p$filepath.config, col.names = F, quote = F, row.names = F, sep = "\t")
  }

#add settings from the command line, overwriting any previous settings
if(!is.null(p$clArguments$task)) p$settings["task",1]<-p$clArguments$task
if(!is.null(p$clArguments$location)) p$settings["location",1]<-p$clArguments$location
if(!is.null(p$clArguments$task_argument)) p$settings["task_argument",1]<-p$clArguments$task_argument




# create database utility
#dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=5432, password="")
#dbutil <- pgDatabaseUtilityClass(host="postgresql.cluster-cjghupwohy3q.eu-west-2.rds.amazonaws.com", dbname="phenodb", user="tng2101", port=5432, password="")
dbutil <- pgDatabaseUtilityClass(host="10.200.105.5", dbname="phenodb", user="postgres", port=5432, password="")

df<-readRDS(file = file.path(p$settings["q.import_path",1],"baseline","dem_1_covid_cns.rds"))

#nname<-dbutil$createCohortInstanceAssessmentDataTableName(cohortCode = "covidcns",instanceCode = "2021",assessmentCode = "covidcnsdem1")



#this should be converted into a regression test routine
c <- dbutil$getCohort(cohort = "covidcns")
cohortinstanceId <- dbutil$getCohortinstance(cohort = "covidcns", instance = "2021")
c <- dbutil$getCohortstage(cohort = "covidcns", "bl")
assessmentId <- dbutil$getAssessment(assessmentCode = "idpukbb", assessmentVersionCode = "2021")
c <- dbutil$getAssessmentItemType(assessmentType = "imaging",assessmentItemTypeCode = "idp")
#error>
# c <- dbutil$x_selectAssessmentItemByFuzzyItemText(
#   assessmentId = dbutil$getAssessment(assessmentCode = "idpukbb", assessmentVersionCode = "2021"),
#   assessmentItemTypeId = 3,  #imaging
#   assessmentItemSourceText = "some text"
#   )

dbutil$parseAssessmentItemCodeFromColumnName("testColumn_withVariable")
dbutil$parseAssessmentItemVariableCodeFromColumnName("testColumn_withVariable")

c <- dbutil$x_getAssessmentItem(assessmentId = assessment_id, assessmentItemTypeId = 3, assessmentItemCode = "myitem")

c <- dbutil$verifyCohortinstanceAssessment(cohortCode = "covidcns", instanceCode = "2021", assessmentCode = "idpukbb", assessmentVersionCode = "2021")

c <- dbutil$x_checkCohortinstanceAssessmentItemVariable(cohortinstanceId = 1, assessmentId = 1, assessmentItemId = 1, assessmentItemVariableId = 1)

c <- dbutil$x_checkCohortinstanceAssessmentItemVariableFromColumnName(cohortinstanceId = 1, assessmentId = 1,columnName = "item1_variable1")

cohortinstanceAssessmentMeta<-dbutil$selectCohortinstanceAssessmentItemVariableMeta(cohortinstanceId,assessmentId)

check<-dbutil$checkCohortinstanceAssessmentDataframe(cohortCode = "covidcns", instanceCode = "2021", assessmentCode = "idpukbb", assessmentVersionCode = "2021", df = df, columnNamePrefixesToExclude = "dem_1\\.")

check$assessment
check$columns



# dbutil$createCohortInstanceAssessmentDataTable(df = df, cohortCode = "covidcns", instanceCode = "2021", assessmentCode = "covidcnsdem1")
#
# typespec <- dbDataType(dbutil$connection, df)
# RPostgres::dbBegin(conn = dbutil$connection)
# dbutil$setSearchPath(searchPath = c("dat_cohort"))
# RPostgres::dbCreateTable(conn = dbutil$connection, name = nname, fields = typespec)
# RPostgres::dbCommit(conn = dbutil$connection)


# create xnat driver
# temporarily using rstudioapi::askforpassword
#xnat<-covidcnsXnatHttpRestDriverClass(username="jkzvrskovec",password=rstudioapi::askForPassword(prompt = "Enter password for xnat user."),xnatURL=p$settings["xnat.url",1],xnatProjectId=p$settings["xnat.project-id",1])

xnat<-covidcnsXnatHttpRestDriverClass(username="jkzvrskovec",password="XXX",xnatURL=p$settings["xnat.url",1],xnatProjectId=p$settings["xnat.project-id",1])

searchElements<-xnat$get.search.elements()
View(searchElements$ResultSet$Result)

searchElementFields<-xnat$get.search.elements.fields(xsiType = "xnat:subjectData")
View(searchElementFields)

View(xnat$getCachedSearchElementFields(xsiType = "xnat:subjectData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:mrSessionData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:petSessionData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:megSessionData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:projectData"))
View(xnat$getCachedSearchElementFields(xsiType = "dpuk:dpukAssessmentData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:petmrSessionData"))
View(xnat$getCachedSearchElementFields(xsiType = "xnat:ctSessionData"))

resources<-xnat$get.project.resources()
resources$ResultSet$Result

#build search xml for testing below
searchXml<-xml_new_root("xml", .version = "1.0", .encoding = "UTF-8")
xml_attr(searchXml, "version") <- "1.0"
xml_attr(searchXml, "encoding") <- "UTF-8"
xml_attr(searchXml, "xmlns:xdat") <- "http://nrg.wustl.edu/security"
xml_attr(searchXml, "xmlns:xsi") <- "http://www.w3.org/2001/XMLSchema-instance"
searchNode<-searchXml %>% xml_add_child("xdat:search")
xml_attr(searchNode, "ID") <- ""
xml_attr(searchNode, "allow-diff-columns") <- "0"
xml_attr(searchNode, "secure") <- "false"
xml_attr(searchNode, "brief-description") <- "test"
searchNode %>% xml_add_child("root_element_name") %>% xml_set_text("xnat:mrSessionData")
searchNode %>% xml_add_child("search_field") %>%
  xml_add_child("element_name") %>% xml_set_text("xnat:mrSessionData") %>%
  xml_add_sibling("field_ID") %>% xml_set_text("SESSION_ID") %>%
  xml_add_sibling("header") %>% xml_set_text("Session")

#test search api
url<-paste0(xnat$xnatURL,"data/search")
#urlTest<-paste0("https://central.xnat.org/","data/search")
url

searchXML<-""
r<-POST(url = url, format = "json", add_headers(Authorization = xnat$authorisation), body = list(search = searchXML), content_type_xml())
status_code(r)
#stop_for_status(r)
#headers(r)
content(r, "text", encoding = "UTF-8")


