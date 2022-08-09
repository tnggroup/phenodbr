#Relational database import from Qualtrics export, 2022-01-20
#Johan Zvrskovec

#libraries
library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)
library(phenodbr)

#settings and configuration
qImportFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/MT-TNG BioResource EDIT - ilovedata - ilovedata/data/latest_freeze/covidcns",mustWork = T)
cognitronExportImportFolderPath<-normalizePath("/Users/jakz/Library/CloudStorage/OneDrive-SharedLibraries-King'sCollegeLondon/MT-TNG BioResource EDIT - ilovedata - ilovedata/covid_cns/covidcns_cognitron",mustWork = T)
pwtemp <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")




dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
#dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=5432, password="")
#dbutil <- pgDatabaseUtilityClass(host="10.200.105.5", dbname="phenodb", user="postgres", port=5432, password="")
rm(pwtemp)

#routine

##AGE
df<-readRDS(file = file.path(qImportFolderPath,"age_covidcns_clean.rds"))
colnames(df)
columnFormat<-dbutil$formatImportColumnNames(columnNames = colnames(df),prefixesToExclude = "dem_1\\.",deitemise = T)
colnames(df)<-columnFormat$new
variableAnnotation<-data.frame(column_name=columnFormat$new,variable_code=NA_character_, variable_original_descriptor=columnFormat$orig,item_code=columnFormat$new)
#fix ID
df$ID<-gsub(pattern = "[^A-Za-z0-9]+", replacement = "\\1", x = df$ID)

dbutil$importDataAsTable(name = "age_covidcns_clean",df,temporary = T)
dbutil$importDataAsTable(name = "variable_annotation",variableAnnotation,temporary = T)

#test select
q <- dbSendQuery(dbutil$connection,
                 "SELECT * FROM age_covidcns_clean")
res<-dbFetch(q)
dbClearResult(q)

# q <- dbSendQuery(dbutil$connection,
#                  "SELECT coh.prepare_import($1,$2,$3,$4,$5,$6)",
#                  list("covidcns","2021","covidcnsdem","1","imp2","{\"ID\"}"))
# res<-dbFetch(q)
# dbClearResult(q)


dbutil$prepareImport(
  cohortCode = "covidcns",instanceCode = "2022",assessmentCode = "covidcnsdem",assessmentVersionCode = "1", tableName = "age_covidcns_clean", cohortIdColumn = "ID", varableAnnotationTableName = "variable_annotation")


View(dbutil$selectImportDataMeta())
View(dbutil$selectImportDataAssessmentStats())

dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", tableName = "age_covidcns_clean", doAnnotate = F, addIndividuals = F,doInsert = F)

View(dbutil$selectImportDataAssessmentItemAnnotation())

dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", tableName = "age_covidcns_clean", doAnnotate = T, addIndividuals = T,doInsert = T)


#ETHNICITY
df<-as.data.frame(readRDS(file = file.path(qImportFolderPath,"ethnicity_covidcns_clean.rds")))
colnames(df)
columnFormat<-dbutil$formatImportColumnNames(columnNames = colnames(df),prefixesToExclude = "dem_1\\.",deitemise = T)
colnames(df)<-columnFormat$names.new
# df.labels<-df[,columnFormat$colsValueLabels]
# df<-df[,columnFormat$colsSelect]

#variable labels
variableLabels<-unlist(lapply(df,function(x)attr(x,which = "label", exact = T)[[1]]))

#fix ID
df$id<-gsub(pattern = "[^A-Za-z0-9]+", replacement = "\\1", x = df$id)

#Change data type of two-category 1/0 columns to boolean true/false, based on the value space data from a sample (tail) of 2000 rows
df.sample<-tail(df,n = 2000)
if(nrow(df.sample)>99){ #only run if we have a sufficient sample to base the decision on. may still cause sparse variables to be misinterpreted here as boolean where there in fact are more values but not in the sample.
  for(iCol in 1:length(colnames(df))){
    #iCol<-24
    if(!is.numeric(df.sample[,iCol])) next
    if(sum(!is.na(df.sample[,iCol]))>10){
      vals<-na.omit(unique(df.sample[,iCol]))
      vals<-vals[order(vals)]
      if(length(vals)>1 & length(vals) < 3 & (0 %in% vals | 1 %in% vals)) df[,iCol]<-(df[,iCol]==1)
    }
  }
}

variableValueLabels<-data.frame(matrix(data = NA,nrow = 0,ncol = 3))
colnames(variableValueLabels)<-c("variable_index","value","label")
#parse value labels
for(iCol in 1:length(columnFormat$names.orig)){
  #iCol<-24
  if(is.na(columnFormat$valueLabelColumn[iCol])) next
  svl<-columnFormat$valueLabelColumn[iCol]
  svlindex<-which(columnFormat$names.orig %in% svl)
  if(length(svlindex)>0){
    perm<-unique(df[,c(iCol,svlindex[1])])
  }

  if(nrow(perm)!=length(unique(perm[,1]))){
    print(perm)
    stop(paste0("Value label combination inconsistency found for column ",columnFormat$names.new[iCol]))
  }

  variableValueLabels<-rbind(variableValueLabels,data.frame(variable_index=iCol,value=perm[,1],label=perm[,2]))

}


#select actual columns to import
df<-df[,columnFormat$colsSelect]

#annotation tables
variableAnnotation<-data.frame(
  column_name=colnames(df),
  variable_code=NA_character_,
  variable_original_descriptor=columnFormat$names.orig[columnFormat$colsSelect],
  item_code=colnames(df)
  )
variableAnnotation$variable_label<-variableLabels[variableAnnotation$column_name]
variableAnnotation$index<-1:nrow(variableAnnotation)

#itemAnnotation<-data.frame(item_code=colnames(df))
#itemAnnotation$item_text<-variableLabels[itemAnnotation$item_code]
uniqueVariableLabels<-unique(na.omit(variableAnnotation$variable_label))

# #Create item codes using text mining (tm package) - EXPERIMENTAL
# #source: https://www.red-gate.com/simple-talk/databases/sql-server/bi-sql-server/text-mining-and-sentiment-analysis-with-r/
# novelEnglishCorpus<-tm::Corpus(tm::VectorSource(readLines(file("data/tm_corpora/NovelEnglish_All.csv"))))
# #Replacing "/", "@" and "|" with space
# toSpace <- tm::content_transformer(function (x , pattern ) gsub(pattern, " ", x))
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, toSpace, "/")
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, toSpace, "@")
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, toSpace, "\\|")
# # Convert the text to lower case
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::content_transformer(tolower))
# # Remove numbers
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::removeNumbers)
# # Remove english common stopwords
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::removeWords, tm::stopwords("english"))
# # Remove your own stop word
# # specify your custom stopwords as a character vector
# #novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::removeWords, c("s", "company", "team"))
# # Remove punctuations
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::removePunctuation)
# # Eliminate extra white spaces
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::stripWhitespace)
# # Text stemming - which reduces words to their root form
# ##Needs SnowballC package
# novelEnglishCorpus <- tm::tm_map(novelEnglishCorpus, tm::stemDocument)
#
# # Build a term-document matrix
# novelEnglishCorpus_dtm <- tm::TermDocumentMatrix(novelEnglishCorpus)
# dtm_m <- as.matrix(novelEnglishCorpus_dtm)
# # Sort by descearing value of frequency
# dtm_v <- sort(rowSums(dtm_m),decreasing=TRUE)
# dtm_d <- data.frame(word = names(dtm_v),freq=dtm_v)
# # Display the top 5 most frequent words
# head(dtm_d, 5)


#Create item codes using text mining (tm package)
n_item_code_raw<-gsub(pattern = "[^a-z0-9 ]+", replacement = "\\1", x = tolower(uniqueVariableLabels)) #only letters, numbers, spaces
n_item_code_split<-strsplit(x = n_item_code_raw,split = " ")
trimmedStopwordsEnglish= gsub(pattern = "[^a-z0-9]+", replacement = "\\1", x = tm::stopwords("english"))
newItemAnnotationCodes<-data.frame(item_text=uniqueVariableLabels)
newItemAnnotationCodes$item_code<-lapply(n_item_code_split, function(x){
  paste0(x[!x %in% trimmedStopwordsEnglish],collapse = "")
})


merged<-merge(variableAnnotation,newItemAnnotationCodes, by.x = "variable_label",by.y = "item_text",all.x = T, all.y = F)
merged$item_code.x<-as.character(merged$item_code.x)
merged$item_code.y<-as.character(merged$item_code.y)
merged<-merged[order(merged$index),]

merged_agg<-aggregate(column_name ~ item_code.y,merged,length)
multiItems<-merged_agg[merged_agg$column_name>1,c("item_code.y")]
multiItemsVarL<-merged$item_code.y %in% multiItems
merged$item_code<-ifelse(multiItemsVarL,merged$item_code.y,merged$item_code.x)
merged$variable_code<-ifelse(multiItemsVarL,merged$item_code.x,merged$variable_code)

variableAnnotation<-merged[,c("column_name","item_code","variable_code","variable_original_descriptor")]

itemAnnotation<-merged[,c("item_code","variable_label")]
colnames(itemAnnotation)<-c("item_code","item_text")
itemAnnotation<-unique(itemAnnotation)
itemAnnotation$assessment_type<-"questionnaire"
itemAnnotation$assessment_item_type_code<-ifelse(itemAnnotation$item_code %in% multiItems, "multi","single")
itemAnnotation$item_index<-1:nrow(itemAnnotation)
itemAnnotation$item_documentation<-""


dbutil$importDataAsTable(name = "ethnicity_covidcns_clean",df,temporary = F)
dbutil$importDataAsTable(name = "item_annotation",itemAnnotation,temporary = F)
dbutil$importDataAsTable(name = "variable_annotation",variableAnnotation,temporary = F)
dbutil$importDataAsTable(name = "variable_value_labels",variableValueLabels,temporary = F)

dbutil$prepareImport(
  cohortCode = "covidcns",instanceCode = "2022",assessmentCode = "covidcnsdem",assessmentVersionCode = "1", tableName = "ethnicity_covidcns_clean", cohortIdColumn = "ID", varableAnnotationTableName = "variable_annotation")

dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", tableName = "ethnicity_covidcns_clean", doAnnotate = F, addIndividuals = F,doInsert = F)

View(dbutil$selectImportDataAssessmentItemVariableStats())




#EMPLOYMENT

dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"employment_covidcns_clean.rds"))

#View(dbutil$importDataDf)

#dbutil$defaultAnnotateAndImportProcedure(prefixesToExclude = "impact\\.",deitemise = T,forceItem = NULL, interpretBooleanDatatypeFromData = T)

variableLabels<-lapply(dbutil$importDataDf,function(x)attr(x,which = "label", exact = T)[[1]])
#names(variableLabels)<-columnFormat$names.orig

columnFormat <- formatImportColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToExclude = "impact\\.",deitemise = T, forceItem = NULL)
colnames(dbutil$importDataDf) <- columnFormat$names.new

#
# for(iCol in 1:length(columnFormat$names.orig)){
#   variableLabels[iCol]<-variableLabels[iCol][[1]]
# }

#synchronise item text/label between value and label column - only works for numeric columns missing labels
for(iCol in 1:length(columnFormat$names.orig)){
  #iCol<-46
  if(is.null(variableLabels[iCol][[1]]) & !is.null(variableLabels[columnFormat$valueLabelColumn[iCol]][[1]]) ){
    variableLabels[iCol]<-variableLabels[columnFormat$valueLabelColumn[iCol]][[1]]
  }
}

#variableLabels<-unlist(lapply(dbutil$importDataDf,function(x)attr(x,which = "label", exact = T)[[1]]))


dbutil$importDataDf$id <- gsub(pattern = "[^A-Za-z0-9]+", replacement = "\\1", x = dbutil$importDataDf$id)

variableValueLabelsDf <- data.frame(matrix(data = NA,nrow = 0,ncol = 3))
#colnames(variableValueLabelsDf)<-c("variable_index","value","label")
#parse value labels
for(iCol in 1:length(columnFormat$names.orig)){
  #iCol<-24
  if(is.na(columnFormat$valueLabelColumn[iCol])) next
  svl<-columnFormat$valueLabelColumn[iCol]
  svlindex<-which(columnFormat$names.orig %in% svl)
  if(length(svlindex)>0){
    perm<-unique(na.omit(dbutil$importDataDf[,c(iCol,svlindex[1])]))
  }

  if(nrow(perm)!=length(unique(perm[,1]))){
    print(perm)
    warning(paste0("Value label combination inconsistency found for column ",columnFormat$names.new[iCol]))
  }

  variableValueLabelsDf <- rbind(variableValueLabelsDf,data.frame(variable_index=iCol,value=perm[,1],label=perm[,2]))
}

colnames(variableValueLabelsDf)<-c("variable_index","value","label")

#select actual columns to import
dbutil$importDataDf <- dbutil$importDataDf[,columnFormat$colsSelect]

#annotation tables
variableAnnotationDf <- data.frame(
  column_name=colnames(dbutil$importDataDf)
  )
variableAnnotationDf$variable_code <- NA_character_
variableAnnotationDf$variable_original_descriptor <- columnFormat$names.orig[columnFormat$colsSelect]
variableAnnotationDf$item_code <- variableAnnotationDf$column_name
variableAnnotationDf$variable_label <- as.character(variableLabels[columnFormat$colsSelect])
variableAnnotationDf$index <- 1:nrow(variableAnnotationDf)

uniqueVariableLabels <- unlist(unique(na.omit(variableAnnotationDf$variable_label)))

#Create item codes using text mining (tm package)
n_item_code_raw<-gsub(pattern = "[^a-z0-9 ]+", replacement = "\\1", x = tolower(uniqueVariableLabels)) #only letters, numbers, spaces
n_item_code_split<-strsplit(x = n_item_code_raw,split = " ")
trimmedStopwordsEnglish= gsub(pattern = "[^a-z0-9]+", replacement = "\\1", x = tm::stopwords("english"))
newItemAnnotationCodes<-data.frame(item_text=uniqueVariableLabels)
newItemAnnotationCodes$item_code<-lapply(n_item_code_split, function(x){
  paste0(x[!x %in% trimmedStopwordsEnglish],collapse = "")
})
newItemAnnotationCodes$item_code<-substr(newItemAnnotationCodes$item_code,1,30) #restrict to max 30 chars

#compare the new item codes (using the original label text) and detect which items to merge into one
merged<-merge(variableAnnotationDf,newItemAnnotationCodes, by.x = "variable_label",by.y = "item_text",all.x = T, all.y = F)
merged$item_code.x<-as.character(merged$item_code.x)
merged$item_code.y<-as.character(merged$item_code.y)
merged<-merged[order(merged$index),]

merged_agg<-aggregate(column_name ~ item_code.y,merged,length)
multiItems<-merged_agg[merged_agg$column_name>1,c("item_code.y")]
multiItems<-multiItems[multiItems!="null"] #remove strings denoting 'null'
multiItemsVarL<-merged$item_code.y %in% multiItems
merged$item_code<-ifelse(multiItemsVarL,merged$item_code.y,merged$item_code.x)
merged$variable_code<-ifelse(multiItemsVarL,merged$item_code.x,merged$variable_code)

variableAnnotationDf <- merged[,c("column_name","item_code","variable_code","variable_original_descriptor")]

itemAnnotationDf <- merged[,c("item_code","variable_label")]
colnames(itemAnnotationDf) <- c("item_code","item_text")
itemAnnotationDf <- unique(itemAnnotationDf)
itemAnnotationDf$assessment_type <- "questionnaire"
itemAnnotationDf$assessment_item_type_code <- ifelse(itemAnnotationDf$item_code %in% multiItems, "multi","single")
itemAnnotationDf$item_index <- 1:nrow(itemAnnotationDf)
itemAnnotationDf$item_documentation <- ""

dbutil$importDataAsTable(name = "_import_data_df",dbutil$importDataDf,temporary = F)
dbutil$importDataAsTable(name = "_item_annotation_df",itemAnnotationDf,temporary = F)
dbutil$importDataAsTable(name = "_variable_annotation_df",variableAnnotationDf,temporary = F)
dbutil$importDataAsTable(name = "_variable_value_labels_df",variableValueLabelsDf,temporary = F)

#CAGE
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"cage_covidcns_clean.rds"))
#View(dbutil$importDataDf)
dbutil$parseVariableLabels()
dbutil$formatImportColumnNames(prefixesToExcludeRegex = "cage\\.",deitemise = T)
dbutil$synchroniseVariableLabelTextForValueColumns()
dbutil$fixIdColumn()
dbutil$parseVariableValueLabels()
dbutil$interpretAndParseBooleanDataTypes() #this fails to interpret them as boolean because of multiple other values
dbutil$filterColumnsOnFormatting()
dbutil$createVariableAnnotation()
dbutil$amendVariableAnnotationFromVariableLabelText()
#View(dbutil$variableAnnotationDf)
dbutil$importDataAsTables()
dbutil$prepareImport(cohortCode = "covidcns", instanceCode = "2022",assessmentCode = "cage", assessmentVersionCode = "covidcns",cohortIdColumn = "id")
dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cage", assessmentVersionCode = "covidcns",stageCode = "bl",doAnnotate = T,addIndividuals = T,doInsert = T)

#CFS
dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"cfs_covidcns_clean.rds"))
#View(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cfq11", assessmentVersionCode = "covidcns", stageCode = "bl", prefixesToExcludeRegex = "cfs\\.", deitemise = T)

#SEX
dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"sex_gender_sexuality_covidcns_clean.rds"))
#View(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", prefixesToExcludeRegex = "dem\\.", deitemise = T)

#EDUCATION - highest qualification
dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"highest_qualification_covidcns_clean.rds"))
#View(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", prefixesToExcludeRegex = "dem\\.", deitemise = T)

#LANGUAGE
dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"language_covidcns_clean.rds"))
#View(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "covidcnsdem", assessmentVersionCode = "1", stageCode = "bl", prefixesToExcludeRegex = "dem\\.", deitemise = T)

#COGNITRON questionnaire
dbutil <- pgDatabaseUtilityClass(host="localhost", dbname="phenodb", user="postgres", port=65432, password= pwtemp)
dbutil$readImportData(filepath.rds = file.path(qImportFolderPath,"cognitron_outp_covidcns_clean.rds"))

#exclude a duplicate variable
# dbutil$importDataDf<-dbutil$importDataDf[,!colnames(dbutil$importDataDf)=="cognitron_outp.participant_difficulties_test_attempted.1_numeric"]

#View(dbutil$importDataDf)
# columnFormat <- phenodbr::formatImportColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToExcludeRegex = "cognitron_outp\\.",deitemise = T)
# colnames(dbutil$importDataDf)<-columnFormat$names.new


dbutil$parseVariableLabels()

#format column names
dbutil$formatImportColumnNames(prefixesToExcludeRegex = "cognitron_outp\\.", deitemise = T)

#synchronise variable text/label between value and label columns - only works for numeric columns missing labels
dbutil$synchroniseVariableLabelTextForValueColumns()

#fix ID
dbutil$fixIdColumn()

#variable value labels
dbutil$parseVariableValueLabels()

#select actual columns to import
dbutil$filterColumnsOnFormatting()

#annotation tables
dbutil$createVariableAnnotation()


dbutil$defaultAnnotateAndImportProcedure(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cognitronq", assessmentVersionCode = "2022", stageCode = "bl", itemCodeEndHead = F,  prefixesToExcludeRegex = "cognitron_outp\\.", parseItemsFromVariableLabelText = F, deitemise = T, prepare = F, import = F)



dbutil$importDataAsTables(temporary = F, itemAnnotation = F)
#This was then imported manually from the database because some issues with the R script - new bug?
#dbutil$importData(cohortCode = "covidcns", instanceCode = "2022", assessmentCode = "cognitronq", assessmentVersionCode = "2022", stageCode = "bl", doAnnotate = T, addIndividuals = T, doInsert = T)


#Test of export data functionality
res <- dbutil$selectExportData(cohortCode = "covidcns",instanceCode = "2022", assessmentCode="covidcnsdem",assessmentVersionCode="1")

#ERROR WHEN NULL ITEM AND VARIABLE LISTS!!! -- SEE THE create-adaptations.sql
q <- dbSendQuery(dbutil$connection,
                 "SELECT * FROM coh._create_current_assessment_item_variable_tview(
                      	met.get_cohort($1),
                      	met.get_cohortinstance($1,$2),
                      	met.get_assessment_item_variables(
                          assessment_code => $3,
                          assessment_version_code => $4,
                          assessment_item_code => $5,
                          assessment_variable_code_full => $6,
                          assessment_variable_code_original => $7
                      	)
                      )",
                 list("covidcns","2022","covidcnsdem","1",NA,NA,NA)
)
res<-dbFetch(q)
dbClearResult(q)
