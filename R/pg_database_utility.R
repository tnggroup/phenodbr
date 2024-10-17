


pgDatabaseUtilityClass <- setRefClass("pgDatabaseUtility",
                                             fields = list(
                                               dbname = "character",
                                               user = "character",
                                               host = "character",
                                               port = "numeric",
                                               statusCode = "numeric",
                                               cacheDb = "ANY",
                                               savedCachePath = "character",
                                               connection = "ANY",
                                               importDataDf = "ANY",
                                               exportDataDf = "ANY",
                                               itemMetaDataDf = "ANY",
                                               variableMetaDataDf = "ANY",
                                               itemAnnotationDf = "ANY",
                                               variableAnnotationDf = "ANY",
                                               valueAnnotationDf = "ANY",
                                               variableLabelsDf = "ANY",
                                               variableValueLabelsDf = "ANY",
                                               columnFormat = "ANY"
                                             ),
                                             methods = list
                                             (
                                               #this is the constructor as per convention
                                               initialize=function(dbname, host, user, port, password, doCache=T, askForPassword=F)
                                               {

                                                 dbname <<- dbname
                                                 host <<- host
                                                 user <<- user
                                                 port <<- port
                                                 if(askForPassword){
                                                   connection <<- dbConnect(RPostgres::Postgres(),
                                                                            dbname = dbname,
                                                                            host = host,
                                                                            port = port,
                                                                            user = user,
                                                                            password = rstudioapi::askForPassword(prompt = "Enter database password for specified user."))
                                                 } else {
                                                   connection <<- dbConnect(RPostgres::Postgres(),
                                                             dbname = dbname,
                                                             host = host,
                                                             port = port,
                                                             user = user,
                                                             password = password)
                                                 }
                                                 savedCachePath <<- "./pgDatabaseUtilityCache.Rds"

                                                 if(doCache==T) cache()
                                               }
                                             )
)

# we can add more methods after creating the ref class (but not more fields!)

# cache frequently and rarely changing data from the server
pgDatabaseUtilityClass$methods(
  cache=function(refresh=F, write=T, cacheSaveFilePath=NULL){
    if(is.null(cacheSaveFilePath)){
      cacheSaveFilePath <- savedCachePath
    }

    if(file.exists(cacheSaveFilePath) && refresh == F){
      cacheDb <<- readRDS(file=cacheSaveFilePath)
    } else {
      cacheDb <<- c()

      #store cache data here

      if(write) {
        saveRDS(cacheDb,file = cacheSaveFilePath)
        savedCachePath <<- cacheSaveFilePath
      }

    }
  }
)

#this is not injection safe
pgDatabaseUtilityClass$methods(
  setSearchPath=function(searchPath){
    #searchPath<-"dat_cohort"
    c <- paste0("SET search_path TO ",paste(searchPath, sep = ","))
    #cat(c)
    q <- dbSendQuery(connection, c)
  }
)

pgDatabaseUtilityClass$methods(
  setSearchPath.standard=function(){
    setSearchPath("\"$user\", public")
  }
)

pgDatabaseUtilityClass$methods(
  getPGTempSchema=function(cohort){
    q <- dbSendQuery(connection,
                     "SELECT nspname FROM pg_namespace WHERE oid  =  pg_my_temp_schema()")
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  getCohort=function(cohort){
    q <- dbSendQuery(connection,
                     "SELECT met.get_cohort($1)",
                     list(cohort))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)

pgDatabaseUtilityClass$methods(
  getCohortinstance=function(cohort,instance){
    q <- dbSendQuery(connection,
                     "SELECT met.get_cohortinstance($1,$2)",
                     list(cohort,instance))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)

pgDatabaseUtilityClass$methods(
  getCohortstage=function(cohort,stage){
    q <- dbSendQuery(connection,
                     "SELECT met.get_cohortstage($1,$2)",
                     list(cohort,stage))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)

pgDatabaseUtilityClass$methods(
  getAssessment=function(assessmentCode,assessmentVersionCode=NA){
    if(is.na(assessmentVersionCode))
    {
      q <- dbSendQuery(connection,
                       "SELECT met.get_assessment($1)",
                       list(assessmentCode))
    } else {
      q <- dbSendQuery(connection,
                       "SELECT met.get_assessment($1,$2)",
                       list(assessmentCode,assessmentVersionCode))
    }
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)

pgDatabaseUtilityClass$methods(
  getAssessmentItemType=function(assessmentType,assessmentItemTypeCode){
    q <- dbSendQuery(connection,
                       "SELECT met.get_assessment_item_type($1,$2)",
                       list(assessmentType,assessmentItemTypeCode))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)


pgDatabaseUtilityClass$methods(
  x_selectAssessmentItemByFuzzyItemText=function(assessmentId,assessmentItemSourceText){

    q <- dbSendQuery(connection,
                       "SELECT met._select_assessment_item_by_fuzzy_item_text($1,$2)",
                       list(assessmentId,assessmentItemSourceText))

    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA)
  }
)

pgDatabaseUtilityClass$methods(
  x_getAssessmentItem=function(assessmentId,assessmentItemCode){

    q <- dbSendQuery(connection,
                     "SELECT met._get_assessment_item($1,$2)",
                     list(assessmentId,assessmentItemCode))

    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_integer_)
  }
)

pgDatabaseUtilityClass$methods(
  constructCohortinstanceAssessmentTableName=function(cohortCode,instanceCode,assessmentCode,assessmentVersionCode="",tableIndex=0){
    q <- dbSendQuery(connection,
                     "SELECT met.construct_cohortinstance_table_name($1,$2,$3,$4,$5)",
                     list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,tableIndex))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  constructCohortinstanceAssessmentColumnName=function(assessmentItemCode,columnCode){
    q <- dbSendQuery(connection,
                     "SELECT met.construct_cohortinstance_column_name($1,$2)",
                     list(assessmentItemCode,columnCode))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  verifyCohortinstanceAssessment=function(cohortCode,instanceCode,assessmentCode,assessmentVersionCode=""){
    q <- dbSendQuery(connection,
                     "SELECT met.verify_cohortinstance_assessment($1,$2,$3,$4)",
                     list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)


pgDatabaseUtilityClass$methods(
  x_checkCohortinstanceAssessmentItemVariable=function(cohortinstanceId,assessmentId,assessmentItemId, assessmentItemVariableId){
    q <- dbSendQuery(connection,
                     "SELECT met._check_cohortinstance_assessment_item_variable($1,$2,$3,$4)",
                     list(cohortinstanceId,assessmentId,assessmentItemId,assessmentItemVariableId))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  x_checkCohortinstanceAssessmentItemVariableFromColumnName=function(cohortinstanceId,assessmentId,columnName){
    q <- dbSendQuery(connection,
                     "SELECT met._check_cohortinstance_assessment_item_variable_from_column_name($1,$2,$3)",
                     list(cohortinstanceId,assessmentId,columnName))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  parseAssessmentItemCodeFromColumnName=function(columnName){
    q <- dbSendQuery(connection,
                     "SELECT met.parse_assessment_item_code_from_column_name($1)",
                     list(columnName))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)

pgDatabaseUtilityClass$methods(
  parseAssessmentItemVariableCodeFromColumnName=function(columnName){
    q <- dbSendQuery(connection,
                     "SELECT met.parse_assessment_item_variable_code_from_column_name($1)",
                     list(columnName))
    res<-dbFetch(q)
    dbClearResult(q)
    if(length(res)>0) return(res[[1]]) else return(NA_character_)
  }
)


pgDatabaseUtilityClass$methods(
  selectCohortinstanceAssessmentItemVariableInventory=function(cohortinstanceId,assessmentId){
    q <- dbSendQuery(connection,
                     "SELECT * FROM met.cohort_inventory WHERE cohortinstance_id=$1 AND assessment_id=$2",
                     list(cohortinstanceId,assessmentId))
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentVariableAnnotation=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_variable_annotation")
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentItemAnnotation=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_item_annotation")
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataMeta=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_meta")
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)


pgDatabaseUtilityClass$methods(
  readImportData=function(filepath.rds=NULL, dataframe=NULL){

    importDataDf <<- NULL

    if(!is.null(filepath.rds)){
      importDataDf <<- as.data.frame(readRDS(file = file.path(filepath.rds)))
    } else if(!is.null(dataframe)){
      importDataDf <<- as.data.frame(dataframe)
    }

  }
)

#should be used with care as it passes the data by value rather than reference
pgDatabaseUtilityClass$methods(
  importDataAsTable=function(name,df,temporary = T){

    if(!is.null(df)){
      if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= temporary)
      dbCreateTable(conn = connection, name = name, fields = df, temporary = temporary)
      dbAppendTable(conn = connection, name = name, value = df)
    }

  }
)



pgDatabaseUtilityClass$methods(
  importDataAsTables=function(temporary = T, itemAnnotation=T, variableAnnotation=T, variableValueLabels=T, excludeColIndexFrom=NULL, excludeColIndexTo=NULL){

    if(!is.null(nrow(importDataDf))){ #working null check for object
      #main import data
      name<-"_import_data_df"
      if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= temporary)
      if(!is.null(excludeColIndexFrom)){
        if(is.null(excludeColIndexTo)) excludeColIndexTo=ncol(importDataDf)
        colIndices <- 1:(excludeColIndexFrom-1)
        if(excludeColIndexTo < ncol(importDataDf)) colIndices <- c(colIndices,excludeColIndexTo:ncol(importDataDf))
        tempImport<-importDataDf[,colIndices]
        dbCreateTable(conn = connection, name = name, fields = tempImport, temporary = temporary)
        dbAppendTable(conn = connection, name = name, value = tempImport)
      } else {
        dbCreateTable(conn = connection, name = name, fields = importDataDf, temporary = temporary)
        dbAppendTable(conn = connection, name = name, value = importDataDf)
      }
    }

    #itemAnnotation
    name<-"_item_annotation_df"
    if(itemAnnotation & !is.null(nrow(itemAnnotationDf))){
      if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= temporary)
      dbCreateTable(conn = connection, name = name, fields = itemAnnotationDf, temporary = temporary)
      dbAppendTable(conn = connection, name = name, value = itemAnnotationDf)
    }

    if(variableAnnotation & !is.null(nrow(variableAnnotationDf))){
      #variableAnnotation
      name<-"_variable_annotation_df"
      if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= temporary)
      dbCreateTable(conn = connection, name = name, fields = variableAnnotationDf, temporary = temporary)
      dbAppendTable(conn = connection, name = name, value = variableAnnotationDf)
    }

    if(variableValueLabels & !is.null(nrow(variableValueLabelsDf))){
      #variableValueLabels
      name<-"_variable_value_labels_df"
      if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= temporary)
      dbCreateTable(conn = connection, name = name, fields = variableValueLabelsDf, temporary = temporary)
      dbAppendTable(conn = connection, name = name, value = variableValueLabelsDf)
    }
  }
)

# pgDatabaseUtilityClass$methods(
#   importDataAsTemporaryTable=function(name){
#     if(dbExistsTable(conn = connection, name = name)) dbRemoveTable(conn = connection, name = name, temporary= T)
#     dbCreateTable(conn = connection, name = name, fields = importData, temporary = T)
#     dbAppendTable(conn = connection, name = name, value = importData)
#   }
# )

#make sure you don't have any tables named as the temporary tables - they will precede the temp tables in the path when run from R and make the access right assignment fail!!!
pgDatabaseUtilityClass$methods(
  prepareImport=function(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,tableName="_import_data_df",cohortIdColumn,variableAnnotationTableName="_variable_annotation_df",itemAnnotationTableName="_item_annotation_df"){
    q <- dbSendQuery(connection,
                     "SELECT coh.prepare_import($1,$2,$3,$4,$5,$6,$7,$8)",
                     list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,tableName,cohortIdColumn,variableAnnotationTableName,itemAnnotationTableName))
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

#must be a member of phenodb_coworker
pgDatabaseUtilityClass$methods(
  importData=function(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,stageCode,tableName="_import_data_df",doAnnotate=F,addIndividuals=F,doInsert=F){
    q <- dbSendQuery(connection,
                     "SELECT coh.import_data($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                     list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,stageCode,tableName,doAnnotate,addIndividuals,doInsert))
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)


pgDatabaseUtilityClass$methods(
  selectImportDataMeta=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_meta"
                     )
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentItemStats=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_item_stats"
    )
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentItemVariableStats=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_item_variable_stats"
    )
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentItemAnnotation=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_item_annotation"
    )
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)

pgDatabaseUtilityClass$methods(
  selectImportDataAssessmentItemVariableAnnotation=function(){
    q <- dbSendQuery(connection,
                     "SELECT * FROM t_import_data_assessment_variable_annotation"
    )
    res<-dbFetch(q)
    dbClearResult(q)
    return(res)
  }
)


pgDatabaseUtilityClass$methods(
  parseVariableLabels=function(){
    variableLabelsDf <<- lapply(importDataDf,function(x)attr(x,which = "label", exact = T)[[1]])
  }
)


pgDatabaseUtilityClass$methods(
  parseVariableValueLabels=function(){
    variableValueLabelsDf <<- data.frame(matrix(data = NA,nrow = 0,ncol = 3))
    #colnames(variableValueLabelsDf)<-c("variable_index","value","label")
    #parse value labels
    for(iCol in 1:length(columnFormat$names.orig)){
      #iCol<-143
      if(is.na(columnFormat$valueLabelColumn[iCol])) next
      svl<-columnFormat$valueLabelColumn[iCol]
      svlindex<-which(columnFormat$names.orig %in% svl)
      perm<-NULL
      if(length(svlindex)>0){
        perm<-unique(na.omit(importDataDf[,c(iCol,svlindex[1])]))
      }

      if(nrow(perm)!=length(unique(perm[,1]))){
        print(perm)
        warning(paste0("Value label combination inconsistency found for column ",columnFormat$names.new[iCol]))
      }

      if(nrow(perm)>0) variableValueLabelsDf <<- rbind(variableValueLabelsDf,data.frame(variable_index=iCol,value=as.character(perm[,1]),label=as.character(perm[,2])))
    }

    colnames(variableValueLabelsDf) <<- c("variable_index","value","label")

  }
)


pgDatabaseUtilityClass$methods(
  formatImportColumnNames=function(prefixesToExcludeRegex=c(), prefixesToItemiseRegex=c(), suffixesToExcludeRegex=c(), deitemise=F, forceItem=NULL, maxVariableNameLength=30){
    columnFormat <<- formatStdColumnNames(columnNames = colnames(importDataDf),prefixesToExcludeRegex = prefixesToExcludeRegex, prefixesToItemiseRegex = prefixesToItemiseRegex, suffixesToExcludeRegex=suffixesToExcludeRegex, deitemise = deitemise, forceItem = forceItem, maxVariableNameLength = maxVariableNameLength)
    colnames(importDataDf) <<- columnFormat$names.new
  }
)

#this also sets the column id data to uppercase
pgDatabaseUtilityClass$methods(
  fixIdColumn=function(cohortIdColumn="id"){
    importDataDf[,c(cohortIdColumn)] <<- toupper(gsub(pattern = "[^A-Za-z0-9]+", replacement = "\\1", x = importDataDf[,c(cohortIdColumn)]))
  }
)


#synchronise item text/label between value and label column - only works for numeric columns missing labels
pgDatabaseUtilityClass$methods(
  synchroniseVariableLabelTextForValueColumns=function(){
    for(iCol in 1:length(columnFormat$names.orig)){
      #iCol<-46
      if(is.null(variableLabelsDf[iCol][[1]]) & !is.null(variableLabelsDf[columnFormat$valueLabelColumn[iCol]][[1]]) ){
        variableLabelsDf[iCol] <<- variableLabelsDf[columnFormat$valueLabelColumn[iCol]][[1]]
      }
    }
  }
)

#Change data type of two-category 1/0 columns to boolean true/false, based on the value space data from a sample (tail) of 2000 rows
pgDatabaseUtilityClass$methods(
  interpretAndParseBooleanDataTypes=function(){
    df.sample<-tail(importDataDf,n = 2000)
    if(nrow(df.sample)>99){ #only run if we have a sufficient sample to base the decision on. may still cause sparse variables to be misinterpreted here as boolean where there in fact are more values but not in the sample.
      for(iCol in 1:length(colnames(df))){
        #iCol<-24
        if(!is.numeric(df.sample[,iCol])) next
        if(sum(!is.na(df.sample[,iCol]))>10){
          vals<-na.omit(unique(df.sample[,iCol]))
          vals<-vals[order(vals)]
          if(length(vals)>1 & length(vals) < 3 & (0 %in% vals | 1 %in% vals)) importDataDf[,iCol] <<- (importDataDf[,iCol]==1)
        }
      }
    }
  }
)


pgDatabaseUtilityClass$methods(
  createVariableAnnotation=function(parseItems=F){
    variableAnnotationDf <<- data.frame(
      column_name=columnFormat$names.new
    )

    variableAnnotationDf$variable_original_descriptor <<- columnFormat$names.orig
    if(length(variableLabelsDf) == nrow(variableAnnotationDf)){
      variableAnnotationDf$variable_label <<- as.character(variableLabelsDf)
    } else {
      variableAnnotationDf$variable_label <<- NA_character_
    }
    variableAnnotationDf$variable_index <<- 1:nrow(variableAnnotationDf)

    if(parseItems){
      variableAnnotationDf$item_code <<- sub(pattern = paste0("^([a-z0-9]+)_.*"), replacement = "\\1", x = columnFormat$names.new)
      variableAnnotationDf$variable_code <<- sub(pattern = paste0("^.+_([a-z0-9]+)"), replacement = "\\1", x = columnFormat$names.new)
      variableAnnotationDf$variable_code <<- ifelse(variableAnnotationDf$item_code == variableAnnotationDf$variable_code, NA_character_,variableAnnotationDf$variable_code)

    } else {
      variableAnnotationDf$item_code <<- variableAnnotationDf$column_name
      variableAnnotationDf$variable_code <<- NA_character_
    }

    variableAnnotationDf$variable_documentation <<- ""
    variableAnnotationDf$variable_unit <<- NA_character_
    variableAnnotationDf$variable_data_type <<- NA_character_
  }
)

pgDatabaseUtilityClass$methods(
  createBasicItemAnnotationFromVariableAnnotation=function(itemAnnotationAssessmentType="questionnaire"){

    aggVarAnnot<-aggregate(x = variableAnnotationDf, by = list(variableAnnotationDf$item_code), FUN = head, 1)
    itemAnnotationDf<<-data.frame(item_code=aggVarAnnot$item_code, item_text=aggVarAnnot$variable_label)
    itemAnnotationDf$assessment_type <<- itemAnnotationAssessmentType

    merged_agg<-aggregate(column_name ~ item_code,dbutil$variableAnnotationDf,length)
    multiItems<-merged_agg[merged_agg$column_name>1,c("item_code")]
    multiItems<-multiItems[multiItems!="null"] #remove strings denoting 'null'

    itemAnnotationDf$assessment_item_type_code <<- ifelse(itemAnnotationDf$item_code %in% multiItems, "multi","single")
    itemAnnotationDf$item_index<<-1:nrow(itemAnnotationDf)
    itemAnnotationDf$item_documentation <<-""
  }
)

#This also creates an (new) item annotation
#TODO items created here do not consider duplicate item names
pgDatabaseUtilityClass$methods(
  amendVariableAnnotationFromVariableLabelText=function(itemAnnotationAssessmentType="questionnaire",itemCodeEndHead=T){
    uniqueVariableLabels <- unlist(unique(na.omit(variableAnnotationDf$variable_label)))

    #Create item codes using text mining (tm package)
    n_item_code_raw<-gsub(pattern = "[^a-z0-9 ]+", replacement = "\\1", x = tolower(uniqueVariableLabels)) #only letters, numbers, spaces
    n_item_code_split<-strsplit(x = n_item_code_raw,split = " ")
    trimmedStopwordsEnglish= gsub(pattern = "[^a-z0-9]+", replacement = "\\1", x = tm::stopwords("english"))
    newItemAnnotationCodes<-data.frame(item_text=uniqueVariableLabels)
    newItemAnnotationCodes$item_code<-lapply(n_item_code_split, function(x){
      paste0(x[!x %in% trimmedStopwordsEnglish],collapse = "")
    })
    #restrict to max 30 chars
    if(itemCodeEndHead){
      newItemAnnotationCodes$item_code<-substr(newItemAnnotationCodes$item_code,1,30)
    } else {
      newItemAnnotationCodes$item_code<-substr(newItemAnnotationCodes$item_code,nchar(newItemAnnotationCodes$item_code) - 30,nchar(newItemAnnotationCodes$item_code))
    }

    #compare the new item codes (using the original label text) and detect which items to merge into one
    merged<-merge(variableAnnotationDf,newItemAnnotationCodes, by.x = "variable_label",by.y = "item_text",all.x = T, all.y = F)
    merged$item_code.x<-as.character(merged$item_code.x)
    merged$item_code.y<-as.character(merged$item_code.y)
    merged<-merged[order(merged$variable_index),]

    merged_agg<-aggregate(column_name ~ item_code.y,merged,length)
    multiItems<-merged_agg[merged_agg$column_name>1,c("item_code.y")]
    multiItems<-multiItems[multiItems!="null"] #remove strings denoting 'null'
    multiItemsVarL<-merged$item_code.y %in% multiItems
    merged$item_code<-ifelse(multiItemsVarL,merged$item_code.y,merged$item_code.x)
    merged$variable_code<-ifelse(multiItemsVarL,merged$item_code.x,merged$variable_code)

    variableAnnotationDf[variableAnnotationDf$column_name %in% merged$column_name,c("column_name","variable_original_descriptor","variable_label", "variable_index","item_code","variable_code")] <<- merged[,c("column_name","variable_original_descriptor","variable_label", "variable_index","item_code","variable_code")]

    itemAnnotationDf <<- merged[,c("item_code","variable_label")]
    colnames(itemAnnotationDf) <<- c("item_code","item_text")
    itemAnnotationDf <<- unique(itemAnnotationDf)
    itemAnnotationDf$assessment_type <<- itemAnnotationAssessmentType
    itemAnnotationDf$assessment_item_type_code <<- ifelse(itemAnnotationDf$item_code %in% multiItems, "multi","single")
    itemAnnotationDf$item_index <<- 1:nrow(itemAnnotationDf)
    itemAnnotationDf$item_documentation <<- ""

  }
)


pgDatabaseUtilityClass$methods(
  amendVariableAnnotationDataTypeIntegerFromData=function(){
    rtypes <- sapply(importDataDf,typeof)
    for(iVar in 1:nrow(variableAnnotationDf)){
      #iVar <- 4
      cVar <- variableAnnotationDf[iVar,]
      if(rtypes[cVar$column_name]=="double" & !inherits(importDataDf[,cVar$column_name],c("Date","POSIXt"))){
        if(all(na.omit(importDataDf[,cVar$column_name]%%1==0))) variableAnnotationDf[iVar,"variable_data_type"] <<- "integer"
      }
    }

  }
)


pgDatabaseUtilityClass$methods(
  castVariablesAsAnnotated=function(){
    rtypes <- sapply(importDataDf,typeof)
    for(iCol in 1:ncol(importDataDf)){
      #iCol<-1
      cAn<-variableAnnotationDf[variableAnnotationDf$column_name==colnames(importDataDf)[iCol],]
      if(is.na(cAn[1,]$variable_data_type)) next
      if(
        (rtypes[iCol]=='character' & nrow(cAn)>0 & !(cAn[1,]$variable_data_type=="text" | cAn[1,]$variable_data_type=="character varying")) #character
        |
        (rtypes[iCol]=='double' & nrow(cAn)>0 & !(cAn[1,]$variable_data_type=="double precision")) #double
        |
        (rtypes[iCol]=='integer' & nrow(cAn)>0 & !(cAn[1,]$variable_data_type=="integer")) #integer
        ){
        cat("\n",cAn$item_code," ", cAn$variable_code)
        #test - new type
        #cat(typeof(as.numeric(dbutil$importDataDf[,iCol])))
        if(cAn$variable_data_type=="integer"){
          importDataDf[,iCol] <<- as.integer(importDataDf[,iCol])
        } else if (cAn$variable_data_type=="double precision"){
          importDataDf[,iCol] <<- as.numeric(importDataDf[,iCol])
        }

        cat("\nNew type: ",typeof(importDataDf[,iCol]))

      } #numeric types will be cast as part of the database insert later also
    }

  }
)

pgDatabaseUtilityClass$methods(
  filterColumnsOnFormatting=function(){
    importDataDf <<- importDataDf[,columnFormat$colsSelect]
    variableLabelsDf <<- variableLabelsDf[columnFormat$colsSelect]
    columnFormat <<- columnFormat[columnFormat$colsSelect,]
  }
)

pgDatabaseUtilityClass$methods(
  compareColumnFormatWithSelectedExportData=function(){
  list(
   newNotInSelected = columnFormat[columnFormat$colsSelect==TRUE,]$names.new[!columnFormat[columnFormat$colsSelect==TRUE,]$names.new %in% colnames(exportDataDf)],
   selectedNotInNew = colnames(exportDataDf)[!colnames(exportDataDf) %in% columnFormat[dbutil$columnFormat$colsSelect==TRUE,]$names.new]
  )
  }
)

pgDatabaseUtilityClass$methods(
  renameAnnotatedVariableColumn=function(toChangeVariableColumnName,targetVariableColumnName,forceVariable=T,forceItem=NULL
){

    if(any(variableAnnotationDf$item_code==toChangeVariableColumnName & variableAnnotationDf$column_name==toChangeVariableColumnName)){

      itemAnnotationDf[itemAnnotationDf$item_code==toChangeVariableColumnName,c("item_code")]<<-targetVariableColumnName

    }

    if(forceVariable){
      variableAnnotationDf[variableAnnotationDf$column_name==toChangeVariableColumnName,c("variable_code")]<<-targetVariableColumnName
    }

    if(!is.null(forceItem)){
      variableAnnotationDf[variableAnnotationDf$column_name==toChangeVariableColumnName,c("item_code")]<<-forceItem
     } else {
      variableAnnotationDf[variableAnnotationDf$item_code==toChangeVariableColumnName & variableAnnotationDf$column_name==toChangeVariableColumnName,c("item_code")]<<-targetVariableColumnName
     }

    #sync item and variable codes - remove variable code if equal to item code
    variableAnnotationDf[!is.na(variableAnnotationDf$variable_code) & !is.na(variableAnnotationDf$item_code) & variableAnnotationDf$variable_code==variableAnnotationDf$item_code,c("variable_code")]<<-NA

    variableAnnotationDf[variableAnnotationDf$column_name==toChangeVariableColumnName,c("column_name")]<<-targetVariableColumnName

    columnFormat$names.new[columnFormat$names.new==toChangeVariableColumnName]<<-targetVariableColumnName

    colnames(importDataDf)[colnames(importDataDf)==toChangeVariableColumnName]<<-targetVariableColumnName

  }
)

#this should not be needed for some of the temporary tables, as they should be cleaned up by the responsible script
# t_export_data is not cleanded up though and will be removed by this
pgDatabaseUtilityClass$methods(
  cleanup=function(){
    q <- dbSendQuery(connection,"DROP TABLE IF EXISTS t_prepare_import_data_settings CASCADE"
    )
    dbClearResult(q)
    q <- dbSendQuery(connection,"DROP TABLE IF EXISTS t_import_data_assessment_variable_annotation CASCADE"
    )
    dbClearResult(q)
    q <- dbSendQuery(connection,"DROP TABLE IF EXISTS t_import_data_assessment_item_annotation CASCADE"
    )
    dbClearResult(q)
    q <- dbSendQuery(connection,"DROP VIEW IF EXISTS t_import_data_meta CASCADE"
    )
    dbClearResult(q)
    q <- dbSendQuery(connection,"DROP VIEW IF EXISTS t_export_data CASCADE"
    )
    dbClearResult(q)
  }
)

#must be a member of phenodb_coworker
pgDatabaseUtilityClass$methods(
  defaultAnnotateAndImportProcedure=function(cohortCode, instanceCode, assessmentCode, assessmentVersionCode, stageCode, itemAnnotationAssessmentType="questionnaire",itemCodeEndHead=T, cohortIdColumn="id", interpretBooleanDatatypeFromData=F, parseItemsFromVariableLabelText=T, prefixesToExcludeRegex=c(), suffixesToExcludeRegex=c(), deitemise=F, forceItem=NULL, prepare=T, import=T, doAnnotate = T, addIndividuals = T, doInsert = T, parseAndBusiness=T){

    if(parseAndBusiness | is.null(variableLabelsDf)){
      #variable labels
      parseVariableLabels()

      #format column names
      formatImportColumnNames(prefixesToExcludeRegex = prefixesToExcludeRegex, suffixesToExcludeRegex=suffixesToExcludeRegex, deitemise = deitemise)

      #synchronise variable text/label between value and label columns - only works for numeric columns missing labels
      synchroniseVariableLabelTextForValueColumns()

      #fix ID
      fixIdColumn(cohortIdColumn = cohortIdColumn)

      #variable value labels
      parseVariableValueLabels()

      #Change data type of two-category 1/0 columns to boolean true/false, based on the value space data from a sample (tail) of 2000 rows
      if(interpretBooleanDatatypeFromData) interpretAndParseBooleanDataTypes()

      #select actual columns to import
      filterColumnsOnFormatting()

      #annotation tables
      createVariableAnnotation()
      createBasicItemAnnotationFromVariableAnnotation()

      #update item categorisation from variable label texts, create item annotation
      if(parseItemsFromVariableLabelText) amendVariableAnnotationFromVariableLabelText(itemAnnotationAssessmentType,itemCodeEndHead) #THIS IS APPARENTLY WIP - fix when working on new qualtrics imports!!! Is it still WIP?

      #determine data type from data
      amendVariableAnnotationDataTypeIntegerFromData()
    }

    #cast according to variable annotation - based on amended variable annotations with new data types
    if(prepare) castVariablesAsAnnotated()

    #import all tables
    if(prepare) importDataAsTables()

    #perform database preparation procedures for import
    if(prepare) prepareImport(cohortCode = cohortCode, instanceCode = instanceCode, assessmentCode = assessmentCode, assessmentVersionCode = assessmentVersionCode, cohortIdColumn = cohortIdColumn)

    #perform database import
    if(import) importData(cohortCode = cohortCode, instanceCode = instanceCode, assessmentCode = assessmentCode, assessmentVersionCode = assessmentVersionCode, stageCode = stageCode, doAnnotate = doAnnotate, addIndividuals = addIndividuals, doInsert = doInsert)

  }
)

pgDatabaseUtilityClass$methods(
  selectVariableMetaData=function(cohortCode,instanceCode,assessmentCode=NULL,assessmentVersionCode=NULL,assessmentItemCodeList=NULL,assessmentVariableCodeFullList=NULL,assessmentVariableCodeOriginalList=NULL){

    sQuery <- ""
    lArguments <- c()
    if(is.null(assessmentCode) | is.null(assessmentVersionCode)){
      sQuery <- paste0("SELECT * FROM met.select_assessment_item_variable_meta(
                        cohort_code => $1,
                      	instance_code => $2 )")
      lArguments <- list(cohortCode,instanceCode)
    } else {
      sQuery <- paste0("SELECT * FROM met.select_assessment_item_variable_meta(
                        cohort_code => $1,
                      	instance_code => $2,
                      	assessment_code => $3,
                      	assessment_version_code => $4,
                      	assessment_item_code => ",asPgsqlTextArray(assessmentItemCodeList),",
                      	assessment_variable_code_full => ",asPgsqlTextArray(assessmentVariableCodeFullList),",
                      	assessment_variable_code_original => ",asPgsqlTextArray(assessmentVariableCodeOriginalList),"
                      )")
      lArguments <- list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode)
    }

    q <- dbSendQuery(connection,
                     sQuery,
                     lArguments
    )

    variableMetaDataDf <<- dbFetch(q)
    dbClearResult(q)
    return(variableMetaDataDf)
  }
)

pgDatabaseUtilityClass$methods(
  selectItemMetaData=function(cohortCode,instanceCode,assessmentCode=NULL,assessmentVersionCode=NULL,assessmentItemCodeList=NULL,assessmentVariableCodeFullList=NULL,assessmentVariableCodeOriginalList=NULL){

    sQuery <- ""
    lArguments <- c()
    if(is.null(assessmentCode) | is.null(assessmentVersionCode)){
      sQuery <- paste0("SELECT * FROM met.select_assessment_item_meta(
                        cohort_code => $1,
                      	instance_code => $2 )")
      lArguments <- list(cohortCode,instanceCode)
    } else {
      sQuery <- paste0("SELECT * FROM met.select_assessment_item_meta(
                        cohort_code => $1,
                      	instance_code => $2,
                      	assessment_code => $3,
                      	assessment_version_code => $4,
                      	assessment_item_code => ",asPgsqlTextArray(assessmentItemCodeList),",
                      	assessment_variable_code_full => ",asPgsqlTextArray(assessmentVariableCodeFullList),",
                      	assessment_variable_code_original => ",asPgsqlTextArray(assessmentVariableCodeOriginalList),"
                      )")
      lArguments <- list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode)
    }

    q <- dbSendQuery(connection,
                     sQuery,
                     lArguments
    )

    itemMetaDataDf <<- dbFetch(q)
    dbClearResult(q)
    return(itemMetaDataDf)
  }
)

pgDatabaseUtilityClass$methods(
  selectExportData=function(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,assessmentItemCodeList=NULL,assessmentVariableCodeFullList=NULL,assessmentVariableCodeOriginalList=NULL,joinSec=FALSE){

    sQuery <- ""
    lArguments <- c()

    sQuery <- paste0("SELECT * FROM coh.create_current_assessment_item_variable_tview(
                      cohort_code => $1,
                    	instance_code => $2,
                    	assessment_code => $3,
                    	assessment_version_code => $4,
                    	assessment_item_code => ",asPgsqlTextArray(assessmentItemCodeList),",
                    	assessment_variable_code_full => ",asPgsqlTextArray(assessmentVariableCodeFullList),",
                    	assessment_variable_code_original => ",asPgsqlTextArray(assessmentVariableCodeOriginalList),",
                    	join_sec => ",joinSec,"
                    )")
    lArguments <- list(cohortCode,instanceCode,assessmentCode,assessmentVersionCode)

    q <- dbSendQuery(connection,
                     sQuery,
                     lArguments
                     )
    res<-dbFetch(q)
    dbClearResult(q)

    q <- dbSendQuery(connection,"SELECT * FROM t_export_data")
    exportDataDf <<- dbFetch(q)
    dbClearResult(q)

    #automatically fetch the corresponding metadata as well
    selectVariableMetaData(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,assessmentItemCodeList,assessmentVariableCodeFullList,assessmentVariableCodeOriginalList)
    selectItemMetaData(cohortCode,instanceCode,assessmentCode,assessmentVersionCode,assessmentItemCodeList,assessmentVariableCodeFullList,assessmentVariableCodeOriginalList)

    return("The data retrieved by this function is stored in the exportDataDf with the corresponding metadata in itemMetaDataDf and variableMetaDataDf.")
  }
)
