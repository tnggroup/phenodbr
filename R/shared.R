#general shared package utilities

#parses and formats the provided column names to the database standard
formatImportColumnNames=function(columnNames,prefixesToExcludeRegex=c(), deitemise=F, forceItem=NULL, columnNameLength=30){

  #columnNames<-colnames(df)
  #prefixesToExcludeRegex = "dem_1\\."
  columnNames.orig<-columnNames

  colsNumeric<-grepl(pattern = ".+_numeric$",x = columnNames, ignore.case = T)

  valueLabels<-data.frame(valueColumn=columnNames.orig[colsNumeric])
  valueLabels$valueLabelColumn<-gsub(pattern = "(.+)_numeric$",replacement = "\\1", x = valueLabels$valueColumn, ignore.case = T)
  valueLabels<-merge(data.frame(valueColumn=columnNames.orig),valueLabels,by = "valueColumn", all.x = T)
  rownames(valueLabels)<-valueLabels$valueColumn
  valueLabels<-valueLabels[columnNames.orig,]
  colsValueLabels<-columnNames.orig %in% valueLabels$valueLabelColumn


  #add label suffix to label column names
  columnNames[colsValueLabels] <- paste0(columnNames[colsValueLabels],"l")


  colsSelect<-!colsValueLabels  #add more logic here when additional column types


  #per column name
  for(iCol in 1:length(columnNames)){
    #iCol<-5
    cName<-columnNames[iCol]
    #parse column name further
    ##exclude prefixes if any
    if(length(prefixesToExcludeRegex)>0){
      for(iPat in 1:length(prefixesToExcludeRegex)){
        #iPat<-1
        cName<-gsub(pattern = paste0("^",prefixesToExcludeRegex[iPat],"(.+)"),replacement = "\\1", x = cName)
      }
    }

    ##exclude numeric suffix
    cName<-gsub(pattern = paste0("(.+)_numeric$"),replacement = "\\1", x = cName, ignore.case = T)
    columnNames[iCol]<-cName
  }

  if(deitemise) columnNames<-gsub(pattern = "[_]",replacement = "", x = columnNames)

  if(!is.null(forceItem)) columnNames<-paste0(forceItem,"_",columnNames)

  #trim unwanted characters
  columnNames<-gsub(pattern = "[^A-Za-z0-9_]",replacement = "", x = columnNames) #includes the _ character to accomodate the item categorisation
  columnNames<-gsub(pattern = "[\\.]",replacement = "", x = columnNames)

  #case
  columnNames<-substr(tolower(columnNames),start =nchar(columnNames)-columnNameLength, stop=nchar(columnNames)) #take the tail rather than the head to accommodate tail numbering

  #return(list(colsSelect=colsSelect, names.new=columnNames, names.orig=columnNames.orig, colsValueLabels=colsValueLabels, valueLabelColumn=valueLabels$valueLabelColumn))
  return(data.frame(colsSelect=colsSelect, names.new=columnNames, names.orig=columnNames.orig, colsValueLabels=colsValueLabels, valueLabelColumn=valueLabels$valueLabelColumn)) #experimental
}

asPgsqlTextArray=function(listToParse=c()){
  if(length(listToParse)<1) return("ARRAY[]::character varying(100)[]")
  modifiedList<-unlist(lapply(listToParse, function(x){paste0("'",x,"'")}))
  return(paste0("ARRAY[",paste(modifiedList,collapse = ","),"]"))
}


