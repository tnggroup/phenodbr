#COVID-CNS specific routines
covidcnsReadIDP=function(folderpathIDP,folderpathIDPMeta){
  #test
  #folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/TRAVEL_processed")
  #folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")
  #library(data.table)

  folderpathIDP<-normalizePath(folderpathIDP,mustWork = T)
  folderpathIDPMeta<-normalizePath(folderpathIDPMeta,mustWork = T)
  folderpathFMRI<-file.path(folderpathIDP,"fMRI")
  folderpathIDPfiles<-file.path(folderpathIDP,"IDP_files")

  #IDP_files
  bbFiles<-file.path(folderpathIDPfiles,list.files(path = folderpathIDPfiles, pattern = paste0("^bb_.+\\.txt")))
  filepathIDP<-file.path(folderpathIDPfiles,"IDPs.txt")
  filepathFSIDP<-file.path(folderpathIDPfiles,"FS_IDPs.txt")

  #IDPMetaFiles
  filepathIDPHeader<-file.path(folderpathIDPMeta,"IDPs_header.txt")
  filepathIDPInfo<-file.path(folderpathIDPMeta,"IDPs_info.txt")
  filepathIDPNames<-file.path(folderpathIDPMeta,"idp_names.txt")
  filepathFSIDPHeader<-file.path(folderpathIDPMeta,"FS_IDPs_header.txt")
  filepathFSIDPInfo<-file.path(folderpathIDPMeta,"FS_IDPs_info.txt")

  #dataframes
  lIDPHeader <- unlist(read.table(file = filepathIDPHeader, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T))
  dfIDPInfo <- read.table(file = filepathIDPInfo, header = F, encoding = "UTF8", sep = "\t", blank.lines.skip = T, fill = T)
  colnames(dfIDPInfo)<-c("header","unit","datatype","description")
  setDT(dfIDPInfo)
  dfIDPNames <- read.table(file = filepathIDPNames, header = T, encoding = "UTF8", sep = "\t", blank.lines.skip = T, fill = T)
  setDT(dfIDPNames)
  lFSIDPHeader <- unlist(read.table(file = filepathFSIDPHeader, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T))
  #this file has a botched formatting
  #dfFSIDPInfo <- read.table(file = filepathFSIDPInfo, header = F, encoding = "UTF8", sep = "\t", blank.lines.skip = T, fill = T)

  lIDP <- unlist(read.table(file = filepathIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T))

  #this can't be processed yet
  lFSIDP <- unlist(read.table(file = filepathFSIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T))


  dfIDP <- data.frame(header=lIDPHeader)
  dfIDP$order<-1:nrow(dfIDP)
  setDT(dfIDP)
  #nrow(dfIDP)
  dfIDP<-dfIDPInfo[dfIDP, on=c("header")]
  dfIDP<-dfIDP[dfIDPNames, on=c(header=c("IDP_name")),c('IDP','UKB_ID_name','IDP_name','IDP_cat','IDP_category') := list(i.IDP,i.UKB_ID_name,i.IDP_name,i.IDP_cat,i.IDP_category)]

  #add visit? value - TEMPORARY!
  lIDP <- c(lIDP[1:1],NA,lIDP[2:length(lIDP)])
  dfIDP$d<-lIDP

  #transpose data to column variable dataframe
  dataToImport<-as.data.frame(matrix(data = NA,nrow = 0,ncol = nrow(dfIDP)))
  dataToImport[1,]<-dfIDP$d
  colnames(dataToImport)<-dfIDP$header


  return(list(importDataDf=dataToImport))

}

