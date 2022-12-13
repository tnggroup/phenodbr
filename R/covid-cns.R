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
  #formatting is fixed now
  dfFSIDPInfo <- read.table(file = filepathFSIDPInfo, header = F, encoding = "UTF8", sep = "\t", blank.lines.skip = T, fill = T)
  colnames(dfFSIDPInfo)<-c("header","unit","datatype","description")
  setDT(dfFSIDPInfo)

  #dfIDPmeta
  dfIDPmeta <- data.frame(header=lIDPHeader)
  dfIDPmeta$order<-1:nrow(dfIDPmeta)
  setDT(dfIDPmeta)
  #nrow(dfIDPmeta)
  dfIDPmeta<-dfIDPInfo[dfIDPmeta, on=c("header")]
  dfIDPmeta<-dfIDPmeta[dfIDPNames, on=c(header=c("IDP_name")),c('IDP','UKB_ID_name','IDP_name','IDP_cat','IDP_category') := list(i.IDP,i.UKB_ID_name,i.IDP_name,i.IDP_cat,i.IDP_category)]

  #dfFSIDPmeta
  dfFSIDPmeta <- data.frame(header=lFSIDPHeader)
  dfFSIDPmeta$order<-1:nrow(dfFSIDPmeta)
  setDT(dfFSIDPmeta)
  #nrow(dfFSIDPmeta)
  dfFSIDPmeta<-dfFSIDPInfo[dfFSIDPmeta, on=c("header")]
  dfFSIDPmeta<-dfFSIDPmeta[dfIDPNames, on=c(header=c("IDP_name")),c('IDP','UKB_ID_name','IDP_name','IDP_cat','IDP_category') := list(i.IDP,i.UKB_ID_name,i.IDP_name,i.IDP_cat,i.IDP_category)]


  dfIDP <- read.table(file = filepathIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T)
  #add visit (NA)? value - TEMPORARY?
  dfIDP <- cbind(dfIDP[,1],NA,dfIDP[,2:ncol(dfIDP)])
  #lIDP <- c(lIDP[1:1],NA,lIDP[2:length(lIDP)])
  colnames(dfIDP)<-dfIDPmeta$header

  #this can't be processed yet - maybe now after newly formatted info file?
  dfFSIDP <- read.table(file = filepathFSIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T)
  #add visit (NA)? value - TEMPORARY?
  dfFSIDP <- cbind(dfFSIDP[,1],NA,dfFSIDP[,2:ncol(dfFSIDP)])
  colnames(dfFSIDP)<-dfFSIDPmeta$header

  return(list(dfIDPmeta=dfIDPmeta,dfFSIDPmeta=dfFSIDPmeta,dfIDP=dfIDP,dfFSIDP=dfFSIDP))

}

