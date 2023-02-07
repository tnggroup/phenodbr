#COVID-CNS specific routines
covidcnsReadIDP=function(folderpathIDP,folderpathIDPMeta){
  #test
  #folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ALL_202301")
  #folderpathIDPMeta<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/idp/IDPs_44k")
  #library(data.table)

  folderpathIDP<-normalizePath(folderpathIDP,mustWork = T)
  folderpathIDPMeta<-normalizePath(folderpathIDPMeta,mustWork = T)
  #folderpathFMRI<-file.path(folderpathIDP,"fMRI")
  #folderpathIDPfiles<-file.path(folderpathIDP,"IDP_files")






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



  #IDPs
  dfIDP<-NULL
  dfFSIDP<-NULL
  folderpathIDP.all<- paste(folderpathIDP,list.files(path = folderpathIDP, pattern = paste0("^CNS.+")),sep = '/')
  for(iIdp in 1:length(folderpathIDP.all)){
    #iIdp<-1

    folderpathIDPfiles<-folderpathIDP.all[iIdp]

    #IDP_files
    bbFiles<-file.path(folderpathIDPfiles,list.files(path = folderpathIDPfiles, pattern = paste0("^bb_.+\\.txt")))
    filepathIDP<-file.path(folderpathIDPfiles,"IDPs.txt")
    filepathFSIDP<-file.path(folderpathIDPfiles,"FS_IDPs.txt")

    if(file.exists(filepathIDP)){
      # sIDP<-readChar(filepathIDP,file.info(filepathIDP)$size)
      # for(iChar in 1:nchar(sIDP)){ #nchar(sFSIDP)
      #   cat("\n",iChar,":")
      #   s<-substr(sIDP,start = iChar, stop = iChar)
      #   cat(s)
      #   cat("[",utf8ToInt(s),"]")
      # }

      cdfIDP <- read.table(file = filepathIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T)
      #add visit (NA)? value - TEMPORARY?
      cdfIDP <- cbind(cdfIDP[,1],NA_character_,cdfIDP[,2:ncol(cdfIDP)])
      colnames(cdfIDP)<-dfIDPmeta$header
      if(is.null(dfIDP)){
        dfIDP<-cdfIDP
      } else {
        dfIDP<-rbind(dfIDP,cdfIDP)
      }
    }

    if(file.exists(filepathFSIDP)){
      # sFSIDP<-readChar(filepathFSIDP,file.info(filepathFSIDP)$size)
      # for(iChar in 1:nchar(sFSIDP)){ #nchar(sFSIDP)
      #   cat("\n",iChar,":")
      #   s<-substr(sFSIDP,start = iChar, stop = iChar)
      #   cat(s)
      #   cat("[",utf8ToInt(s),"]")
      # }

      cdfFSIDP <- read.table(file = filepathFSIDP, header = F, encoding = "UTF8", blank.lines.skip = T, fill = T)
      #add visit (NA)? value - TEMPORARY?
      cdfFSIDP <- cbind(cdfFSIDP[,1],NA_character_,cdfFSIDP[,2:ncol(cdfFSIDP)])
      colnames(cdfFSIDP)<-dfFSIDPmeta$header
      if(is.null(dfFSIDP)){
        dfFSIDP<-cdfFSIDP
      } else {
        dfFSIDP<-rbind(dfFSIDP,cdfFSIDP)
      }
    }

  }

  #TODO could possible merge IDP data here


  return(list(dfIDPmeta=dfIDPmeta,dfFSIDPmeta=dfFSIDPmeta,dfIDP=dfIDP,dfFSIDP=dfFSIDP))

}


covidcnsParseIDColumn=function(IDs){
  #IDs<-c("RGZ_CNS1005","RET_CNS02002","RJZ_CNS01009","RGZ_CNS1085")

  #remove the prefixes
  IDs<-substr(IDs,start = 5, stop = nchar(IDs))

  #add a zero to the 01-series with shorter length
  return(ifelse(nchar(IDs)==7 & substring(IDs, first = 4, last = 4)=="1", paste0("CNS0",substring(IDs, first = 4, last = nchar(IDs))),IDs))
}

