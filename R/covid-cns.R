#COVID-CNS specific routines
covidcnsReadIDP=function(folderpathIDP,folderpathIDPMeta){
  #test
  #folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ALL_202305")
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


  #non tabular data, preparations
  nontabMeta<-data.frame(code=c(
    "bb_IDP_all_align_to_T1",
    "bb_IDP_diff_autoPtx",
    "bb_IDP_diff_eddy_outliers",
    "bb_IDP_diff_TBSS",
    "bb_IDP_func_head_motion",
    "bb_IDP_func_task_activation",
    "bb_IDP_func_TSNR",
    "bb_IDP_subject_centre",
    "bb_IDP_subject_COG_table",
    "bb_IDP_SWI_T2star",
    "bb_IDP_T1_align_to_std",
    "bb_IDP_T1_FIRST_vols",
    "bb_IDP_T1_GM_parcellation",
    "bb_IDP_T1_noise_ratio",
    "bb_IDP_T1_SIENAX",
    "bb_IDP_T2_FLAIR_WMH"))
  #nontabMeta$filePath<-file.path(folderpathIDP,paste0(nontabMeta$code,".txt"))
  rownames(nontabMeta)<-nontabMeta$code

  #IDPs
  dfIDP<-NULL
  dfFSIDP<-NULL
  lNontab<-c()
  folderpathIDP.all<- paste(folderpathIDP,list.files(path = folderpathIDP, pattern = paste0("^CNS.+")),sep = '/')
  for(iIdp in 1:length(folderpathIDP.all)){
    #iIdp<-2

    folderpathIDPfiles<-folderpathIDP.all[iIdp]

    #IDP_files
    #bbFiles<-file.path(folderpathIDPfiles,list.files(path = folderpathIDPfiles, pattern = paste0("^bb_.+\\.txt")))
    filepathIDP<-file.path(folderpathIDPfiles,"IDPs.txt")
    filepathFSIDP<-file.path(folderpathIDPfiles,"FS_IDPs.txt")

    cNontabMeta<-nontabMeta
    cNontabMeta$filePath<-file.path(folderpathIDPfiles,paste0(cNontabMeta$code,".txt"))
    for(iNontab in 1:nrow(cNontabMeta)){
      #iNontab <- 1
      cNontab <- read.table(file = cNontabMeta[iNontab,]$filePath, header = F, na.strings =c(".",NA,"NA",""), blank.lines.skip = T)
      if(length(lNontab)<iNontab){
        lNontab[iNontab]<-list(cNontab)
      } else {
        lNontab[iNontab]<-list(rbind(lNontab[iNontab][[1]],cNontab))
      }
    }

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

  dfIDP$ID<-covidcnsParseIDColumn(IDs = dfIDP$ID)
  dfFSIDP$ID<-covidcnsParseIDColumn(IDs = dfFSIDP$ID)
  if(any(nchar(dfIDP$ID)!=8)) {
    warning("There are IDs with length != 8 (IDP)")
    print(dfIDP[nchar(dfIDP$ID)!=8,]$ID)
  }
  if(any(nchar(dfFSIDP$ID)!=8)) {
    warning("There are IDs with length != 8 (FSIDP)")
    print(dfFSIDP[nchar(dfFSIDP$ID)!=8,]$ID)
  }
  names(lNontab)<-nontabMeta$code

  #add individual IDs for nontab data in the same format as dfIDP - just parsed above
  for(iNontab in 1:nrow(cNontabMeta)){
    #iNontab<-1
    cNontab <- lNontab[iNontab][[1]]
    cNontab$ID<-dfIDP$ID
    lNontab[iNontab]<-list(cNontab)
  }

  return(list(dfIDPmeta=dfIDPmeta,dfFSIDPmeta=dfFSIDPmeta,dfIDP=dfIDP,dfFSIDP=dfFSIDP,lNontab=lNontab))

}


covidcnsParseIDColumn=function(IDs){
  #IDs<-c("RGZ_CNS1005","RET_CNS02002","RJZ_CNS01009","RGZ_CNS1085")

  #remove the prefixes
  IDs<-substr(IDs,start = 5, stop = nchar(IDs))

  #add a zero to the 01-series with shorter length - JZ: I expanded this to other series as well.
  #deprecated & substring(IDs, first = 4, last = 4)=="1"
  return(ifelse(nchar(IDs)==7, paste0("CNS0",substring(IDs, first = 4, last = nchar(IDs))),IDs))
}

