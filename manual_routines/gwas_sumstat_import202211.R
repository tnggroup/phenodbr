#Relational database import from old GWAS database export, 2022-11-24
#Johan Zvrskovec

library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)
library(phenodbr)
library(data.table)

#settings and configuration
filepathDBExportRds<-normalizePath("/Users/jakz/Documents/work_rstudio/genetic_correlations/data_gwas_sumstats_repository_2021/database_export_gwas_reference_category_phenotype.Rds",mustWork = T)


cinfo<-c()
cinfo$pw <- rstudioapi::askForPassword(prompt = "Enter database password for specified user.")
cinfo$host<-"localhost"
cinfo$dbname<-"phenodb"
cinfo$user<-"tng2101"
cinfo$port<-65432 #65432 for remote

dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)

dbutil$readImportData(filepath.rds = filepathDBExportRds)
df<-dbutil$importDataDf
#View(df)
setDT(df,key = "code")
df[,legacy_code:=(substr(code,1,4))]
df[,legacy_code_counter:=as.integer(substr(code,5,6))]
df.phenotypes <- unique(df[,legacy_code:=(substr(code,1,4))][,c("phenotype_id","phenotype_type","phenotype","phenotype_id","legacy_code","category_name")] )
#setkeyv(df.phenotypes,cols = c("phenotype_id"))
#View(df.phenotypes)

#create new phenotype codes
n_code_raw<-gsub(pattern = "[^a-z0-9 ]+", replacement = "\\1", x = tolower(df.phenotypes$phenotype))
n_prefix_raw<-gsub(pattern = "[^a-z0-9 ]+", replacement = "\\1", x = tolower(df.phenotypes$legacy_code))
n_code_split<-strsplit(x = n_code_raw,split = " ")
trimmedStopwordsEnglish= gsub(pattern = "[^a-z0-9]+", replacement = "\\1", x = tm::stopwords("english"))

#put back 'very' as it is used and critical for distinguishing between some phenotypes
trimmedStopwordsEnglish<-trimmedStopwordsEnglish[trimmedStopwordsEnglish!='very']

n_code_filtered<-lapply(n_code_split, function(x){
  paste0(x[!x %in% trimmedStopwordsEnglish],collapse = "")
})
df.phenotypes$code<-unlist(n_code_filtered)
# #restrict to max 30 chars
# if(itemCodeEndHead){
#   newItemAnnotationCodes$item_code<-substr(newItemAnnotationCodes$item_code,1,30)
# } else {
#   newItemAnnotationCodes$item_code<-substr(newItemAnnotationCodes$item_code,nchar(newItemAnnotationCodes$item_code) - 30,nchar(newItemAnnotationCodes$item_code))
# }


#add all phenotypes in order according to the original id
phenotypeIdMap<-c()
for(iP in 1:nrow(df.phenotypes)){
  #iP <- 1
  cP <- df.phenotypes[iP,]
  nId<-NULL
  pcId<-NULL
  cat("\n",cP$phenotype)


  #get category id
  q <- dbSendQuery(dbutil$connection,
                   "SELECT pc.id FROM met.phenotype_category pc WHERE pc.code=$1",
                   list(
                     cP$category_name
                   ))
  res<-dbFetch(q)
  pcId<-as.integer(res[[1]])

  q <- dbSendQuery(dbutil$connection,
                   "SELECT p.id FROM met.phenotype p WHERE p.code=$1",
                   list(
                     cP$code
                   ))
  res<-dbFetch(q)
  nId<-as.integer(res[[1]])

  if(length(nId)<1){
    q <- dbSendQuery(dbutil$connection,
                     "INSERT INTO met.phenotype(phenotype_type,code,sort_code,name) VALUES($1,$2,$3,$4) RETURNING id",
                     list(
                       ifelse(cP$phenotype_type=='biomarker','bio',
                              ifelse(cP$phenotype_type=='disorder','dis','trt')
                              ),
                       cP$code,
                       cP$legacy_code,
                       paste0(toupper(substr(cP$phenotype,1,1)),substr(cP$phenotype,2,nchar(cP$phenotype)))
                       ))
    res<-try(dbFetch(q))
    nId<-as.integer(res[[1]])
  }

  if(length(pcId)>0){
    q <- dbSendQuery(dbutil$connection,
                     "INSERT INTO met.phenotype_phenotype_category(phenotype,phenotype_category) VALUES($1,$2) ON CONFLICT DO NOTHING",
                     list(
                       nId,
                       pcId
                     ))
  }

  dbClearResult(q)
  phenotypeIdMap[cP$phenotype_id]<-nId

}

#add all trait GWASs
for(iGWAS in 1:nrow(df)){
  #iGWAS <- 1
  cGWAS <- df[iGWAS,]
  cSumId<-NULL

  q <- dbSendQuery(dbutil$connection,
                   "SELECT s.id FROM met.summary s WHERE s.sort_code=$1 AND s.sort_counter=$2",
                   list(
                     cGWAS$legacy_code,
                     cGWAS$legacy_code_counter
                   ))
  res<-dbFetch(q)
  cGWASId<-as.integer(res[[1]])
  if(length(cGWASId)<1){
    q <- dbSendQuery(dbutil$connection,
                     "INSERT INTO s.summary(sort_code,sort_counter,name,summary_type,sex,is_meta_analysis,phenotype,phenotype_assessment_type,ancestry_population,ancestry_details,reference,documentation) VALUES($1,$2,$3,$4) RETURNING id ON CONFLICT DO NOTHING",
                     list(
                       ifelse(cP$phenotype_type=='biomarker','bio',
                              ifelse(cP$phenotype_type=='disorder','dis','trt')
                       ),
                       cP$code,
                       cP$legacy_code,
                       paste0(toupper(substr(cP$phenotype,1,1)),substr(cP$phenotype,2,nchar(cP$phenotype)))
                     ))
    res<-try(dbFetch(q))
    nId<-as.integer(res[[1]])
  }

}


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
