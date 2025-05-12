#Relational database import from cleaned Qualtrics export, 2025-05-12
#Johan Zvrskovec

#libraries
library(RPostgres)
library(DBI)
library(optparse)
library(tidyverse)
library(phenodbr)
library(data.table)

folderpathIDP<-file.path("/Users/jakz/Documents/local_db/COVIDCNS/data/IDP_ALL_202505")
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
#dbutil$setSearchPath.standard()

#IDPs

#read and parse IDPs - also pads wrongly assigned numeric part of the ID with additional preceding 0s
#DEPRECATED - if the first digit is 1 only.
idpData<-covidcnsReadIDP(folderpathIDP = folderpathIDP, folderpathIDPMeta = folderpathIDPMeta)



#Nontab data
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

#View(table(idpData$dfIDP$ID))

allNontab<-NULL
for(iNontab in 1:length(idpData$lNontab)){
  #iNontab<-2
  nameCNontab<-names(idpData$lNontab)[iNontab]
  cNontab<-as.data.frame(idpData$lNontab[iNontab][[1]])
  colnames(cNontab)<-ifelse(colnames(cNontab)!="ID",paste0(nameCNontab,".",colnames(cNontab)),colnames(cNontab))
  setDT(cNontab)
  if(is.null(allNontab)) {
    allNontab<-cNontab
    #setDT(allNontab)
    #setkeyv(allNontab, cols = c("ID"))
  } else {
    #setDT(cNontab)
    #setkeyv(cNontab, cols = c("ID"))
    #allNontab<-merge(x = allNontab, y = cNontab, by = "ID", all = T)
    allNontab<-cbind(allNontab,cNontab[,!c("ID")])
  }
}

#colnames(allNontab)

dbutil$readImportData(dataframe = allNontab)
dbutil$formatImportColumnNames(deitemise = T,prefixesToItemiseRegex = paste0(nontabMeta$code,"\\.")) #not sure why this does not work
# dbutil$columnFormat <- formatStdColumnNames(columnNames = colnames(importDataDf),prefixesToExcludeRegex = prefixesToExcludeRegex,deitemise = deitemise, forceItem = forceItem, maxVariableNameLength = maxVariableNameLength)
# colnames(importDataDf) <<- columnFormat$names.new

#ANY ADDITIONAL EDITS, for example if there are additional numbers appended to the ID's
#View(dbutil$importDataDf)
id2<-strsplit(x = dbutil$importDataDf$id, split = "_",fixed = T)
dbutil$importDataDf$id<-unlist(lapply(X = id2,FUN = function(x){
  x[1]
}))

dbutil$fixIdColumn(cohortIdColumn = "id")

#annotation tables
dbutil$createVariableAnnotation(parseItems = T)

#import all tables
dbutil$importDataAsTables(temporary = T)
#perform database preparation procedures for importq
dbutil$prepareImport(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'idpukbbnontab', assessmentVersionCode = '2022', cohortIdColumn = 'id', itemAnnotationTableName = NA) #set itemAnnotationTableName to NA/database NULL as we do not have an itemAnnotation
#perform database import
dbutil$importData(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'idpukbbnontab', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)


#connect again
dbutil <- pgDatabaseUtilityClass(host=cinfo$host, dbname=cinfo$dbname, user=cinfo$user, port=cinfo$port, password= cinfo$pw)

#Standard IDPs
dbutil$readImportData(dataframe = idpData$dfIDP)


#does not work for some reason
# dbutil$formatImportColumnNames(deitemise = T,
#                                prefixesToItemiseRegex = c(
#                                  "QC_",
#                                  "IDP_T1_SIENAX_",
#                                  "IDP_T1_FIRST_",
#                                  "IDP_T1_FAST_ROIs_",
#                                  "IDP_T2_FLAIR_BIANCA_",
#                                  "IDP_SWI_T2star_",
#                                  "IDP_tfMRI_",
#                                  "IDP_dMRI_TBSS_FA_",
#                                  "IDP_dMRI_TBSS_MD_",
#                                  "IDP_dMRI_TBSS_MO_",
#                                  "IDP_dMRI_TBSS_L1_",
#                                  "IDP_dMRI_TBSS_L2_",
#                                  "IDP_dMRI_TBSS_L3_",
#                                  "IDP_dMRI_TBSS_ICVF_",
#                                  "IDP_dMRI_TBSS_OD_",
#                                  "IDP_dMRI_TBSS_ISOVF_",
#                                  "IDP_dMRI_ProbtrackX_FA_",
#                                  "IDP_dMRI_ProbtrackX_MD_",
#                                  "IDP_dMRI_ProbtrackX_MO_",
#                                  "IDP_dMRI_ProbtrackX_L1_",
#                                  "IDP_dMRI_ProbtrackX_L2_",
#                                  "IDP_dMRI_ProbtrackX_L3_",
#                                  "IDP_dMRI_ProbtrackX_ICVF_",
#                                  "IDP_dMRI_ProbtrackX_OD_",
#                                  "IDP_dMRI_ProbtrackX_ISOVF_"
#                                )
#                                )


nformat <- formatStdColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToItemiseRegex = c(
  "QC_",
  "IDP_T1_SIENAX_",
  "IDP_T1_FIRST_",
  "IDP_T1_FAST_ROIs_",
  "IDP_T2_FLAIR_BIANCA_",
  "IDP_SWI_T2star_",
  "IDP_tfMRI_",
  "IDP_dMRI_TBSS_FA_",
  "IDP_dMRI_TBSS_MD_",
  "IDP_dMRI_TBSS_MO_",
  "IDP_dMRI_TBSS_L1_",
  "IDP_dMRI_TBSS_L2_",
  "IDP_dMRI_TBSS_L3_",
  "IDP_dMRI_TBSS_ICVF_",
  "IDP_dMRI_TBSS_OD_",
  "IDP_dMRI_TBSS_ISOVF_",
  "IDP_dMRI_ProbtrackX_FA_",
  "IDP_dMRI_ProbtrackX_MD_",
  "IDP_dMRI_ProbtrackX_MO_",
  "IDP_dMRI_ProbtrackX_L1_",
  "IDP_dMRI_ProbtrackX_L2_",
  "IDP_dMRI_ProbtrackX_L3_",
  "IDP_dMRI_ProbtrackX_ICVF_",
  "IDP_dMRI_ProbtrackX_OD_",
  "IDP_dMRI_ProbtrackX_ISOVF_"
), deitemise = T)
dbutil$columnFormat<-nformat
colnames(dbutil$importDataDf)<-dbutil$columnFormat$names.new

#ANY ADDITIONAL EDITS, for example if there are additional numbers appended to the ID's
#View(dbutil$importDataDf)
id2<-strsplit(x = dbutil$importDataDf$id, split = "_",fixed = T)
dbutil$importDataDf$id<-unlist(lapply(X = id2,FUN = function(x){
  x[1]
}))

#fix ID
dbutil$fixIdColumn(cohortIdColumn = "id")

#dbutil$parseVariableValueLabels() #not needed

#select actual columns to import
#filterColumnsOnFormatting() #not needed

#annotation tables
dbutil$createVariableAnnotation(parseItems = T)

nVarAnnotation <- merge(dbutil$variableAnnotationDf,idpData$dfIDPmeta,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
setDT(nVarAnnotation)
nVarAnnotation$variable_documentation<-nVarAnnotation$description
nVarAnnotation$variable_unit<-nVarAnnotation$unit
nVarAnnotation$variable_data_type<-trimws(nVarAnnotation$datatype)
nVarAnnotation[variable_data_type=="int",variable_data_type:="integer"]
nVarAnnotation[variable_data_type=="float",variable_data_type:="double precision"]
dbutil$variableAnnotationDf<-unique(as.data.frame(nVarAnnotation[,c("column_name","variable_code","variable_original_descriptor","variable_label","variable_index","item_code","variable_documentation","variable_unit","variable_data_type")]))

dbutil$castVariablesAsAnnotated()

dbutil$itemAnnotationDf <- data.frame(
  item_code=unique(dbutil$variableAnnotationDf[order(dbutil$variableAnnotationDf$variable_index),]$item_code)
)
#merge(dbutil$variableAnnotationDf,idpData$dfIDP,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
#dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[order(dbutil$itemAnnotationDf$order),]
dbutil$itemAnnotationDf$item_text<-""
dbutil$itemAnnotationDf$assessment_type<-"imaging"
dbutil$itemAnnotationDf$assessment_item_type_code<-"idp"
#dbutil$itemAnnotationDf$item_index<-dbutil$itemAnnotationDf$order
dbutil$itemAnnotationDf$item_documentation<-""
dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[,c("item_code","item_text","assessment_type","assessment_item_type_code","item_documentation")]
dbutil$itemAnnotationDf <- unique(dbutil$itemAnnotationDf)
dbutil$itemAnnotationDf$item_index <- 1:nrow(dbutil$itemAnnotationDf)

#check single variable items
grep(pattern = "_",x = dbutil$variableAnnotationDf$column_name, value = T, fixed = T, invert = T)

#import all tables
dbutil$importDataAsTables(temporary = T)

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'idpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'idpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)


#FS IDPs
dbutil$readImportData(dataframe = idpData$dfFSIDP)

nformat <- formatStdColumnNames(columnNames = colnames(dbutil$importDataDf),prefixesToItemiseRegex = c(
  "aseg_global_intensity_",
  "aseg_global_volume_",
  "aseg_global_volume-ratio_",
  "aseg_lh_intensity_",
  "aseg_lh_number_",
  "aseg_lh_volume_",
  "aseg_rh_intensity_",
  "aseg_rh_number_",
  "aseg_rh_volume",
  "AmygNuclei_lh_",
  "AmygNuclei_rh_",
  "HippSubfield_lh_",
  "HippSubfield_rh_",
  "ThalamNuclei_lh",
  "ThalamNuclei_rh",
  "Brainstem_global_volume_",
  "aparc-Desikan_lh_area_",
  "aparc-Desikan_lh_thickness_",
  "aparc-Desikan_lh_volume_",
  "aparc-Desikan_rh_area_",
  "aparc-Desikan_rh_thickness_",
  "aparc-Desikan_rh_volume_",
  "aparc-pial_lh_area_",
  "aparc-pial_rh_area_",
  "wg_lh_intensity-contrast_",
  "wg_rh_intensity-contrast_",
  "BA-exvivo_lh_area_",
  "BA-exvivo_lh_thickness_",
  "BA-exvivo_lh_volume_",
  "BA-exvivo_rh_area_",
  "BA-exvivo_rh_thickness_",
  "BA-exvivo_rh_volume_",
  "aparc-DKTatlas_lh_area_",
  "aparc-DKTatlas_lh_thickness_",
  "aparc-DKTatlas_lh_volume_",
  "aparc-DKTatlas_rh_area_",
  "aparc-DKTatlas_rh_thickness_",
  "aparc-DKTatlas_rh_volume_",
  "aparc-a2009s_lh_area_",
  "aparc-a2009s_lh_thickness_",
  "aparc-a2009s_lh_volume_",
  "aparc-a2009s_rh_area_",
  "aparc-a2009s_rh_thickness_",
  "aparc-a2009s_rh_volume_"
), deitemise = T)
dbutil$columnFormat<-nformat
colnames(dbutil$importDataDf)<-dbutil$columnFormat$names.new

#ANY ADDITIONAL EDITS, for example if there are additional numbers appended to the ID's
#View(dbutil$importDataDf)
id2<-strsplit(x = dbutil$importDataDf$id, split = "_",fixed = T)
dbutil$importDataDf$id<-unlist(lapply(X = id2,FUN = function(x){
  x[1]
}))

#fix ID
dbutil$fixIdColumn(cohortIdColumn = "id")

#annotation tables
dbutil$createVariableAnnotation(parseItems = T)

nVarAnnotation <- merge(dbutil$variableAnnotationDf,idpData$dfIDPmeta,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
setDT(nVarAnnotation)
nVarAnnotation$variable_documentation<-nVarAnnotation$description
nVarAnnotation$variable_unit<-nVarAnnotation$unit
nVarAnnotation$variable_data_type<-trimws(nVarAnnotation$datatype)
nVarAnnotation[variable_data_type=="int",variable_data_type:="integer"]
nVarAnnotation[variable_data_type=="float",variable_data_type:="double precision"]
dbutil$variableAnnotationDf<-unique(as.data.frame(nVarAnnotation[,c("column_name","variable_code","variable_original_descriptor","variable_label","variable_index","item_code","variable_documentation","variable_unit","variable_data_type")]))

dbutil$castVariablesAsAnnotated()

dbutil$itemAnnotationDf <- data.frame(
  item_code=unique(dbutil$variableAnnotationDf[order(dbutil$variableAnnotationDf$variable_index),]$item_code)
)
#merge(dbutil$variableAnnotationDf,idpData$dfIDP,by.x="variable_original_descriptor", by.y="header", all.x = T, all.y = F)
#dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[order(dbutil$itemAnnotationDf$order),]
dbutil$itemAnnotationDf$item_text<-""
dbutil$itemAnnotationDf$assessment_type<-"imaging"
dbutil$itemAnnotationDf$assessment_item_type_code<-"idp"
#dbutil$itemAnnotationDf$item_index<-dbutil$itemAnnotationDf$order
dbutil$itemAnnotationDf$item_documentation<-""
dbutil$itemAnnotationDf<-dbutil$itemAnnotationDf[,c("item_code","item_text","assessment_type","assessment_item_type_code","item_documentation")]
dbutil$itemAnnotationDf <- unique(dbutil$itemAnnotationDf)
dbutil$itemAnnotationDf$item_index <- 1:nrow(dbutil$itemAnnotationDf)

#check single variable items
grep(pattern = "_",x = dbutil$variableAnnotationDf$column_name, value = T, fixed = T, invert = T)

#test
# name<-"_import_data_df"
# if(dbExistsTable(conn = dbutil$connection, name = name)) dbRemoveTable(conn = dbutil$connection, name = name, temporary= F)
# excludeColIndexFrom <- 2
# excludeColIndexTo <- 500
#
# if(!is.null(excludeColIndexFrom)){
#   if(is.null(excludeColIndexTo)) excludeColIndexTo=ncol(importDataDf)
#   colIndices <- 1:(excludeColIndexFrom-1)
#   if(excludeColIndexTo < ncol(dbutil$importDataDf)) colIndices <- c(colIndices,excludeColIndexTo:ncol(dbutil$importDataDf))
#   tempImport<-dbutil$importDataDf[,colIndices]
#   dbCreateTable(conn = dbutil$connection, name = name, fields = tempImport, temporary = F)
#   dbAppendTable(conn = dbutil$connection, name = name, value = tempImport)
# } else {
#   dbCreateTable(conn = dbutil$connection, name = name, fields = importDataDf, temporary = F)
#   dbAppendTable(conn = dbutil$connection, name = name, value = importDataDf)
# }


#import all tables  -should be 1275 columns
dbutil$importDataAsTables(temporary = T,excludeColIndexFrom = 501) #Import too big for one table import

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)

#import all tables again for the rest of the columns - This seems to work now!
dbutil$importDataAsTables(temporary = T,excludeColIndexFrom = 2, excludeColIndexTo = 500) #Import too big for one table import

#perform database preparation procedures for import
dbutil$prepareImport(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', cohortIdColumn = 'id')

#perform database import
dbutil$importData(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = 'fsidpukbb', assessmentVersionCode = '2022', stageCode = 'bl', doAnnotate = T, addIndividuals = T, doInsert = T)



