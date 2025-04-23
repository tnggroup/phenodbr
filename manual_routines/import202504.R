#COVID-CNS additional biomarkers - WIP
df<-shru::readFile(filePath = file.path("/Users/jakz/Downloads","COVIDCNS_Biomarkers.04.12.2024.csv"))
dbutil$readImportData(dataframe=df)
head(dbutil$importDataDf)
colnames(dbutil$importDataDf)
dbutil$defaultAnnotateAndImportProcedure(cohortCode = cohortSetting, instanceCode = cohortInstanceSetting, assessmentCode = "covidcnsbiomarkers", assessmentVersionCode = "blood", stageCode = "bl",  prefixesToExcludeRegex = "dem\\.,covid19\\.,ncrf1_admission\\.", deitemise = F,prepare = T,import = T)

View(dbutil$columnFormat)
View(dbutil$itemAnnotationDf)
View(dbutil$variableAnnotationDf)
View(dbutil$columnFormat)

#dbutil$columnFormat[,]
dbutil$variableAnnotationDf[which(dbutil$variableAnnotationDf$variable_original_descriptor=='n19exact_date_symptoms_starttxt'),]$variable_data_type<-"text"
dbutil$variableAnnotationDf[which(dbutil$variableAnnotationDf$variable_original_descriptor=='n19latest_test_date_positivetxt'),]$variable_data_type<-"text"
dbutil$variableAnnotationDf[which(dbutil$variableAnnotationDf$variable_original_descriptor=='te_of_positive_covid19_testtxt'),]$variable_data_type<-"text"

View(dbutil$selectImportDataMeta())
View(dbutil$selectImportDataAssessmentItemAnnotation())
View(dbutil$selectImportDataAssessmentVariableAnnotation())
