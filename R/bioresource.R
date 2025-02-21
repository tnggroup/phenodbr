
bioresourcerClass <- setRefClass("bioresourcer",
                                      fields = list(
                                        #General
                                        currentUserTag = "character",
                                        folderpathBioResourceData = "character",
                                        nThreads = "numeric",

                                        #standardised subfolder paths
                                        relSubFolderpathReceiptReports = "character",
                                        #NBC
                                        folderpathBioResourceData.NBC = "character",
                                        folderpathBioResourceData.NBC.cleaned = "character",

                                        folderpathBioResourceData.NBC.raw = "character",
                                        folderpathBioResourceData.NBC.barcodeReports = "character"
                                      ),
                                      methods = list
                                      (
                                        #this is the constructor as per convention
                                        initialize=function(nFolderpathBioResourceData,nCurrentUserTag="anon",nNThreads=3)
                                        {
                                          #General
                                          currentUserTag <<- nCurrentUserTag
                                          folderpathBioResourceData <<- nFolderpathBioResourceData
                                          nThreads <<- nNThreads
                                          #standardised subfolder paths
                                          relSubFolderpathReceiptReports<<-"RR"
                                          #NBC
                                          folderpathBioResourceData.NBC<<-file.path(folderpathBioResourceData,"NBC data")
                                          folderpathBioResourceData.NBC.cleaned<<-file.path(folderpathBioResourceData.NBC,"NBC data (clean)")

                                          folderpathBioResourceData.NBC.raw<<-file.path(folderpathBioResourceData.NBC,"NBC raw reports")


                                          folderpathBioResourceData.NBC.barcodeReports<<-"RR RedCap barcode reports"
                                        }
                                      )
)

# we can add more methods after creating the ref class (but not more fields!)

# create file for upload to redcap
bioresourcerClass$methods(
  kits.NBC_data_RR=function(filename, folderpath.in=NULL, folderpath.out=NULL){
    if(!is.null(folderpath.in)){
      fileToRead<-file.path(folderpath.in,filename)
    } else fileToRead<-file.path(folderpathBioResourceData.NBC.raw,relSubFolderpathReceiptReports,filename)
    #CSV
    fileData <- data.table::fread(file = fileToRead, na.strings =c(".",NA,"NA",""), encoding = "UTF-8",check.names = T, fill = T, blank.lines.skip = T, data.table = T, nThread = nThreads, showProgress = F)

    #standardise filename from file timestamp - NBC_data_RR_DD_MM_YYYY.csv
    filename.split<-strsplit(filename,split = "-",fixed = T)
    filename.split.datepart<-strsplit(filename.split[[1]][length(filename.split[[1]])],split = ".",fixed = T)[[1]][[1]]
    fileDT<-strptime(filename.split.datepart,format='%Y%m%d')
    newFilenamePart<-paste0("NBC_data_RR_",strftime(fileDT,format='%d_%m_%Y'))

    if(!is.null(folderpath.out)){
      fileToWrite<-file.path(folderpath.out,newFilename)
    } else {
      #BUSINESS RULES
      #1. Data - save to folderpathBioResourceData.NBC.barcodeReports/YEAR
      #2. No data - save to folderpathBioResourceData.NBC.cleaned/relSubFolderpathReceiptReports

      if(nrow(fileData)>0){
        fileToWrite<-file.path(folderpathBioResourceData.NBC.barcodeReports,strftime(fileDT,format='%Y'),paste0(newFilename,".csv"))
      } else {
        fileToWrite<-file.path(folderpathBioResourceData.NBC.cleaned,relSubFolderpathReceiptReports,paste0(newFilename,"_empty",".csv"))
      }
    }

    data.table::fwrite(x = fileData,file = fileToWrite, append = F,quote = T,sep = ",",col.names = T,nThread=nThreads)

  }
)
