#xnat http rest driver

#install.packages('httr')
#install.packages('jsonlite')
#install.packages('xml2')
#install.packages('XML')


library(httr)
library(jsonlite)
library(xml2)
#library(XML)

covidcnsXnatHttpRestDriverClass <- setRefClass("covidcnsXnatHttpRestDriver",
                          fields = list(
                            authorisation = "character",
                            xnatURL = "character",
                            xnatProjectId = "character",
                            statusCode = "numeric",
                            cacheDb = "ANY",
                            savedCachePath = "character"
                          ),
                          methods = list
                          (
                            #this is the constructor as per convention
                            initialize=function(username, password, xnatURL, xnatProjectId, doCache=T)
                            {
                              authorisation <<- paste0("Basic ",base64_enc(paste0(username,":",password)))
                              xnatURL <<- xnatURL
                              xnatProjectId <<- xnatProjectId
                              savedCachePath <<- "./covidcnsXnatHttpRestDriverCache.Rds"
                              
                              if(doCache==T) cache()
                            }
                          )
)

# we can add more methods after creating the ref class (but not more fields!)

covidcnsXnatHttpRestDriverClass$methods(
  get.search.elements=function(){
    url<-paste0(xnatURL,"data/search/elements")
    r<-GET(url = url, add_headers(Authorization = authorisation))
    statusCode <<- status_code(r)
    return(fromJSON(content(r, "text", encoding = "UTF-8")))
  }
  )

covidcnsXnatHttpRestDriverClass$methods(
  get.search.elements.fields=function(xsiType){
    url<-paste0(xnatURL,"data/search/elements/",xsiType)
    r<-GET(url = url, add_headers(Authorization = authorisation))
    statusCode <<- status_code(r)
    
    xml<-read_xml(x=content(r, "text", encoding = "UTF-8"),encoding = "UTF-8",options = c("RECOVER","NOWARNING"), as_html = T)
    resultsNode<-xml_find_first(x = xml, xpath = ".//results")
    columns<-xml_text(xml_find_all(x = resultsNode, xpath = "./columns/column"))
    rowNodes<-xml_find_all(x = resultsNode, xpath = "./rows/row")
    #iRow<-1
    elementFields<-data.frame()
    for(iRow in 1:length(rowNodes)){
      elementFields<-rbind(elementFields,xml_text(xml_find_all(rowNodes[iRow], xpath = "./cell")))
    }
    
    if(ncol(elementFields)==length(columns)) names(elementFields)<-columns
    
    return(elementFields)
  }
)

# cache frequently and rarely changing data from the server
covidcnsXnatHttpRestDriverClass$methods(
  cache=function(refresh=F, write=T, cacheSaveFilePath=NULL){
    if(is.null(cacheSaveFilePath)){
      cacheSaveFilePath <- savedCachePath
    }
    
    if(file.exists(cacheSaveFilePath) && refresh == F){
      cacheDb <<- readRDS(file=cacheSaveFilePath)
    } else {
      cacheDb <<- c()
      searchElements <- get.search.elements()
      cacheDb$searchElement <<- searchElements$ResultSet$Result
      cacheDb$searchElement$field <<- NA
      if("ELEMENT_NAME" %in% names(cacheDb$searchElement)) {
        for (iSearchElement in 1:nrow(cacheDb$searchElement)) {
          cSearchElement<-cacheDb$searchElement[iSearchElement,]
           cacheDb$searchElement[[iSearchElement,c("field")]] <<- list(get.search.elements.fields(xsiType = cSearchElement$ELEMENT_NAME))
        }
      } else {
        warning("ELEMENT_NAME not found cached elements - needed for querying element fields.")
      }
      
      if(write) {
        saveRDS(cacheDb,file = cacheSaveFilePath)
        savedCachePath <<- cacheSaveFilePath
        }
      
    }
  }
)

covidcnsXnatHttpRestDriverClass$methods(
  getCachedSearchElementFields=function(xsiType){
    return(cacheDb$searchElement[[which(cacheDb$searchElement$ELEMENT_NAME==xsiType),c("field")]][[1]])
  }
)

covidcnsXnatHttpRestDriverClass$methods(
  get.project.resources=function(){
    url<-paste0(xnatURL,"data/projects/",xnatProjectId,"/resources")
    r<-GET(url = url, add_headers(Authorization = authorisation))
    statusCode <<- status_code(r)
    return(fromJSON(content(r, "text", encoding = "UTF-8")))
  }
)

#not completed
# covidcnsXnatHttpRestDriverClass$methods(
#   post.search=function(searchXML){
#     url<-paste0(xnatURL,"data/search")
#     r<-POST(url = url, format = "json", add_headers(Authorization = authorisation), body = list(search = searchXML), content_type_xml())
#     statusCode <<- status_code(r)
#     return(1)
#     #return(ifelse(statusCode==400,fromJSON(content(r, "text", encoding = "UTF-8")),NULL))
#   }
# )
