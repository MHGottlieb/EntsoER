#' Fetch data from ENTSO-E csv files, using multiple cores (to speed up process)
#'
#' A universal function to collect and compile data from
#' ENTSO-E's transparency platform, downloaded as csv files
#' via the sftp server. 
#' 
#' For dodumentation, see
#' https://transparency.entsoe.eu/content/static_content/Static%20content/knowledge%20base/SFTP-Transparency_Docs.html
#' https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
#'
#' @param dataset is used to select data domain, e.g. "load", "price", or "generation"
#' @param areas is a list of shorthand area names to subset data
#' @param technology (optional) is used to subset data by generation technology
#' @param units (optional) is used to subset data by generation unit
#' @param aggregate.hourly if TRUE returns values by hour, as sometimes data is more granular
#' @param from date to start time series
#' @param to date to end time series
#' @return a data frame containing the selected data from ENTSO-E
#' @details This function requires having downloaded data as monthly .csv-files via ENTSO-E sftp server, and stored in folder
#' @examples
#' # Examples of how to use the function
#' 
#' \dontrun{
#' load <- fetch_parallel(dataset = "Generation", areas = c("DK1", "DK2"),
#'             technologies = c("Wind Offshore", "Wind Onshore"),
#'             from = "2024-01-01", to = "2024-02-01")
#' }
#' 
#' @references
#' \url{https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html}
#' @author
#' Magnus Hornoe Gottlieb
#' @keywords Energy Power Grid ENTSO-E Europe
#' @import dplyr
#' @import lubridate
#' @import knitr
#' @import foreach
#' @import parallel
#' @import doParallel
#' @importFrom stats aggregate
#' @importFrom utils object.size read.csv
#' @export

fetch_parallel <- function(dataset="LOAD",
                           areas="all",
                           aggregate.hourly = TRUE,
                           from=lubridate::floor_date(Sys.time(), "year"),
                           to=lubridate::floor_date(Sys.time(), "month")-86400,
                           technology = NULL,
                           units = NULL){
  # checking input ----------------------------------------------------------
  
  folder <- get_folder_path()
  
  # C:/Users/MAHOG/OneDrive - Ørsted/R projects/Entso-E/Data
  
  # from-to dates
  from <- as.POSIXct(from, tz="UTC")
  to <- as.POSIXct(to, tz="UTC")
  
  # error and break if dates out of range
  if(from < as.Date("2014-12-01")){
    cat("ERROR: You've asked for data before December 2014 - there isn't any!")
    #break()
  } else if (to > Sys.time()){
    cat("ERROR: You've asked for data into the future - there isn't any!")
    #break()
  }
  
  # technologies
  types_all <- c("Biomass", "Fossil Brown coal/Lignite", "Fossil Coal-derived gas", 
                 "Fossil Gas", "Fossil Hard coal", "Fossil Oil", "Fossil Oil shale", 
                 "Fossil Peat", "Geothermal", "Hydro Pumped Storage", "Hydro Run-of-river and poundage", 
                 "Hydro Water Reservoir", "Marine", "Nuclear", "Other", "Other renewable", 
                 "Solar", "Waste", "Wind Offshore", "Wind Onshore") %>% toupper()
  
  # error and break if some technologies not found
  if(!all(toupper(technology) %in% types_all)){
    unknown <- paste(technology[which(!technology %in% types_all)], collapse="\", \"")
    cat("ERROR: \"", unknown, "\" not known. \nPlease select one or more from:\n\n", sep="")
    knitr::kable(types_all, col.names="Input to technology argument:") %>% cat(sep="\n")
    break()
  }
  
  # dataset
  dataset <- toupper(dataset)
  sets <- data.frame(sets = c("LOAD", "PRICE", "GENERATION", "GENERATION_UNIT"), 
                     file = c("ActualTotalLoad_6.1.A", 
                              "DayAheadPrices_12.1.D",
                              "AggregatedGenerationPerType_16.1.B_C",
                              "ActualGenerationOutputPerGenerationUnit_16.1.A"))
  
  # error and break if dataset not found, or if more than one
  if(length(dataset)>1){
    cat("ERROR: fetch() only supports one dataset at a time. \nPlease select only one from:\n\n", sep="")
    knitr::kable(sets[1], col.names="Input to dataset argument:") %>% cat(sep="\n")
    break()
  } else if(!dataset %in% sets$set){
    unknown <- paste(dataset[which(!dataset %in% types_all)], collapse="\", \"")
    cat("ERROR: Dataset \"", unknown, "\" not known. \nPlease select only one from:\n\n", sep="")
    knitr::kable(sets[1], col.names="Input to dataset argument:") %>% cat(sep="\n")
    break()
  } else {
    set <- sets[which(sets$sets==dataset),2]
  }
  
  #AreaCodes
  AreaCodes <- dplyr::filter(AreaCodes_dat, Dataset==set)
  
  if(all(toupper(areas)=="ALL")){
    areas <- unique(AreaCodes$Code)
  } else if (!all(areas %in% AreaCodes$Code)){
    
    # error and break if areas not found
    unknown <- paste(areas[which(!areas %in% AreaCodes$Code)], collapse="\", \"")
    cat("ERROR: Area(s) \"", unknown, "\" not known. \nPlease select one or more Area Codes from:\n\n", sep="")
    knitr::kable(unique(AreaCodes[c(5:6)]),
                 col.names=c("Area Code", "Equals to"),
                 row.names=FALSE) %>% cat(sep="\n")
    break()
  }
  
  # Define the priority order for AreaTypeCode in case there's both a bidding 
  # zone and/or control area and/or country with that name
  priority_order <- c("BZN", "CTA", "CTY")
  
  # Filter, prioritize, and select one row per Code
  AreaCodes <- AreaCodes %>%
    dplyr::filter(Code %in% areas) %>%
    dplyr::group_by(Code) %>%
    dplyr::arrange(match(AreaTypeCode, priority_order)) %>%
    dplyr::slice(1)
  
  AreaCodes <- droplevels(AreaCodes)
  
  # fecthing data depending on dataset --------------------------------------
  
  # Detect the number of cores available
  num_cores <- parallel::detectCores()
  
  # Create a cluster with a number of cores (leaving one idle for good measures)
  cl <- parallel::makeCluster(max(1, num_cores-1))
  
  # Register the parallel backend with foreach
  doParallel::registerDoParallel(cl)
  
  switch(dataset,
         LOAD={
           
           # selecting the right folder
           folder <- file.path(folder, set)
           
           # selecting which files in folder 
           files <- sort(dir(folder))
           files <- files[c(grep(format(from, "%Y_%m"), files):
                              grep(format(to, "%Y_%m"), files))]
           
           # opening each file and getting the data we're interested in
           out <- NULL
           
           out <- foreach::foreach(i=files, .combine=rbind) %dopar% {
             temp <- read.csv(file.path(folder, i), sep="\t")
             temp <- dplyr::filter(temp, AreaCode %in% AreaCodes$AreaCode)
             temp[c(2, 4:6, 8)] <- NULL  # dripping not needed columns
             temp[1] <- as.POSIXct(temp$DateTime, tz = "UTC")
             temp <- dplyr::filter(temp, DateTime >= from & DateTime <= to)
             temp[2] <- as.factor(temp$AreaCode)
             temp[3] <- as.integer(temp$TotalLoadValue)
             if(aggregate.hourly){
               temp <- aggregate(temp$TotalLoadValue, by = list(
                 DateTime = lubridate::floor_date(temp$DateTime, unit = "hours"),
                 AreaCode = temp$AreaCode),
                 FUN = mean)
               names(temp)[3] <- "TotalLoadMW"
             }
             temp <- merge(temp, AreaCodes[c(1,5:6)])
             temp <- temp[,c(2,1,4,5,3)]
             temp <- droplevels(temp)
             out <- rbind(out, temp)
             return(out)
           } %>% unique()
           
           stopCluster(cl)
           
         }, 
         PRICE={
           
           # selecting the right folder
           folder <- file.path(folder, set)
           
           # selecting which files in folder 
           files <- sort(dir(folder))
           files <- files[c(grep(format(from, "%Y_%m"), files):
                              grep(format(to, "%Y_%m"), files))]
           
           # opening each file and getting the data we're interested in
           out <- NULL
           
           out <- foreach::foreach(i=files, .combine=rbind) %dopar% {
             temp <- read.csv(file.path(folder, i), sep="\t")
             temp <- dplyr::filter(temp, AreaCode %in% AreaCodes$AreaCode)
             temp[1] <- as.POSIXct(temp$DateTime, tz = "UTC")
             temp <- dplyr::filter(temp, DateTime >= from & DateTime <= to)
             temp[3] <- as.factor(temp$AreaCode)
             temp[8] <- as.factor(temp$Currency)
             if(aggregate.hourly){
               temp <- aggregate(temp$Price, by = list(
                 DateTime = lubridate::floor_date(temp$DateTime, unit = "hours"),
                 AreaCode = temp$AreaCode,
                 Currency = temp$Currency),
                 FUN = mean)
             }
             names(temp)[4] <- "Price"
             temp <- merge(temp, AreaCodes[c(1,5:6)])
             temp <- temp[c(2,1,5,6,3,4)]
             temp <- droplevels(temp)
             out <- rbind(out, temp)
             return(out)
           } %>% unique()
           
           stopCluster(cl)
           
         },
         GENERATION={
           
           # selecting the right folder
           folder <- file.path(folder, set)
           
           # selecting which files in folder 
           files <- sort(dir(folder))
           files <- files[c(grep(format(from, "%Y_%m"), files):
                              grep(format(to, "%Y_%m"), files))]
           
           # opening each file and filtering out the data we're not interested in
           out <- NULL
           
           out <- foreach::foreach(i=files, .combine=rbind) %dopar% {
             temp <- read.csv(file.path(folder, i), sep="\t")
             temp <- dplyr::filter(temp, AreaCode %in% AreaCodes$AreaCode)
             if(!is.null(technology)){
               temp <- dplyr::filter(temp, toupper(ProductionType) %in% toupper(technology))
             }
             temp[1] <- as.POSIXct(temp$DateTime, tz = "UTC")
             temp <- dplyr::filter(temp, DateTime >= from & DateTime <= to)
             temp[3] <- as.factor(temp$AreaCode)
             temp$Generation <- ifelse(is.na(temp$ActualConsumption), 
                                       temp$ActualGenerationOutput, 
                                       temp$ActualGenerationOutput-temp$ActualConsumption)
             if(aggregate.hourly){
               temp <- aggregate(temp$Generation, by = list(
                 DateTime = lubridate::floor_date(temp$DateTime, unit = "hours"),
                 AreaCode = temp$AreaCode,
                 ProductionType = temp$ProductionType),
                 FUN = mean)
             }
             names(temp)[4] <- "Generation"
             temp[4] <- as.integer(temp[,4])
             temp <- merge(temp, AreaCodes[c(1,5:6)])
             temp <- temp[,c(2,1,5,6,3,4)]
             temp <- droplevels(temp)
             temp <- unique(temp)
             out <- rbind(out, temp)
             return(out)
           } %>% unique()
           
           stopCluster(cl)
           
         },
         GENERATION_UNIT={
           
           # selecting the right folder
           folder <- file.path(folder, set)
           
           # selecting which files in folder 
           files <- sort(dir(folder))
           files <- files[c(grep(format(from, "%Y_%m"), files):
                              grep(format(to, "%Y_%m"), files))]
           
           # opening each file and filtering out the data we're not interested in
           out <- NULL
           
           out <- foreach::foreach(i=files, .combine=rbind) %dopar% {
             temp <- read.csv(file.path(folder, i), sep="\t")
             temp <- dplyr::filter(temp, AreaCode %in% AreaCodes$AreaCode)
             if(!is.null(technology)){
               temp <- dplyr::filter(temp, toupper(ProductionType) %in% toupper(technology))
             }
             if(aggregate.hourly){
               temp <- dplyr::filter(temp, ResolutionCode=="PT60M")
             }
             temp[1] <- as.POSIXct(temp$DateTime, tz = "UTC")
             temp <- dplyr::filter(temp, DateTime >= from & DateTime <= to)
             temp[3] <- as.factor(temp$AreaCode)
             temp$Generation <- as.integer(ifelse(is.na(temp$ActualConsumption), 
                                                  temp$ActualGenerationOutput, 
                                                  temp$ActualGenerationOutput-temp$ActualConsumption))
             temp[c(2, 4:7, 10, 11, 13)] <- NULL
             temp <- merge(temp, AreaCodes[c(1,5:6)])
             temp <- temp[,c(2,1,7,8,3:6)]
             temp <- droplevels(temp)
             out <- rbind(out, temp)
             return(out)
           } %>% unique()
           
           stopCluster(cl)
         })
  return(out)
}
