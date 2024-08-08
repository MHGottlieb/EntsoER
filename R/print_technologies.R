#' Print Available Technologies
#'
#' This function prints a list of technologies that can be used as inputs for the 
#' `technology` argument of the `fetch` function. It provides a reference for users to know
#' which technologies are available, and it also notes that the `technology` argument 
#' can be left blank to include all technologies.
#' @import dplyr
#' @importFrom knitr kable
#' @return prints a table of available technologies to the console.
#' @details Outputs a list of all possible technologies. 
#' These technologies can be used as valid inputs for the `technology` argument in the `fetch` function.
#' @examples
#' \dontrun{
#' print_technologies()
#' }
#' @export

print_technologies <- function(){
  cat("These are the technologies that can go to the 'technology' argument of the 'fetch' function\nnote: leave blank for all technologies\n")
  
  # List of technologies
  types_all <- c("Biomass", "Fossil Brown coal/Lignite", "Fossil Coal-derived gas", 
                 "Fossil Gas", "Fossil Hard coal", "Fossil Oil", "Fossil Oil shale", 
                 "Fossil Peat", "Geothermal", "Hydro Pumped Storage", "Hydro Run-of-river and poundage", 
                 "Hydro Water Reservoir", "Marine", "Nuclear", "Other", "Other renewable", 
                 "Solar", "Waste", "Wind Offshore", "Wind Onshore") %>% toupper()
  
  # Print the technologies in a formatted table
  knitr::kable(types_all, col.names = "Input to technology argument:") %>% cat(sep = "\n")
}
