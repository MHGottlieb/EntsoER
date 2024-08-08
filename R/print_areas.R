#' Print Available Area Codes and Names
#'
#' This function loads the `AreaCodes` dataset and prints a table of unique area codes and their corresponding names.
#' The output is intended to help users identify valid values for the `areas` argument in the `fetch` function.
#'
#' @import knitr
#' @import dplyr
#' @return This function does not return a value but prints a table of area codes and area names to the console.
#' @details The function loads the `AreaCodes` dataset from the `data/AreaCodes.Rda` file and 
#' displays a unique list of area codes and names using the `knitr::kable()` function for formatted output.
#' @examples
#' \dontrun{
#' print_areas()
#' }
#' @export

print_areas <- function(){
  cat("These are the areas that can go to the 'areas' argument of the 'fetch' function:\n")
  knitr::kable(unique(AreaCodes_dat[c(5:6)]),
               col.names = c("Area Code", "Area Name"),
               row.names = FALSE) %>% cat(sep = "\n")
}
