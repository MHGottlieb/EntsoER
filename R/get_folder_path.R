#' Get Folder Path for CSV Files
#'
#' This function checks if the folder path containing the CSV files is already set.
#' If the path is not set, it prompts the user to input the path, validates it, 
#' and then saves it for future use.
#'
#' @return A character string representing the folder path.
#' @export
get_folder_path <- function() {
  # Check if the folder path is already stored in options
  folder_path <- getOption("EntsoER.folder_path")
  
  if (is.null(folder_path)) {
    # If not set, prompt the user to input the folder path
    folder_path <- readline(prompt = "Enter local root folder where ENTSO-E csv files are stored: ")
    
    # Validate the folder path
    if (!dir.exists(folder_path)) {
      stop("The provided folder path does not exist.")
    }
    
    # Save the folder path using options
    options(EntsoER.folder_path = folder_path)
    
    # Save the option to the .Rprofile file for future sessions
    rprofile_path <- file.path(Sys.getenv("HOME"), ".Rprofile")
    cat(sprintf('options(EntsoER.folder_path = "%s")\n', folder_path),
        file = rprofile_path, append = TRUE)
  }
  
  return(folder_path)
}