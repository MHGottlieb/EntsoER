#' Reset the Stored Folder Path
#'
#' This function resets the stored folder path, clearing it from the current R session. 
#' The next time the `fetch()` function is run, the user will be prompted to enter a new folder path.
#'
#' @return NULL
#' @export
reset_folder_path <- function() {
  options(EntsoER.folder_path = NULL)
  cat("Folder path has been reset. You will be prompted to enter a new path next time you run 'fetch'.\n")
}