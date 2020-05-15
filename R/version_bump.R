library(reticulate)
library(tidyverse)
args <- commandArgs(trailingOnly = TRUE)
username <- args[1]
password <- args[2]

# # local conda env with Python 3.7 and synapseclient installed
# use_condaenv("synapse", required = TRUE)

synapseclient <- reticulate::import("synapseclient")
syn <- synapseclient$Synapse()
syn$login(email=username, password=password)


bump_table_version <- function(table_id) {
  timestamp_comment <- lubridate::now(tzone = "UCT") %>% 
    stringr::str_replace(":[0-9]{2}$", "") %>% 
    stringr::str_replace(" ", "T")

  snapshot_data <- list(
    snapshotComment = timestamp_comment
  )
  syn$restPOST(
    glue::glue("/entity/{table_id}/table/snapshot"), 
    body = jsonlite::toJSON(snapshot_data, auto_unbox = TRUE)
  )
}

table_ids <- list(
  # tools = "syn21930566",
  # grants = "syn21918972",
  # datasets = "syn21897968",
  # projects = "syn21868602",
  # publications = "syn21868591"
  test = "syn21093721"
)

purrr::walk(table_ids, bump_table_version)
