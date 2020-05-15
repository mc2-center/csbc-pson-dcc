library(tidyverse)
library(reticulate)

reticulate::use_condaenv(
    condaenv = "py37b",
    required = TRUE,
    conda = "/home/aelamb/anaconda3/condabin/conda"
)

synapseclient <- reticulate::import("synapseclient")
synapseutils  <- reticulate::import("synapseutils")
syntab <- reticulate::import("synapseclient.table")
syn <- synapseclient$Synapse()
syn$login()

dataset_file_view_id <- "syn21897968"
destination_dir_id   <- "syn22047105"

# functions --------------------------------------------------------------------

create_entity_tbl <- function(synid, syn = .GlobalEnv$syn, ...){
    synid %>%
        syn$getChildren(...) %>%
        reticulate::iterate(.) %>%
        purrr::map(dplyr::as_tibble) %>%
        dplyr::bind_rows()
}

get_synapse_tbl <- function(synid, syn = .GlobalEnv$syn, ..., row_data = F){
    "select * from {synid}" %>%
        glue::glue() %>%
        syn$tableQuery(includeRowIdAndRowVersion = row_data, ...) %>%
        purrr::pluck("filepath") %>%
        readr::read_csv(.)
}

moved_datasets <- destination_dir_id %>% 
    create_entity_tbl() %>% 
    dplyr::pull("id")

dataset_file_view_id %>% 
    get_synapse_tbl() %>% 
    dplyr::filter(!.data$datasetId %in% moved_datasets) %>% 
    dplyr::pull("datasetId") %>% 
    purrr::walk(
        ~ try(syn$move(
            .x,
            destination_dir_id
        ))
    )


