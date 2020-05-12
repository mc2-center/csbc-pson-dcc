library(tidyverse)
library(reticulate)

reticulate::use_condaenv(
    condaenv = "py37b",
    required = TRUE,
    conda = "/home/aelamb/anaconda3/condabin/conda"
)

synapseclient <- reticulate::import("synapseclient")
synapseutils  <- reticulate::import("synapseutils")
syntab        <- reticulate::import("synapseclient.table")
syn <- synapseclient$Synapse()
syn$login()

prod_project <- "syn21498902"
test_project <- "syn21989819"

update_synapse_table <- function(
    id, 
    tbl, 
    syntab = .GlobalEnv$syntab,
    syn    = .GlobalEnv$syn
){
    current_rows <- syn$tableQuery(glue::glue("SELECT * FROM {id}"))
    syn$delete(current_rows)
    tmpfile <- fs::file_temp("rows.csv")
    readr::write_csv(tbl, tmpfile, na = "")
    update_rows <- syntab$Table(id, tmpfile)
    syn$store(update_rows)
}

create_entity_tbl <- function(synid, syn = .GlobalEnv$syn, ...){
    synid %>%
        syn$getChildren(...) %>%
        reticulate::iterate(.) %>%
        purrr::map(dplyr::as_tibble) %>%
        dplyr::bind_rows()
}

get_synapse_tbl <- function(synid, syn = .GlobalEnv$syn, ...){
    "select * from {synid}" %>%
        glue::glue() %>%
        syn$tableQuery(...) %>%
        purrr::pluck("filepath") %>%
        readr::read_csv(.)
}

create_synapse_table <- function(
    project_id,
    tbl,
    name,
    syntab = .GlobalEnv$syntab ,
    syn = .GlobalEnv$syn
){
    syntab$build_table(name, project_id, tbl) %>%
        syn$store()
}

prod_table_tbl <- prod_project %>%
    create_entity_tbl(includeTypes = list("table")) %>%
    dplyr::select(.data$name, .data$id) 



test_table_tbl <- test_project %>%
    create_entity_tbl(includeTypes = list("table", "entityview"))

if (nrow(test_table_tbl) == 0) {
    test_table_tbl <- dplyr::tibble(
        id = character(),
        name = character()
    )
} else {
    test_table_tbl <- test_table_tbl %>%
        dplyr::select(.data$name, .data$id)
}

test_table_tbl %>%
    dplyr::filter(.data$name %in% prod_table_tbl$name) %>%
    dplyr::pull(id) %>% 
    purrr::walk(syn$delete)
 
purrr::walk(
    prod_table_tbl$id,
    ~ synapseutils$copy(
        syn,
        .x,
        destinationId = test_project, 
        updateExisting = T
    )
)

prod_view_tbl <- prod_project %>%
    create_entity_tbl(includeTypes = list("entityview")) %>%
    dplyr::select(.data$name, .data$id) %>% 
    dplyr::mutate(tbl = purrr::map(
        id, get_synapse_tbl, includeRowIdAndRowVersion = FALSE)
    )

prod_view_tbl2 <- prod_view_tbl %>%
    dplyr::select(.data$name, .data$tbl) %>%
    dplyr::left_join(test_table_tbl, by = "name")

prod_view_tbl2 %>% 
    dplyr::filter(is.na(id)) %>% 
    dplyr::mutate(project_id = test_project) %>%
    dplyr::select("project_id", "tbl", "name") %>% 
    purrr::pmap(create_synapse_table)

prod_view_tbl2 %>% 
    dplyr::filter(!is.na(.data$id)) %>% 
    dplyr::select("id", "tbl") %>% 
    purrr::pmap(update_synapse_table)


