library(tidyverse)
library(reticulate)

reticulate::use_condaenv(
    condaenv = "py37b",
    required = TRUE,
    conda = "/home/aelamb/anaconda3/condabin/conda"
)

synapseclient <- reticulate::import("synapseclient")
syntab <- reticulate::import("synapseclient.table")
syn <- synapseclient$Synapse()
syn$login()

source_project <- "syn7080714"
destination_project <- "syn21989819"

# functions --------------------------------------------------------------------


update_synapse_table <- function(
    table_id, 
    tbl, 
    syntab = .GlobalEnv$syntab,
    syn    = .GlobalEnv$syn
){
    current_rows <- syn$tableQuery(glue::glue("SELECT * FROM {table_id}"))
    syn$delete(current_rows)
    tmpfile <- fs::file_temp("rows.csv")
    readr::write_csv(tbl, tmpfile, na = "")
    update_rows <- syntab$Table(table_id, tmpfile)
    syn$store(update_rows)
}

update_tbl_with_new_data <- function(
    table_id,
    current_tbl, 
    updated_tbl, 
    id_col = "id"
){
    non_updated_cols <- c(
        id_col, 
        setdiff(colnames(current_tbl), colnames(updated_tbl))
    )
    update_tbl <- dplyr::full_join(
        dplyr::select(current_tbl, non_updated_cols),
        updated_tbl,
        by = "id"
    )
    update_synapse_table(table_id, update_tbl)
}

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

# misc -------------------------------------------------------------------------

entity_columns <- c(
    "createdOn", "createdBy", "currentVersion", "modifiedOn", "modifiedBy"
)

dest_table_list <- destination_project %>%
    create_entity_tbl() %>% 
    dplyr::select("name", "id") %>% 
    tibble::deframe(.)

source_table_list <- source_project %>%
    create_entity_tbl() %>% 
    dplyr::select("name", "id") %>% 
    tibble::deframe(.)

# grants -----------------------------------------------------------------------

merged_grants_tbl <- get_synapse_tbl(
    source_table_list["Portal - Grants Merged"]
)

grants_tbl <- get_synapse_tbl(dest_table_list["grant"]) %>% 
    dplyr::select(-entity_columns)

updated_grants_tbl <- merged_grants_tbl %>%
    dplyr::select(
        "id"          = .data$grantId,
        "consortiumId",
        "name"        = .data$grantName,
        "description" = .data$abstract,
        "grantNumber",
        "grantType"
    )

update_tbl_with_new_data(
    dest_table_list["grant"], grants_tbl, updated_grants_tbl
)

merged_grants_tbl %>%
    dplyr::select("grantId", "institutionId") %>%
    tidyr::separate_rows("institutionId", sep = " \\| ") %>%
    update_synapse_table(dest_table_list["institution_grant"], .)

merged_grants_tbl %>%
    dplyr::select("grantId", "themeId") %>%
    tidyr::separate_rows("themeId", sep = ", ") %>%
    update_synapse_table(dest_table_list["theme_grant"], .)


# projects ---------------------------------------------------------------------

#TODO: adapt for file view
merged_project_tbl <- get_synapse_tbl(
    source_table_list["Portal - Projects Merged"], row_data = F
)

project_tbl <- get_synapse_tbl(dest_table_list["project"], row_data = F)

new_project_tbl <- merged_project_tbl %>%
    dplyr::filter(!.data$projectId %in% project_tbl$id)

updated_project_tbl <- merged_project_tbl %>%
    dplyr::select(
        "id" = .data$projectId,
        "grantId",
        "displayName" = .data$projectName,
        "projectType"
    )

update_tbl_with_new_data(
    dest_table_list["project"], project_tbl, updated_project_tbl
)

merged_project_tbl %>%
    dplyr::select("projectId", "description") %>%
    tidyr::drop_na() %>%
    update_synapse_table(dest_table_list["description_project"], .)


# # datasets ---------------------------------------------------------------------

#TODO: come up with plan for adding new datasets, and updating existing ones.
merged_dataset_tbl <- get_synapse_tbl(
    source_table_list["Portal - Datasets Merged"]
)

dataset_tbl <- get_synapse_tbl(dest_table_list["dataset"]) %>% 
    dplyr::select(-entity_columns)

updated_dataset_tbl <- merged_dataset_tbl %>%
    dplyr::select(
        "id" = .data$datasetId,
        "displayName" = .data$datasetAlias,
        "fullName" = .data$datasetName,
        "grantId",
        "overallDesign"
    ) %>% 
    dplyr::mutate("fullName" = dplyr::if_else(
        .data$fullName == .data$displayName,
        NA_character_,
        .data$fullName
    )) 

update_tbl_with_new_data(
    dest_table_list["dataset"], dataset_tbl, updated_dataset_tbl
)

merged_dataset_tbl %>%
    dplyr::select("datasetId","description") %>%
    tidyr::drop_na() %>% 
    update_synapse_table(dest_table_list["description_dataset"], .)
    
# publications -----------------------------------------------------------------

#TODO: deal with file view
#TODO: deal with person_publication table
merged_publication_tbl <- get_synapse_tbl(
    source_table_list["Portal - Publications Merged"]
)

publication_tbl <- get_synapse_tbl(dest_table_list["publication"]) %>% 
    dplyr::select(-entity_columns)

updated_publication_tbl <- merged_publication_tbl %>%
    dplyr::select(
        "id" = .data$publicationId,
        "grantId",
        "title" = .data$publicationTitle,
        "journal",
        "publicationYear",
        "doi",
        "pubMedUrl",
        "keywords"
    )

update_tbl_with_new_data(
    dest_table_list["publication"], publication_tbl, updated_publication_tbl
)

# tools ------------------------------------------------------------------------

#TODO: Deal with file view
merged_tool_tbl <- get_synapse_tbl(
    source_table_list["Portal - Tools Merged"]
)

tool_tbl <- get_synapse_tbl(dest_table_list["tool"]) %>% 
    dplyr::select(-entity_columns)


updated_tool_tbl <- merged_tool_tbl %>%
    dplyr::select(
        "id" = .data$toolId,
        "grantId",
        "displayName" = .data$toolName,
        "toolType"
    )

update_tbl_with_new_data(
    dest_table_list["tool"], tool_tbl, updated_tool_tbl
)

merged_tool_tbl %>%
    dplyr::select(
        "toolId",
        "input" = .data$inputDataType,
        "output" = .data$outputDataType
    ) %>%
    tidyr::pivot_longer(., -"toolId", values_to = "dataType", names_to = "role") %>%
    tidyr::separate_rows(.data$dataType, sep = ", ") %>%
    dplyr::mutate(
        dataType = stringr::str_remove_all(.data$dataType, "[\\[\\]\\\\]")
    ) %>%
    tidyr::drop_na() %>%
    dplyr::filter(!.data$dataType == "") %>%
    update_synapse_table(dest_table_list["datatype_tool"], .)

merged_tool_tbl %>%
    dplyr::select("toolId", "softwareLanguage") %>%
    tidyr::separate_rows(.data$softwareLanguage, sep = ", ") %>%
    dplyr::mutate(
        softwareLanguage = stringr::str_remove_all(.data$softwareLanguage, "[\\[\\]\\\\]")
    ) %>%
    tidyr::drop_na() %>%
    dplyr::filter(!.data$softwareLanguage == "") %>%
    update_synapse_table(dest_table_list["language_tool"], .)

merged_tool_tbl %>%
    dplyr::select("toolId", url = .data$homepageUrl) %>%
    tidyr::drop_na() %>%
    dplyr::mutate("source" = dplyr::case_when(
        stringr::str_detect(url, "github") ~ "GitHub",
        stringr::str_detect(url, "bitbucket") ~ "Bitbucket",
        stringr::str_detect(url, "readthedocs") ~ "ReadTheDocs",
        stringr::str_detect(url, "bioconductor") ~ "Bioconductor",
    )) %>%
    mutate("source" = dplyr::if_else(
        is.na(.data$source) | .data$source == "",
        "Website",
        .data$source)
    ) %>%
    update_synapse_table(dest_table_list["link_tool"], .)

