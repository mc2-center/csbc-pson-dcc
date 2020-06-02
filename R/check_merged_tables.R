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

prod_project <- "syn7080714"
test_project <- "syn21989819"

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

find_table_differences <- function(prod_tbl, test_tbl, id_col){
  list(
    "missing_cols" = find_missing_cols(prod_tbl, test_tbl),
    "missing_rows" = find_missing_rows(prod_tbl, test_tbl, id_col),
    "value_diffs" = find_value_differences(prod_tbl, test_tbl, id_col)
  ) %>%
    purrr::keep(., purrr::map(., length) > 0)
}

find_missing_cols <- function(tbl1, tbl2){
  list(
    "missing_cols1" = setdiff(colnames(tbl2), colnames(tbl1)),
    "missing_cols2" = setdiff(colnames(tbl1), colnames(tbl2))
  ) %>%
    purrr::keep(., purrr::map(., length) > 0)
}

find_missing_rows <- function(tbl1, tbl2, id_col){
  list(
    "missing_rows1" = setdiff(tbl2[[id_col]], tbl1[[id_col]]),
    "missing_rows2" = setdiff(tbl1[[id_col]], tbl2[[id_col]])
  ) %>%
    purrr::keep(., purrr::map(., length) > 0)

}

find_value_differences <- function(tbl1, tbl2, id_col){
  cols <- setdiff(colnames(tbl1), id_col)
  purrr::imap(
    set_names(cols, cols),
    ~find_table_diff_by_col(tbl1, tbl2, id_col, .x)
  ) %>%
    purrr::keep(., purrr::map(., nrow) > 0)
}

find_diff <- function(val1, val2){
  if (is.na(val1) & is.na(val2)) return(F)
  if (val1 == "[]" & is.na(val2)) return(F)
  if (val2 == "[]" & is.na(val1)) return(F)
  if (is.na(val1) | is.na(val2)) return(T)
  return(val1 != val2)
}

find_table_diff_by_col <- function(tbl1, tbl2, id_col, test_col){
  dplyr::inner_join(
    dplyr::select(tbl1, id_col, "prod" = test_col),
    dplyr::select(tbl2, id_col, "test" = test_col),
    by = id_col
  ) %>%
    dplyr::mutate(
      "different" = purrr::map2_lgl(.data$prod, .data$test, find_diff)
    ) %>% 
    dplyr::filter(.data$different) %>% 
    dplyr::select(-"different")
}



# misc -------------------------------------------------------------------------

prod_table_list <- prod_project %>%
  create_entity_tbl() %>%
  dplyr::select("name", "id") %>%
  tibble::deframe(.)

test_table_list <- test_project %>%
  create_entity_tbl() %>%
  dplyr::select("name", "id") %>%
  tibble::deframe(.)


# grants -----------------------------------------------------------------------
grant_prod_tbl <- get_synapse_tbl(prod_table_list[["Portal - Grants Merged"]])
grant_test_tbl <- get_synapse_tbl(test_table_list[["Portal - Grants Merged"]])

grant_differences <- find_table_differences(
  grant_prod_tbl,
  grant_test_tbl,
  "grantId"
)

dataset_prod_tbl <- get_synapse_tbl(prod_table_list[["Portal - Datasets Merged"]])
dataset_test_tbl <- get_synapse_tbl(test_table_list[["Portal - Datasets Merged"]])

dataset_differences <- find_table_differences(
  dataset_prod_tbl,
  dataset_test_tbl,
  "datasetId"
)

publication_prod_tbl <- get_synapse_tbl(prod_table_list[["Portal - Publications Merged"]])
publication_test_tbl <- get_synapse_tbl(test_table_list[["Portal - Publications Merged"]])

publication_differences <- find_table_differences(
  publication_prod_tbl,
  publication_test_tbl,
  "publicationId"
)


project_prod_tbl <- get_synapse_tbl(prod_table_list[["Portal - Projects Merged"]])
project_test_tbl <- get_synapse_tbl(test_table_list[["Portal - Projects Merged"]])

project_differences <- find_table_differences(
  project_prod_tbl,
  project_test_tbl,
  "projectId"
)

tool_prod_tbl <- get_synapse_tbl(prod_table_list[["Portal - Tools Merged"]])
tool_test_tbl <- get_synapse_tbl(test_table_list[["Portal - Tools Merged"]])

tool_differences <- find_table_differences(
  tool_prod_tbl,
  tool_test_tbl,
  "toolId"
)









