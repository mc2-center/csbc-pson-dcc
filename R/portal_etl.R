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

source_project <- "syn21989819"
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

collapse_column_to_string <- function(col, collapse = ", "){
  str_c(unique(col), collapse = collapse)
}

collapse_column_to_json <- function(col){
  json_str <- col %>% 
    unique() %>% 
    jsonlite::toJSON()
  if (json_str == "[null]") {
    NA
  } else {
    json_str
  }
}

collapse_df_columns <- function(df, list_cols, pseudolist_cols){
  group_cols <- setdiff(colnames(df), c(pseudolist_cols, list_cols))

  df %>% 
    dplyr::group_by_at(dplyr::vars(dplyr::all_of(group_cols))) %>%
    dplyr::summarize(
      dplyr::across(dplyr::all_of(pseudolist_cols), collapse_column_to_string),
      dplyr::across(dplyr::all_of(list_cols), collapse_column_to_json),
      .groups = 'drop'
    ) 
}

# misc -------------------------------------------------------------------------

dest_table_list <- destination_project %>%
  create_entity_tbl() %>%
  dplyr::select("name", "id") %>%
  tibble::deframe(.)

source_table_list <- source_project %>%
  create_entity_tbl() %>%
  dplyr::select("name", "id") %>%
  tibble::deframe(.)

# load tables -----------------------------------------------------------------

db_consortium_df <- get_synapse_tbl(source_table_list["consortium"])
db_theme_df <- get_synapse_tbl(source_table_list["theme"])
db_grant_df <- get_synapse_tbl(source_table_list["grant"])
db_institution_df <- get_synapse_tbl(source_table_list["institution"])
db_theme_grant <- get_synapse_tbl(source_table_list["theme_grant"]) 
db_institution_grant <- get_synapse_tbl(source_table_list["institution_grant"])
db_person_grant <- get_synapse_tbl(source_table_list["person_grant"])
db_tool_df <- get_synapse_tbl(source_table_list["tool"])
db_description_tool <- get_synapse_tbl(source_table_list["description_tool"])
db_datatype_tool <- get_synapse_tbl(source_table_list["datatype_tool"])
db_language_tool <- get_synapse_tbl(source_table_list["language_tool"])
db_link_tool <- get_synapse_tbl(source_table_list["link_tool"])
db_publication_tool <- get_synapse_tbl(source_table_list["publication_tool"])
db_project_df <- get_synapse_tbl(source_table_list["project"])
db_description_project <- get_synapse_tbl(source_table_list["description_project"])
db_description_dataset <- get_synapse_tbl(source_table_list["description_dataset"])
db_link_dataset <- get_synapse_tbl(source_table_list["link_dataset"])
db_dataset_pub <- get_synapse_tbl(source_table_list["dataset_publication"])
db_pub_df <- get_synapse_tbl(source_table_list["publication"])
db_grant_pub <- get_synapse_tbl(source_table_list["grant_publication"])
db_person_pub <- get_synapse_tbl(source_table_list["person_publication"])
db_assay_pub <- get_synapse_tbl(source_table_list["assay_publication"])
db_tissue_pub <- get_synapse_tbl(source_table_list["tissue_publication"])
db_tumortype_pub <- get_synapse_tbl(source_table_list["tumortype_publication"])
db_link_pub <- get_synapse_tbl(source_table_list["link_publication"])
db_dataset_df <- get_synapse_tbl(source_table_list["dataset"])
db_grant_dataset_df <- get_synapse_tbl(source_table_list["grant_dataset"])


property_cols <- c("createdOn", "createdBy",
                   "modifiedOn", "modifiedBy",
                   "currentVersion")


## Grants - merged table ----

merged_grant_df <- db_grant_df %>%
  select(-one_of(property_cols)) %>%
  rename(grantId = id, grantName = name) %>%
  left_join(db_theme_grant, by = "grantId") %>%
  left_join(db_institution_grant, by = "grantId") %>%
  left_join(db_person_grant, by = "grantId") %>%
  left_join(
    db_theme_df %>%
      select(themeId = id, theme = displayName),
    by = "themeId"
  ) %>%
  left_join(
    db_consortium_df %>%
      select(consortiumId = id, consortium = displayName)
  ) %>%
  left_join(
    db_institution_df %>%
      select(
        "institutionId"    = .data$id, 
        "grantInstitution" = .data$fullName,
        "institutionAlias" = .data$displayName 
      )
  ) %>%
  select(
    grantId, grantName, grantNumber, consortiumId, consortium, themeId,
    theme, institutionId, grantType, institutionId, grantInstitution,
    institutionAlias,
    investigator = person,
    abstract = description
  ) %>%
  distinct() %>%
  dplyr::filter(!is.na(.data$grantName)) %>%
  I


merged_formatted_grant_df <- collapse_df_columns(
  merged_grant_df,
  c("theme", "institutionAlias", "grantInstitution"),
  c("institutionId", 
    "investigator",
    "themeId", 
    "consortiumId", 
    "consortium"
  )
)

merged_grant_syntable <- merged_formatted_grant_df %>% 
  update_synapse_table(dest_table_list["Portal - Grants Merged"], .)

## Projects - merged table ----

merged_project_df <- db_project_df %>%
  rename(projectId = id, projectName = name) %>%
  left_join(
    db_description_project, by = "projectId"
  ) %>%
  left_join(
    db_grant_df %>%
      select(grantId = id, grantName = name, grantType, consortiumId, grant = grantNumber),
    by = "grantId"
  ) %>%
  left_join(db_theme_grant, by = "grantId") %>%
  left_join(
    db_theme_df %>%
      select(themeId = id, theme = displayName),
    by = "themeId"
  ) %>%
  left_join(
    db_consortium_df %>%
      select(consortiumId = id, consortium = displayName)
  ) %>%
  select(
    projectId, projectName = displayName, description, grantId, grant,
    themeId, theme, grantName, consortiumId, consortium, grantType, 
    projectType
  ) %>%
  distinct() %>% 
  dplyr::filter(!is.na(.data$grantName))

merged_formatted_project_df <- collapse_df_columns(
  merged_project_df,
  c("grantId", "grantType", "grant", "theme", "grantName"),
  c("themeId", "consortiumId","consortium")
)

merged_proj_syntable <- merged_formatted_project_df %>%
  update_synapse_table(dest_table_list["Portal - Projects Merged"], .)


## Datasets - merged table ----

merged_dataset_df <- db_dataset_df %>%
  dplyr::filter(.data$is.dataset) %>%
  select(-one_of(property_cols)) %>%
  rename(datasetId = id, datasetName = fullName, datasetAlias = displayName) %>%
  mutate(datasetName = ifelse(datasetName == "NA", datasetAlias, datasetName)) %>%
  left_join(db_description_dataset, by = "datasetId") %>%
  left_join(db_link_dataset, by = "datasetId") %>%
  left_join(db_dataset_pub, by = "datasetId") %>%
  left_join(db_grant_dataset_df, by = "datasetId") %>%
  left_join(
    db_grant_df %>%
      select(
        grantId = id,
        grantName = name,
        consortiumId,
        grantNumber
      ),
    by = "grantId"
  ) %>%
  left_join(db_theme_grant, by = "grantId") %>%
  left_join(
    db_theme_df %>%
      select(themeId = id, theme = displayName),
    by = "themeId"
  ) %>%
  left_join(
    db_consortium_df %>%
      select(consortiumId = id, consortium = displayName)
  ) %>%
  left_join(
    db_pub_df %>%
      select(publicationId = id, publicationTitle = title, pubMedUrl)
  ) %>%
  mutate(pubMedId = str_extract(pubMedUrl, "[0-9].*")) %>%
  mutate_all(~ ifelse(str_detect(., "^NA$"), NA, .)) %>%
  mutate(externalLink = glue::glue("[{source}:{datasetAlias}]({url})")) %>%
  mutate(pubMedLink = glue::glue("[{publicationTitle} (PMID:{pubMedId})]({pubMedUrl})")) %>%
  mutate(tumorType = "", assay = "", species = "") %>%
  mutate(overallDesign = ifelse(overallDesign == "NA", NA, overallDesign)) %>%
  select(datasetId, datasetName, datasetAlias,
         description, publicationTitle, overallDesign,
         tumorType, assay, species, externalLink,
         publicationId, publication = pubMedLink,
         grantId, grantName, consortiumId, consortium,
         themeId, theme, grantNumber) %>%
  mutate_all(~ ifelse(. == "[NA (PMID:NA)](NA)", NA, .)) %>%
  distinct() %>%
  filter(!is.na(.data$grantName))


merged_formatted_dataset_df <- collapse_df_columns(
  merged_dataset_df,
  c(  
    "assay",
    "species",
    "tumorType",
    "theme",
    "consortium",
    "grantNumber",
    "grantId",
    "publicationId",
    "publication",
    "publicationTitle",
    "grantName"
  ),
  c("themeId", "consortiumId")
)

merged_dataset_syntable <-  merged_formatted_dataset_df %>% 
  update_synapse_table(dest_table_list["Portal - Datasets Merged"], .)


## Publications - merged table ----

merged_pub_df <- db_pub_df %>%
  select(-one_of(property_cols)) %>%
  rename(publicationId = id, publicationTitle = title) %>%
  left_join(db_grant_pub, by = "publicationId") %>%
  left_join(db_person_pub, by = "publicationId") %>%
  left_join(db_assay_pub, by = "publicationId") %>%
  left_join(db_tissue_pub, by = "publicationId") %>%
  left_join(db_tumortype_pub, by = "publicationId") %>%
  left_join(db_link_pub, by = "publicationId") %>%
  left_join(db_dataset_pub, by = "publicationId") %>%
  left_join(
    db_grant_df %>%
      select(
        "grantId" = "id", 
        "grantName" = "name", 
        "grantType", 
        "consortiumId", 
        "grantNumber"
      ),
    by = "grantId"
  ) %>%
  left_join(db_institution_grant, by = "grantId") %>%
  left_join(
    db_institution_df %>%
      select(
        "institutionId"    = .data$id, 
        "grantInstitution" = .data$fullName
      )
  ) %>%
  left_join(db_theme_grant, by = "grantId") %>%
  left_join(
    db_theme_df %>%
      select(themeId = id, theme = displayName),
    by = "themeId"
  ) %>%
  left_join(
    db_consortium_df %>%
      select(consortiumId = id, consortium = displayName)
  ) %>%
  left_join(
    db_dataset_df %>%
      select(datasetId = id, dataset = displayName)
  ) %>%
  mutate(pubMedId = str_extract(pubMedUrl, "[0-9].*")) %>%
  mutate_all(~ ifelse(str_detect(., "^NA$"), NA, .)) %>%
  mutate(pubMedLink = glue::glue("[PMID:{pubMedId}]({pubMedUrl})")) %>%
  select(
    publicationId, doi, journal, grantNumber, grantInstitution,
    pubMedUrl, publicationTitle, publicationYear, keywords,
    authors = person, assay, tissue, tumorType,
    consortium, grantName, theme, datasetId, dataset,
    themeId, consortiumId, grantId, pubMedId, pubMedLink
  ) %>%
  distinct() %>%
  mutate_all(~ ifelse(str_detect(., "Not Applicable"), NA, .)) %>%
  mutate_all(~ ifelse(str_detect(., "^NA$"), NA, .)) %>% 
  filter(!is.na(grantName))


merged_formatted_pub_df <- collapse_df_columns(
  merged_pub_df,
  c(
    "assay",
    "tumorType",
    "tissue",
    "theme",
    "consortium",
    "grantId",
    "grantNumber",
    "grantInstitution",
    "grantName"
  ),
  c(
    "authors",
    "themeId",
    "consortiumId",
    "datasetId",
    "dataset"
  )
)

merged_pub_syntable <- merged_formatted_pub_df %>% 
  update_synapse_table(dest_table_list["Portal - Publications Merged"], .)


## Tools - merged table ----


merged_tool_df <- db_tool_df %>%
  select(-one_of(property_cols)) %>%
  rename(toolId = id, toolName = displayName) %>%
  left_join(db_description_tool, by = "toolId") %>%
  left_join(db_language_tool, by = "toolId") %>%
  left_join(
    pivot_wider(
      db_datatype_tool,
      names_from = "role", 
      values_from = "dataType", 
      values_fn = collapse_column_to_json
    ),
    by = "toolId"
  ) %>%
  rename_at(c("input", "output"), ~ str_c(., "DataType", sep = "")) %>%
  left_join(db_link_tool, by = "toolId") %>%
  left_join(db_publication_tool, by = "toolId") %>%
  left_join(
    db_grant_df %>%
      select(grantId = id, grantName = name, grantType, consortiumId, grant = grantNumber),
    by = "grantId"
  ) %>%
  left_join(db_theme_grant, by = "grantId") %>%
  left_join(
    db_theme_df %>%
      select(themeId = id, theme = displayName),
    by = "themeId"
  ) %>%
  left_join(
    db_consortium_df %>%
      select(consortiumId = id, consortium = displayName)
  ) %>%
  left_join(
    db_pub_df %>%
      select(publicationId = id, publicationTitle = title, pubMedUrl)
  ) %>%
  mutate_all(~ ifelse(str_detect(., "^NA$"), NA, .)) %>%
  mutate(pubMedLink = glue::glue("[{publicationTitle} (PMID:{pubMedId})]({pubMedUrl})")) %>%
  mutate(grantId = str_remove_all(grantId, "[:punct:]")) %>% 
  select(grantId, toolId, toolName, description, softwareLanguage,
         homepageUrl = url, toolType, inputDataType, outputDataType,
         publicationId, publication = pubMedLink,
         theme, grantName, consortium,
         themeId, consortiumId, grantNumber = grant, publicationTitle) %>%
  mutate(publication = ifelse(str_detect(publication, "^\\[NA"), NA, publication)) %>%
  distinct()

merged_formatted_tool_df <-
  collapse_df_columns(
    merged_tool_df,
    c(
      "softwareLanguage",
      "theme",
      "grantId",
      "grantNumber",
      "publicationId",
      "publication",
      "publicationTitle",
      "grantName"
    ),
    c(
      "inputDataType",
      "outputDataType",
      "themeId",
      "consortiumId",
      "consortium"
    )
  )


merged_tool_syntable <- merged_formatted_tool_df  %>%
  update_synapse_table(dest_table_list["Portal - Tools Merged"], .)



