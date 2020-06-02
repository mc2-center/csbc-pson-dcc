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

csv_str_to_json <- function(str, delim = ","){
  json_str <- str_split(str, delim)[[1]] %>% 
    str_trim() %>% 
    jsonlite::toJSON()
  if (json_str == "[null]") {
    NA
  } else {
    json_str
  }
}

summarise_list_cols <- function(df, comma_cols, bar_cols){
  df %>% 
    group_by_at(vars(-c(comma_cols))) %>%
    summarise_at(vars(comma_cols), ~str_c(unique(.x), collapse = ", ")) %>% 
    group_by_at(vars(-c(bar_cols))) %>%
    summarise_at(vars(bar_cols), ~str_c(unique(.x), collapse = " | ")) %>% 
    ungroup() %>%
    distinct()
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

grant_comma_summary_cols <- c(
  "institutionId", 
  "investigator",
  "themeId", 
  "theme", 
  "consortiumId", 
  "consortium",
  "institutionAlias"
)

grant_bar_summary_cols <- c(
  "grantInstitution"
)

grant_comma_list_cols <- c(
  "theme",
  "institutionAlias"
)

grant_bar_list_cols <- c(
  "grantInstitution"
)

merged_formatted_grant_df1 <- merged_grant_df %>% 
  group_by_at(vars(-c(grant_bar_summary_cols, grant_comma_summary_cols))) %>% 
  summarise_at(vars(grant_comma_summary_cols), ~str_c(unique(.x), collapse = ", ")) %>% 
  mutate_at(grant_comma_list_cols, ~ purrr::map_chr(., csv_str_to_json))

merged_formatted_grant_df2 <- merged_grant_df %>% 
  group_by_at(vars(-c(grant_bar_summary_cols, grant_comma_summary_cols))) %>% 
  summarise_at(vars(grant_bar_summary_cols), ~str_c(unique(.x), collapse = " | ")) %>% 
  mutate_at(grant_bar_list_cols, ~ purrr::map_chr(., csv_str_to_json, "\\|"))

merged_grant_syntable <- 
  dplyr::inner_join(merged_formatted_grant_df1, merged_formatted_grant_df2) %>% 
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
  distinct()


proj_summarize_cols <- c(
  "grantId", "grantName", "grantType", "grant", "themeId", "theme", "consortiumId", "consortium"
)

proj_comma_list_cols <- c("grantId", "grantType", "grant", "theme")
proj_bar_list_cols <- c("grantName")

merged_formatted_project_df <- merged_project_df %>%
  filter(!is.na(grantName)) %>%
  group_by_at(vars(-c(proj_summarize_cols))) %>%
  summarize(
    grantId = str_c(unique(grantId), collapse = ", "),
    grant = str_c(unique(grant), collapse = ", "),
    grantName = str_c(unique(grantName), collapse = " | "),
    grantType = str_c(unique(grantType), collapse = ", "),
    themeId = str_c(unique(themeId), collapse = ", "),
    theme = str_c(unique(theme), collapse = ", "),
    consortiumId = str_c(unique(consortiumId), collapse = ", "),
    consortium = str_c(unique(consortium), collapse = ", ")) %>%
  ungroup() %>%
  # rowwise() %>%
  mutate_at(proj_comma_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>%
  mutate_at(proj_bar_list_cols, ~ purrr::map_chr(., csv_str_to_json, "\\|")) %>%
  # ungroup() %>%
  distinct()

# initial table creation (synIDs need to be integers)
# merged_proj_syntable <- merged_formatted_proj_df %>%
#   mutate_at(vars(contains("Id")), ~ str_replace(., "syn", "")) %>%
#   synBuildTable("Portal - projs Merged", "syn7080714", .) %>%
#   synStore()

# update/overwrite the table
merged_proj_syntable <- merged_formatted_project_df %>%
  # mutate(id = projName) %>%
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


dataset_comma_summary_cols <- c(
  "tumorType",
  "assay",
  "species",
  "themeId",
  "theme",
  "consortiumId",
  "consortium",
  "grantId",
  "grantNumber",
  "publicationId",
  "publication",
  "publicationTitle"
)

dataset_bar_summary_cols <- c(
  "grantName"
)

dataset_comma_list_cols <- c(
  "assay",
  "species",
  "tumorType",
  "theme",
  "consortium",
  "grantNumber",
  "grantId",
  "publicationId",
  "publication",
  "publicationTitle"
)

dataset_bar_list_cols <- c(
  "grantName"
)

merged_formatted_dataset_df1 <- merged_dataset_df %>% 
  group_by_at(vars(-c(dataset_bar_summary_cols, dataset_comma_summary_cols))) %>% 
  summarise_at(vars(dataset_comma_summary_cols), ~str_c(unique(.x), collapse = ", ")) %>% 
  mutate_at(dataset_comma_list_cols, ~ purrr::map_chr(., csv_str_to_json))

merged_formatted_dataset_df2 <- merged_dataset_df %>% 
  group_by_at(vars(-c(dataset_bar_summary_cols, dataset_comma_summary_cols))) %>% 
  summarise_at(vars(dataset_bar_summary_cols), ~str_c(unique(.x), collapse = " | ")) %>% 
  mutate_at(dataset_bar_list_cols, ~ purrr::map_chr(., csv_str_to_json, "\\|"))

merged_dataset_syntable <- 
  dplyr::inner_join(merged_formatted_dataset_df1, merged_formatted_dataset_df2) %>% 
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

pub_comma_summary_cols <- c(
  "authors",
  "assay",
  "tumorType",
  "themeId",
  "consortiumId",
  "datasetId",
  "dataset",
  "tissue",
  "theme",
  "consortium",
  "grantId",
  "grantNumber",
  "grantInstitution"
)

pub_bar_summary_cols <- c(
  "grantName"
)

pub_comma_list_cols <- c(
  "assay",
  "tumorType",
  "tissue",
  "theme",
  "consortium",
  "grantId",
  "grantNumber",
  "grantInstitution"
)

pub_bar_list_cols <- c(
  "grantName"
)

merged_formatted_pub_df1 <- merged_pub_df %>% 
  group_by_at(vars(-c(pub_bar_summary_cols, pub_comma_summary_cols))) %>% 
  summarise_at(vars(pub_comma_summary_cols), ~str_c(unique(.x), collapse = ", ")) %>% 
  mutate_at(pub_comma_list_cols, ~ purrr::map_chr(., csv_str_to_json))

merged_formatted_pub_df2 <- merged_pub_df %>% 
  group_by_at(vars(-c(pub_bar_summary_cols, pub_comma_summary_cols))) %>% 
  summarise_at(vars(pub_bar_summary_cols), ~str_c(unique(.x), collapse = " | ")) %>% 
  mutate_at(pub_bar_list_cols, ~ purrr::map_chr(., csv_str_to_json, "\\|"))

merged_pub_syntable <- 
  dplyr::inner_join(merged_formatted_pub_df1, merged_formatted_pub_df2) %>% 
  update_synapse_table(dest_table_list["Portal - Publications Merged"], .)


## Tools - merged table ----

merged_tool_df <- db_tool_df %>%
  select(-one_of(property_cols)) %>%
  rename(toolId = id, toolName = displayName) %>%
  left_join(db_description_tool, by = "toolId") %>%
  left_join(db_datatype_tool, by = "toolId") %>%
  left_join(db_language_tool, by = "toolId") %>%
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
  select(grantId, toolId, toolName, description, dataType, role, softwareLanguage,
         homepageUrl = url, toolType,
         publicationId, publication = pubMedLink,
         theme, grantName, consortium,
         themeId, consortiumId, grantNumber = grant, publicationTitle) %>%
  mutate(publication = ifelse(str_detect(publication, "^\\[NA"), NA, publication)) %>%
  distinct()

tool_summary_cols <- c(
  "dataType",
  "grantId",
  "grantName",
  "grantNumber",
  "publicationId",
  "publication",
  "publicationTitle",
  "softwareLanguage",
  "themeId",
  "theme",
  "consortiumId",
  "consortium"
)

tool_comma_list_cols <- c(
  "softwareLanguage",
  "theme",
  "inputDataType",
  "outputDataType",
  "grantId",
  "grantNumber",
  "publicationId",
  "publication",
  "publicationTitle"
)

tool_bar_list_cols <- c(
  "grantName"
)

merged_formatted_tool_df <- merged_tool_df %>%
  dplyr::mutate(
    dataType = stringr::str_remove_all(dataType, "[:punct:]"),
    softwareLanguage = stringr::str_remove_all(softwareLanguage, "[:punct:]")
  ) %>%
  group_by_at(vars(-c(tool_summary_cols))) %>%
  summarize(
    dataType = str_c(unique(dataType), collapse = ", "),
    grantId = str_c(unique(grantId), collapse = ", "),
    grantName = str_c(unique(grantName), collapse = " | "),
    grantNumber = str_c(unique(grantNumber), collapse = ", "),
    publicationId = str_c(unique(publicationId), collapse = ", "),
    publication = str_c(unique(publication), collapse = ", "),
    publicationTitle = str_c(unique(publicationTitle), collapse = ", "),
    softwareLanguage = str_c(unique(softwareLanguage), collapse = ", "),
    themeId = str_c(unique(themeId), collapse = ", "),
    theme = str_c(unique(theme), collapse = ", "),
    consortiumId = str_c(unique(consortiumId), collapse = ", "),
    consortium = str_c(unique(consortium), collapse = ", ")
  ) %>%
  ungroup() %>%
  # rowwise() %>%
  distinct() %>%
  pivot_wider(names_from = role, values_from = dataType) %>%
  select(-`NA`) %>%
  rename_at(c("input", "output"), ~ str_c(., "DataType", sep = "")) %>%
  mutate_at(tool_comma_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>% 
  mutate_at(tool_bar_list_cols, ~ purrr::map_chr(., csv_str_to_json, "\\|"))

# merged_tool_cols <- list(
#    Column(name = 'toolName', columnType = 'STRING', maximumSize = 30),
#    Column(name = 'inputDataType', columnType = 'STRING_LIST', maximumSize = 30),
#    Column(name = 'outputDataType', columnType = 'STRING_LIST', maximumSize = 30),
#    Column(name = 'softwareLanguage', columnType = 'STRING_LIST', maximumSize = 20),
#    Column(name = 'theme', columnType = 'STRING_LIST', maximumSize = 50),
#    Column(name = 'grantName', columnType = 'STRING', maximumSize = 160),
#    Column(name = 'consortium', columnType = 'STRING', maximumSize = 30),
#    Column(name = 'grantType', columnType = 'STRING', maximumSize = 10)
# )

# merged_tool_schema <- Schema(name = "Portal - Tools Merged",
#                            columns = merged_tool_cols,
#                            parent = "syn7080714")
# merged_tool_schema

# merged_tool_table <- Table(merged_tool_schema, merged_tool_df)
# merged_tool_table <- synStore(merged_tool_table)

merged_tool_syntable <- merged_formatted_tool_df  %>%
  update_synapse_table(dest_table_list["Portal - Tools Merged"], .)



