library(tidyverse)
library(reticulate)
source("../R/synapse_db.R")

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
      select(institutionId = id, institution = fullName)
  ) %>%
  select(grantId, grantName, grantNumber, consortiumId, consortium, themeId,
         theme, institutionId, institution, investigator = person,
         abstract = description, grantType) %>%
  distinct() %>%
  I

grant_summarize_cols <- c(
  "institutionId", "institution", "investigator",
  "themeId", "theme", "consortiumId", "consortium"
)

merged_formatted_grant_df <- merged_grant_df %>%
  filter(!is.na(grantName)) %>%
  group_by_at(vars(-c(grant_summarize_cols))) %>%
  summarize(
    institutionId = str_c(unique(institutionId), collapse = " | "),
    institution = str_c(unique(institution), collapse = " | "),
    investigator = str_c(unique(investigator), collapse = ", "),
    themeId = str_c(unique(themeId), collapse = ", "),
    theme = str_c(unique(theme), collapse = ", "),
    consortiumId = str_c(unique(consortiumId), collapse = ", "),
    consortium = str_c(unique(consortium), collapse = ", ")) %>%
  ungroup() %>%
  distinct()

grant_list_cols <- c(
  "theme"
)

merged_grant_syntable <- merged_formatted_grant_df %>%
  # mutate(id = grantName) %>%
  mutate_at(grant_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>%
  update_synapse_table(dest_table_list["Portal - Grants Merged"], .)

## Projects - merged table ----



merged_project_df <- db_project_df %>%
  rename(projectId = id, projectName = name) %>%
  left_join(
    db_description_project, by = "projectId"
  ) %>%
  left_join(
    db_grant_df %>%
      select(grantId = id, grantName = name, grantType, consortiumId),
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
  select(projectId, projectName = displayName, description,
         themeId, theme, grantName, consortiumId, consortium, grantType) %>%
  distinct()


proj_summarize_cols <- c(
  "grantName", "grantType", "themeId", "theme", "consortiumId", "consortium"
)
proj_list_cols <- c("theme")

merged_formatted_project_df <- merged_project_df %>%
  filter(!is.na(grantName)) %>%
  group_by_at(vars(-c(proj_summarize_cols))) %>%
  summarize(
    grantName = str_c(unique(grantName), collapse = ", "),
    grantType = str_c(unique(grantType), collapse = ", "),
    themeId = str_c(unique(themeId), collapse = ", "),
    theme = str_c(unique(theme), collapse = ", "),
    consortiumId = str_c(unique(consortiumId), collapse = ", "),
    consortium = str_c(unique(consortium), collapse = ", ")) %>%
  ungroup() %>%
  # rowwise() %>%
  mutate_at(proj_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>%
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


## Datasets - merged table

merged_dataset_df <- db_dataset_df %>%
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
        grant = grantNumber
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
         themeId, theme, grant) %>%
  mutate_all(~ ifelse(. == "[NA (PMID:NA)](NA)", NA, .)) %>%
  distinct()

dataset_list_cols <- c(
  "assay",
  "species",
  "tumorType",
  "theme",
  "consortium",
  "grant"
)

dataset_summary_cols <- c(
  "grantId",
  "grantName",
  "tumorType",
  "assay",
  "species",
  "themeId",
  "theme",
  "consortiumId",
  "consortium",
  "grant"
)

merged_formatted_dataset_df <- merged_dataset_df %>%
  filter(!is.na(grantName)) %>%
  group_by_at(vars(-c(dataset_summary_cols))) %>%
  summarize(
    grantId = str_c(unique(grantId), collapse = ", "),
    grantName = str_c(unique(grantName), collapse = ", "),
    grant = str_c(unique(grant), collapse = ", "),
    tumorType = str_c(unique(tumorType), collapse = ", "),
    assay = str_c(unique(assay), collapse = ", "),
    species = str_c(unique(species), collapse = ", "),
    themeId = str_c(unique(themeId), collapse = ", "),
    theme = str_c(unique(theme), collapse = ", "),
    consortiumId = str_c(unique(consortiumId), collapse = ", "),
    consortium = str_c(unique(consortium), collapse = ", ")) %>%
  ungroup() %>%
  # rowwise() %>%
  mutate_at(dataset_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>%
  # ungroup() %>%
  distinct()


# initial table creation (synIDs need to be integers)
# merged_dataset_syntable <- merged_formatted_dataset_df %>%
#   mutate_at(vars(contains("Id")), ~ str_replace(., "syn", "")) %>%
#   synBuildTable("Portal - datasets Merged", "syn7080714", .) %>%
#   synStore()

merged_dataset_table <- merged_formatted_dataset_df %>%
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
      select(grantId = id, grantName = name, grantType, consortiumId),
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
    db_dataset_df %>%
      select(datasetId = id, dataset = displayName)
  ) %>%
  select(publicationId, doi, journal,
         pubMedUrl, publicationTitle, publicationYear, keywords,
         authors = person, assay, tissue, tumorType,
         consortium, grantName, theme, datasetId, dataset,
         themeId, consortiumId, grantId) %>%
  distinct() %>%
  mutate_all(~ ifelse(str_detect(., "Not Applicable"), NA, .)) %>%
  mutate_all(~ ifelse(str_detect(., "^NA$"), NA, .))

pub_summary_cols <- c(
  "grantName",
  "authors",
  "assay",
  "tumorType",
  "grantId",
  "themeId",
  "consortiumId",
  "datasetId",
  "dataset",
  "tissue",
  "theme",
  "consortium"
)

pub_list_cols <- c(
  "assay",
  "tumorType",
  "tissue",
  "theme",
  "consortium"
)

merged_formatted_pub_df <- merged_pub_df %>%
  filter(!is.na(grantName)) %>%
  group_by_at(vars(-c(pub_summary_cols))) %>%
  summarize(grantName = str_c(unique(grantName), collapse = ", "),
            authors = str_c(unique(authors), collapse = ", "),
            assay = str_c(unique(assay), collapse = ", "),
            tissue = str_c(unique(tissue), collapse = ", "),
            tumorType = str_c(unique(tumorType), collapse = ", "),
            grantId = str_c(unique(grantId), collapse = ", "),
            theme = str_c(unique(theme), collapse = ", "),
            themeId = str_c(unique(themeId), collapse = ", "),
            consortiumId = str_c(unique(consortiumId), collapse = ", "),
            consortium = str_c(unique(consortium), collapse = ", "),
            datasetId = str_c(unique(datasetId), collapse = ", "),
            dataset = str_c(unique(dataset), collapse = ", ")
            ) %>%
  ungroup() %>%
  # rowwise() %>%
  mutate_at(pub_list_cols, ~ purrr::map_chr(., csv_str_to_json)) %>%
  # ungroup() %>%
  distinct()

# merged_pub_cols <- list(
#   Column(name = 'authors', columnType = 'LARGETEXT'),
#   Column(name = 'doi', columnType = 'STRING', maximumSize = 29),
#   Column(name = 'journal', columnType = 'STRING', maximumSize = 39),
#   Column(name = 'pubMedUrl', columnType = 'LINK', maximumSize = 50),
#   Column(name = 'publicationTitle', columnType = 'STRING', maximumSize = 203),
#   Column(name = 'publicationYear', columnType = 'INTEGER', maximumSize = 4),
#   Column(name = 'keywords', columnType = 'STRING', maximumSize = 243),
#   Column(name = 'theme', columnType = 'STRING_LIST', maximumSize = 27),
#   Column(name = 'tumorType', columnType = 'STRING_LIST', maximumSize = 50),
#   Column(name = 'tissue', columnType = 'STRING_LIST', maximumSize = 26),
#   Column(name = 'assay', columnType = 'STRING_LIST', maximumSize = 60),
#   Column(name = 'grantName', columnType = 'STRING_LIST', maximumSize = 107),
#   Column(name = 'consortium', columnType = 'STRING_LIST', maximumSize = 5),
#   Column(name = 'grantType', columnType = 'STRING', maximumSize = 3)
# )

# merged_pub_schema <- Schema(name = "Portal - Publications Merged (test 3)",
#                             columns = merged_pub_cols,
#                             parent = "syn7080714")
# merged_pub_schema

# merged_pub_table <- Table(merged_pub_schema, merged_formatted_pub_df)
# merged_pub_table <- synStore(merged_pub_table)

merged_pub_table <- merged_formatted_pub_df %>%
  update_synapse_table(dest_table_list["Portal - Publications Merged"], .)





## Tools - merged table

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
      select(grantId = id, grantName = name, grantType, consortiumId),
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
  select(toolId, toolName, description, dataType, role, softwareLanguage,
         homepageUrl = url, toolType,
         publicationId, publication = pubMedLink,
         theme, grantId, grantName, consortium,
         themeId, consortiumId) %>%
  mutate(publication = ifelse(str_detect(publication, "^\\[NA"), NA, publication)) %>%
  distinct()

tool_summary_cols <- c(
  "dataType",
  "grantId",
  "grantName",
  "publicationId",
  "publication",
  "softwareLanguage",
  "themeId",
  "theme",
  "consortiumId",
  "consortium"
)

tool_list_cols <- c(
  "softwareLanguage",
  "theme",
  "inputDataType",
  "outputDataType"
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
    grantName = str_c(unique(grantName), collapse = ", "),
    publicationId = str_c(unique(publicationId), collapse = ", "),
    publication = str_c(unique(publication), collapse = ", "),
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
  mutate_at(tool_list_cols, ~ purrr::map_chr(., csv_str_to_json))

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



