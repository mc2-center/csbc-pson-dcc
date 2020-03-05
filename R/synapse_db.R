.create_dummy_entity <- function(entity_name, parent_id) {
  file_handle = entity_name
  dummy_path = fs::path_join(c(tempdir(), file_handle))
  write_lines(entity_name, dummy_path)
  dummy_file <- File(dummy_path, parent = parent_id)
  synStore(dummy_file)
}

create_entities <- function(entity_df, parent_id) {
  existing_entities <- as.list(synGetChildren(parent_id)) %>% 
    map_chr("name")
  
  entity_names <- entity_df %>% 
    filter(!id %in% existing_entities) %>% 
    distinct(id) %>% 
    pluck("id")
  
  map(entity_names, with_progress(~ .create_dummy_entity(., parent_id)))
}


update_view <- function(view_id, update_df) {
  view_df <- as.data.frame(synTableQuery(glue::glue("select * from {view_id}")))
  view_df
  property_cols <- c("id", "name", "ROW_ID", "ROW_VERSION", "ROW_ETAG",
                     "createdOn", "createdBy",
                     "modifiedOn", "modifiedBy",
                     "currentVersion")
  annotation_cols <- setdiff(names(view_df), property_cols)
  
  view_df <- view_df %>%
    select(-annotation_cols) %>%
    left_join(update_df, by = c("name" = "id"))

  synview <- synTable(view_id, view_df)
  synStore(synview)
}
