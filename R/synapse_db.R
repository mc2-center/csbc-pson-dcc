.create_dummy_entity <- function(entity_name, parent_id, syn, synfile) {
  file_handle = entity_name
  dummy_path = fs::path_join(c(tempdir(), file_handle))
  write_lines(entity_name, dummy_path)
  dummy_file <- synfile(dummy_path, parent = parent_id)
  syn$store(dummy_file)
}

create_entities <- function(entity_df, parent_id, syn, synfile) {
  existing_entities <- syn$getChildren(parent_id) %>%
    iterate() %>% 
    map_chr("name")
  
  if (length(existing_entities)) {
    entity_df <- entity_df %>% 
      filter(!id %in% existing_entities)
  }
  
  entity_names <- entity_df %>% 
    distinct(id) %>% 
    pluck("id")
  
  map(entity_names, with_progress(
    ~ .create_dummy_entity(., parent_id, syn, synfile)
  ))
}

# update_view <- function(view_id, update_df) {
#   view_df <- as.data.frame(synTableQuery(glue::glue("select * from {view_id}")))
#   
#   property_cols <- c("id", "name", "ROW_ID", "ROW_VERSION", "ROW_ETAG",
#                      "createdOn", "createdBy",
#                      "modifiedOn", "modifiedBy",
#                      "currentVersion")
#   annotation_cols <- setdiff(names(view_df), property_cols)
#   
#   view_df <- view_df %>%
#     select(-annotation_cols) %>%
#     left_join(update_df, by = c("name" = "id"))
# 
#   synview <- synTable(view_id, view_df)
#   synStore(synview)
# }
# 
# update_synapse_table <- function(table_id, update_df, syn, syntab) {
#   current_rows <- syn$tableQuery(glue::glue("SELECT * FROM {table_id}"))
#   syn$delete(current_rows)
#   tmpfile <- fs::file_temp("rows.csv")
#   write_csv(update_df, tmpfile)
#   update_rows <- syntab$Table(table_id, tmpfile)
#   syn$store(update_rows)
# }
# 
# update_table <- function(table_id, update_df) {
#   current_rows <- synTableQuery(glue::glue("SELECT * FROM {table_id}"))
#   synDelete(current_rows)
#   update_rows <- Table(table_id, update_df)
#   synStore(update_rows)
# }
# 
# csv_str_to_json <- function(str) {
#   json_str <- str_split(str, ",")[[1]] %>% 
#     str_trim() %>% 
#     jsonlite::toJSON()
#   if (json_str == "[null]") {
#     NA
#   } else {
#     json_str
#   }
# }