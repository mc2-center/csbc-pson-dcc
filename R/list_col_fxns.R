#' Refresh the rows of a table in Synapse
#'
#' Note: this will overwrite all existing rows, not attempt to merge or
#' update only those rows with changes.
#'
#' @param table_id A string specifying the Synapse Table ID
#' @param update_df A dataframe or tbl representing the new version of the table
#' @param syn A reticulated synapseclient Python object
#' @param syntab A reticulated synapseclient.table module
#'
#' @return The reticulated synapseclient.table.CsvFileTable object representing
#' the updated table

#' @export
#'
#' @examples update_synapse_table("syn21980893", update_df, syn, syntab)
update_synapse_table <- function(table_id, update_df, syn, syntab) {
  current_rows <- syn$tableQuery(glue::glue("SELECT * FROM {table_id}"))
  syn$delete(current_rows)
  tmpfile <- fs::file_temp("rows.csv")
  write_csv(update_df, tmpfile, na = "")
  update_rows <- syntab$Table(table_id, tmpfile)
  syn$store(update_rows)
}

#' Convert comma-separated string values into a JSON array string
#'
#' @param str A string with values separated by commas
#'
#' @return The string containing a JSON array of the values
#' @export
#'
#' @examples
#' .csv_to_json("foo, bar, baz")
.delim_str_to_json <- function(str, delimiter = ",") {
  json_str <- str_split(str, delimiter)[[1]] %>% 
    str_trim() %>% 
    jsonlite::toJSON()
  if (json_str == "[null]") {
    "[]"
  } else {
    json_str
  }
}

#' Compute the maximum string length for a string list column with JSON arrays
#'
#' @param list_col A character vector with all items in the list column
#'
#' @return The (integer) length of the largest string
#' @export
#'
#' @examples 
#' .max_list_str_length(c('["123", "123456"]', '["1234", "12345"]' )
.max_list_str_length <- function(list_col) {
  map(list_col, function(l) {
    if (!is.na(l)) {
      jsonlite::fromJSON(l) %>% 
        map_int(str_length) %>% 
        max(na.rm = TRUE) %>%
        I
    } else {
      0L
    }
  }) %>% 
    discard(is.infinite) %>%
    flatten_int() %>% 
    max()
}

.max_list_size <- function(list_col) {
  map(list_col, function(l) {
    if (!is.na(l)) {
      jsonlite::fromJSON(l) %>% 
        length()
    } else {
      0L
    }
  }) %>% 
    discard(is.infinite) %>%
    flatten_int() %>% 
    max()
}

add_list_column <- function(table_id, column_name, delimiter = ",", 
                            syn, syntab) {
  df <- dccvalidator::get_synapse_table(table_id, syn)
  
  new_name <- str_c(column_name, "list", sep = "_")
  column_name <- rlang::ensym(column_name)
  new_name <- rlang::ensym(new_name)
  
  df <- df %>% 
    rowwise() %>% 
    mutate(!!new_name := .delim_str_to_json(!!column_name, delimiter)) %>% 
    ungroup()
  
  # df
  max_str_len <- .max_list_str_length(df[[rlang::as_string(new_name)]])*1.5
  max_str_len <- as.integer(round(max_str_len))
  print(max_str_len)
  max_size <- .max_list_size(df[[rlang::as_string(new_name)]]) + 1
  max_size <- as.integer(round(max_size))
  print(max_size)
  df
}