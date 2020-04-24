---
title: "List Column Switch"
output: 
  html_document: 
    toc: yes
    toc_float: true
---

## Summary

Convert a single column in a Synapse Table with comma-separated lists of values in `STRING` items into a `STRING_LIST` column (and save a copy of the original); remove the `STRING_LIST` column and revert to the original if you need to access the table with **synapser**.

## Setup

### Environment


```r
library(reticulate)
library(dccvalidator)
library(tidyverse)

# local conda env with Python 3.7 and synapseclient installed
use_condaenv("csbc-pson-dcc", required = TRUE)
```

### Synapse things


```r
synapseclient <- reticulate::import("synapseclient")
syntab <- reticulate::import("synapseclient.table")
syn <- synapseclient$Synapse()
syn$login()
```

## Functions

### Helpers


```r
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
  write_csv(update_df, tmpfile)
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
.csv_str_to_json <- function(str) {
  json_str <- str_split(str, ",")[[1]] %>% 
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
```

### The meat

#### `add_list_column`


```r
add_list_column <- function(table_id, column_name, delimiter = ",", 
                            syn, syntab) {
  df <- dccvalidator::get_synapse_table(table_id, syn)
  
  old_name <- str_c(column_name, "asCsv", sep = "_")
  column_name <- rlang::ensym(column_name)
  old_name <- rlang::ensym(old_name)
  
  df <- df %>% 
    mutate(!!old_name := str_trim(!!column_name)) %>% 
    rowwise() %>% 
    mutate(!!column_name := .csv_str_to_json(!!column_name)) %>% 
    ungroup()
  
  max_str_len <- .max_list_str_length(df[[rlang::as_string(column_name)]])*1.5
  max_str_len <- as.integer(round(max_str_len))

  col_id <-reticulate::iterate(syn$getTableColumns(table_id))  %>%
    map(~ jsonlite::fromJSON(.$json())) %>%
    keep(~ .$name == column_name) %>%
    map_chr("id")
  old_column <- syn$getColumn(col_id)


  new_column <- syn$store(
    syntab$Column(
      name = rlang::as_string(column_name),
      columnType = "STRING_LIST",
      maximumSize = max_str_len,
      facetType = "enumeration"
    )
  )

  schema <- syn$get(table_id)
  schema$removeColumn(old_column)
  schema$addColumn(new_column)

  old_column_data <- jsonlite::fromJSON(old_column$json())
  old_column <- syn$store(
    syntab$Column(
      name = rlang::as_string(old_name),
      columnType = old_column_data$columnType,
      maximumSize = old_column_data$maximumSize
    )
  )
  schema$addColumn(old_column)

  schema <- syn$store(schema)

  update_synapse_table(table_id, df, syn, syntab)
}
```

#### `remove_list_column`


```r
remove_list_column <- function(table_id, column_name, delimiter = ",", 
                               syn, syntab) {
  df <- dccvalidator::get_synapse_table(table_id, syn)
  
  new_name <- str_c(column_name, "asCsv", sep = "_")
  column_name <- rlang::ensym(column_name)
  new_name <- rlang::ensym(new_name)
  
  df <- df %>% 
    select(-!!column_name) %>% 
    rename(!!column_name := !!new_name)

  col_id <-reticulate::iterate(syn$getTableColumns(table_id))  %>% 
    map(~ jsonlite::fromJSON(.$json())) %>% 
    keep(~ .$name == column_name) %>% 
    map_chr("id")
  old_column <- syn$getColumn(col_id)
  
  new_col_id <-reticulate::iterate(syn$getTableColumns(table_id))  %>% 
    map(~ jsonlite::fromJSON(.$json())) %>% 
    keep(~ .$name == rlang::as_string(new_name)) %>% 
    map_chr("id")
  new_column <- syn$getColumn(new_col_id)
  
  schema <- syn$get(table_id)
  schema$removeColumn(old_column)
  schema$removeColumn(new_column)
  schema <- syn$store(schema)
  
  new_column_data <- jsonlite::fromJSON(new_column$json())
  new_column <- syn$store(
    syntab$Column(
      name = rlang::as_string(column_name),
      columnType = new_column_data$columnType,
      maximumSize = new_column_data$maximumSize,
      facetType = "enumeration"
    )
  )
  schema$addColumn(new_column)
  
  schema <- syn$store(schema)
  
  update_synapse_table(table_id, df, syn, syntab)
}
```


## Example

### Adding a list column


```r
table_id <- "syn21980893"
dccvalidator::get_synapse_table(table_id, syn) %>% 
  knitr::kable()
```



|string_col |list_col_2       |list_col_1                |
|:----------|:----------------|:-------------------------|
|row1       |row12_1, row12_2 |row11_1, row11_2, row11_3 |




```r
table <- add_list_column(
  "syn21980893", column_name = "list_col_1", ",", syn, syntab
)
```


```r
dccvalidator::get_synapse_table(table_id, syn) %>% 
  knitr::kable()
```



|string_col |list_col_2       |list_col_1                        |list_col_1_asCsv          |
|:----------|:----------------|:---------------------------------|:-------------------------|
|row1       |row12_1, row12_2 |["row11_1", "row11_2", "row11_3"] |row11_1, row11_2, row11_3 |

### Removing a list column


```r
table <- remove_list_column(
  "syn21980893", column_name = "list_col_1", ",", syn, syntab
)
```

```
## Error in py_call_impl(callable, dots$args, dots$keywords): SynapseHTTPError: 412 Client Error: 
## Object: syn21980893 was updated since you last fetched it, retrieve it again and re-apply the update
```



```r
dccvalidator::get_synapse_table(table_id, syn) %>% 
  knitr::kable()
```



|string_col |list_col_2       |
|:----------|:----------------|
|row1       |row12_1, row12_2 |

## Fin.


```r
sessionInfo()
```

```
## R version 3.5.3 (2019-03-11)
## Platform: x86_64-apple-darwin15.6.0 (64-bit)
## Running under: macOS Mojave 10.14.6
## 
## Matrix products: default
## BLAS: /System/Library/Frameworks/Accelerate.framework/Versions/A/Frameworks/vecLib.framework/Versions/A/libBLAS.dylib
## LAPACK: /Library/Frameworks/R.framework/Versions/3.5/Resources/lib/libRlapack.dylib
## 
## locale:
## [1] en_US.UTF-8/en_US.UTF-8/en_US.UTF-8/C/en_US.UTF-8/en_US.UTF-8
## 
## attached base packages:
## [1] stats     graphics  grDevices utils     datasets  methods   base     
## 
## other attached packages:
##  [1] gistr_0.5.0        forcats_0.4.0      stringr_1.4.0     
##  [4] dplyr_0.8.3        purrr_0.3.4        readr_1.3.1       
##  [7] tidyr_1.0.0        tibble_2.1.3       ggplot2_3.2.0     
## [10] tidyverse_1.2.1    dccvalidator_0.2.0 shinyBS_0.61      
## [13] reticulate_1.14   
## 
## loaded via a namespace (and not attached):
##  [1] httr_1.4.0           pkgload_1.0.2        jsonlite_1.6        
##  [4] modelr_0.1.4         shiny_1.3.2          assertthat_0.2.1    
##  [7] askpass_1.1          highr_0.8            cellranger_1.1.0    
## [10] yaml_2.2.0           remotes_2.0.4        pillar_1.4.2        
## [13] backports_1.1.4      lattice_0.20-38      glue_1.3.1          
## [16] digest_0.6.20        promises_1.0.1       rvest_0.3.4         
## [19] colorspace_1.4-1     htmltools_0.3.6      httpuv_1.5.1        
## [22] pkgconfig_2.0.2      broom_0.5.2          haven_2.1.0         
## [25] config_0.3           xtable_1.8-4         scales_1.0.0        
## [28] later_0.8.0          openssl_1.4          generics_0.0.2      
## [31] usethis_1.5.0        ellipsis_0.2.0.1     withr_2.1.2         
## [34] lazyeval_0.2.2       cli_1.1.0            magrittr_1.5        
## [37] crayon_1.3.4         readxl_1.3.1         mime_0.7            
## [40] evaluate_0.14        golem_0.2.1          fs_1.3.1            
## [43] dockerfiler_0.1.3    fansi_0.4.0          nlme_3.1-139        
## [46] xml2_1.2.0           shinydashboard_0.7.1 rsconnect_0.8.15    
## [49] tools_3.5.3          hms_0.5.0            lifecycle_0.1.0     
## [52] munsell_0.5.0        packrat_0.5.0        compiler_3.5.3      
## [55] rlang_0.4.0          grid_3.5.3           attempt_0.3.0       
## [58] rstudioapi_0.10      base64enc_0.1-3      rmarkdown_1.12      
## [61] testthat_2.1.1       gtable_0.3.0         curl_3.3            
## [64] roxygen2_7.1.0       R6_2.4.1             lubridate_1.7.4     
## [67] knitr_1.23           zeallot_0.1.0        utf8_1.1.4          
## [70] rprojroot_1.3-2      desc_1.2.0           stringi_1.4.3       
## [73] Rcpp_1.0.1           vctrs_0.2.0          tidyselect_0.2.5    
## [76] xfun_0.8
```

