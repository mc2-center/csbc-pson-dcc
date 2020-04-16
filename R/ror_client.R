ror_list_orgs <- function() {
  path <- "https://api.ror.org/organizations/"
  res <- httr::GET(
    path
  )
  return(httr::content(res))
}

ror_query_orgs <- function(query) {
  res <- httr::GET(
    "https://api.ror.org/organizations",
    query = list(
      query = query
    )
  )
  return(httr::content(res))
}

ror_filter_orgs <- function(filter_key, filter_value) {
  path <- glue::glue("https://api.ror.org/organizations?filter={key}:{value}", 
                     key = filter_key, value = filter_value)
  res <- httr::GET(
    path = path
  )
  return(httr::content(res))
}

.filter_res <- function(res, org_name) {
  res$items %>% 
    keep(~ str_detect(.$name, org_name)) %>% 
    .[[1]]
}

.unpack_org <- function(org_res) {
  org_res %>% 
    .[c("id", "name", "acronyms")] %>% 
    modify_at("acronyms", ~ ifelse(length(.), ., "")) %>% 
    as_tibble() %>% 
    unnest(acronyms) %>% 
    separate_rows(acronyms, sep = ",") %>% 
    mutate(acronyms = str_trim(acronyms)) %>% 
    rename(acronym = acronyms)
}

get_org_ror <- function(org_name) {
  message(org_name)
  ror_query_orgs(org_name) %>% 
    .filter_res(org_name) %>% 
    .unpack_org()
}