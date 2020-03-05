parse_bp_dbxref <- function(bp_dbxref) {
  c(id = purrr::flatten_chr(bp_dbxref$ID),
    db = attributes(bp_dbxref)$db)
}

get_pub_bp <- function(pmid) {
  bp_links <- rentrez::entrez_link(
    dbfrom = "pubmed", 
    id = pmid, 
    db = "bioproject"
  )
  bp_links$links$pubmed_bioproject
}

get_bp_data <- function(bpid, data_db) {
  bp_links <- rentrez::entrez_link(
    dbfrom = "bioproject", 
    id = bpid, 
    db = data_db
  )
  bp_links$links
}

get_bp_xrefs <- function(bpid) {
  bp_data <- rentrez::entrez_fetch(
    db = "bioproject", 
    id = bpid, 
    rettype = "xml"
  )
  bp <- xml2::as_list(xml2::as_xml_document(bp_data))$RecordSet
  # bp$DocumentSummary
  bp$DocumentSummary$Project$ProjectDescr$ExternalLink$dbXREF
}

get_pub_dbxrefs <- function(pmid) {
  matched_bp <- get_pub_bp(pmid)
  if (length(matched_bp)) {
    bp_dbxrefs <- get_bp_xrefs(matched_bp)
    parse_bp_dbxref(bp_dbxrefs)
  }
}


# # c("28465358", pmids$pmid[1:20]) %>% 
# pub_data <- pmids$pmid %>% 
#   set_names(.) %>% 
#   map(get_pub_dbxrefs) %>% 
#   discard(is.null) %>% 
#   map_df(enframe, .id = "pmid") %>% 
#   spread(name, value)