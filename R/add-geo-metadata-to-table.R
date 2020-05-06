#!/usr/bin/env Rscript

suppressPackageStartupMessages(library("pacman"))
suppressPackageStartupMessages(p_load("optparse"))
suppressPackageStartupMessages(p_load("plyr"))
suppressPackageStartupMessages(p_load("dplyr"))
suppressPackageStartupMessages(p_load("dplyr"))
suppressPackageStartupMessages(p_load("data.table"))

## install.packages("remotes")
## remotes::install_github("denalitherapeutics/archs4")
suppressPackageStartupMessages(p_load("archs4"))
suppressPackageStartupMessages(p_load("rentrez"))
suppressPackageStartupMessages(p_load("jsonlite"))

lookup_gse_rentrez <- function(accession) {
    geo_template <- "{accession}[ACCN] AND gse[FILT]"
    query <- glue::glue(geo_template)
    res <- rentrez::entrez_search("gds", query)
    df <- rentrez::entrez_summary("gds", res$ids) %>% 
        rentrez::extract_from_esummary(
                     c("uid", "accession", "gds", "title", "gpl", "gse", "taxon", "summary",
                       "gdstype", "ptechtype", "valtype", "ssinfo", "subsetinfo", "pdat",
                       "n_samples", "seriestitle", "platformtitle",
                       "platformtaxa", "samplestaxa", "pubmedids")
                 ) %>% 
        tibble::as_tibble() %>% 
        tidyr::gather(element, value)
    ret.list <- as.list(df$value)
    names(ret.list) <- df$element
    ret.list
}

lookup_gse_both <- function(accession) {
    c(lookup_gse_rentrez(accession), lookup_gse(accession))
}

option_list <- list(
    make_option(c("--input-csv"), action="store",
                default=NULL,
                help="Input CSV file to which GEO metadata will be appended"),
    make_option(c("--output-csv"), action="store",
                default="Authors",
                help="Output CSV file"),
    make_option(c("--id-column"), action="store",
                default=NULL,
                help="Column in `input-csv` holding (comma-separated list of) GSE ids or BioProject PRJNA ids."),
    make_option(c("--geo-query-annotations"), action="store",
                default="Title,Summary,Overall-Design,taxon",
                help="Comma-separated list of GEO annotations to query. This should be a subset of those queried by rentrez (uid, accession, gds, title, gpl, gse, taxon, summary, gdstype, ptechtype, valtype, ssinfo, subsetinfo, pdat, n_samples, seriestitle, platformtitle, platformtaxa, samplestaxa, pubmedids) and by archs4 (Accession, Title, Summary, Overall-Design, Type, Pubmed-ID, Sample)."),
    make_option(c("--output-annotation-columns"), action="store",
                default="datasetName,description,overallDesign,species",
                help="Comma-separated list of names of columns in which to store the annotations. This should parellel the `geo-query-annotations` flag.")
)

descr <- "\
Query GEO for annotations and append to table. Note the following caveats:
'taxon' will be translated from Mus musculus -> Mouse and from Homo sapiens -> Human.
`id-column` is assumed to have at most one GSE or at most one PRJNA id, which will be translated.
Only rows with a GSE or PRJNA id will be translated. `output-annotation-columns` for other rows will
be untouched (or set to NA).
"

parser <- OptionParser(usage = "%prog [options] input.tsv", option_list=option_list, description=descr)

arguments <- parse_args(parser, positional_arguments = TRUE)
opt <- arguments$options

input.csv <- as.character(opt$`input-csv`)
output.csv <- as.character(opt$`output-csv`)
id.column <- as.character(opt$`id-column`)
geo.query.annotations <- as.character(opt$`geo-query-annotations`)
output.annotation.columns <- as.character(opt$`output-annotation-columns`)

if( any(is.null(c(input.csv, output.csv, id.column, geo.query.annotations, output.annotation.columns))) ) {
  print_help(parser)
  q(status=1)
}

tbl <- fread(input.csv)
tbl <- as.data.frame(tbl)

input.cols <- unlist(strsplit(geo.query.annotations, split=",[ ]*"))
output.cols <- unlist(strsplit(output.annotation.columns, split=",[ ]*"))

if(length(input.cols) != length(output.cols)) {
    stop(paste0("Length of input cols (n=", length(input.cols), ") != length of output cols (n=", length(output.cols), ")\n"))
}

## Add empty output columns 
for(output.col in output.cols) {
    if(output.col %in% colnames(tbl)) { next }
    tbl[, output.col] <- NA
}

id.flag <- grepl(tbl[, id.column], pattern="GSE") | grepl(tbl[, id.column], pattern="PRJNA")

## Confirm there is at most one GSE or PRJNA id per columns
ids <- as.character(tbl[id.flag, id.column])

l_ply(ids,
      .fun = function(id.string) {
          xs <- unlist(strsplit(id.string, split=",[ ]*"))
          xs <- xs[grepl(xs, pattern="GSE") | grepl(xs, pattern="PRJNA")]
          if(length(xs) != 1) {
              stop(cat(paste0("Got multiple GSE/PRJNA ids in column: ", id.string, "\n")))
          }
      })

indices <- which(id.flag)

get.gse.from.prjna <- function(prjna) {
    r_search <- entrez_search(db="gds", term=paste0(prjna, "[ACCN]"))
    if(length(r_search$ids) != 1) { stop(paste0("Query did not return one hit\n")) }
    entrez_summary(db="gds", id=r_search$ids)$accession
}

translate.species <- function(val) {
    if(val == "Mus musculus") { val <- "Mouse" }
    if(val == "Homo sapiens") { val <- "Human" }
    if(val == "Rattus norvegicus") { val <- "Rat" }
    val
}

for(indx in indices) {
    id.string <- tbl[indx, id.column]
    ids <- unlist(strsplit(as.character(id.string), split=",[ ]*"))
    ids <- ids[grepl(ids, pattern="GSE") | grepl(ids, pattern="PRJNA")]
    if(length(ids) != 1) {
        stop(cat(paste0("Got multiple GSE/PRJNA ids in column: ", id.string, "\n")))
    }
    gse.id <- ids
    if(grepl(gse.id, pattern="PRJNA")) { gse.id <- get.gse.from.prjna(gse.id) }
    print(gse.id)
    annos <- lookup_gse_both(gse.id)
    input.cols <- tolower(input.cols)
    annos <- annos[input.cols]
    for(col.indx in 1:length(input.cols)) {
        val <- as.character(annos[input.cols[col.indx]])
        if(input.cols[col.indx] == "taxon") {
            val <- unlist(strsplit(val, split=";[ ]*"))
            val <- unlist(lapply(val, function(x) paste0("\"", translate.species(x), "\"")))
            val <- toJSON(val)
            print(val)            
        }
        tbl[indx, output.cols[col.indx]] <- val
    }
}

write.table(file=output.csv, tbl, sep=",", row.names=FALSE, col.names=TRUE, quote=TRUE)
