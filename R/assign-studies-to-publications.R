#!/usr/bin/env Rscript

suppressPackageStartupMessages(library("pacman"))
suppressPackageStartupMessages(p_load("optparse"))
suppressPackageStartupMessages(p_load("plyr"))
suppressPackageStartupMessages(p_load("dplyr"))
suppressPackageStartupMessages(p_load("xlsx"))
suppressPackageStartupMessages(p_load("stringi"))

option_list <- list(
    make_option(c("--input-publication-table"), action="store",
                default=NULL,
                help="Publication table, representing CSBC/PS-ON publications for the portal, in XLS format."),
    make_option(c("--input-publication-table-authors-column"), action="store",
                default="Authors",
                help="Column in input-publication-table holding comma-separated list of authors (Last_Name First_Initials, Last_Name ...)"),
    make_option(c("--input-publication-table-grant-synapse-id-column"), action="store",
                default="CSBC.PSON.Center",
                help="Column in input-study-pi-table listing synapse ID of grant (U01 or U54) [default: %default]"),
    make_option(c("--input-study-pi-table"), action="store",
                default=NULL,
                help="XLS file assigning PIs to grant studies"),
    make_option(c("--study-pi-table-grant-synapse-id-column"), action="store",
                default="synapseId",
                help="Column in input-study-pi-table listing synapse ID of grant (U01 or U54) [default: %default]"),
    make_option(c("--study-pi-table-grant-pi-column"), action="store",
                default="PI.last.name",
                help="Column in input-study-pi-table listing last name of grant- and study-associated PI [default: %default]"),
    make_option(c("--study-pi-table-grant-study-column"), action="store",
                default="Project",
                help="Column in input-study-pi-table listing study (i.e., U54 Project number) [default: %default]"),
    make_option(c("--output-publication-table"), action="store",
                default=NULL,
                help="Name of output publication table XLS file, which will have study appended"),
    make_option(c("--output-study-column"), action="store",
                default="study",
                help="Column to hold appended study [default: %default]")
)

descr <- "\
Assign studies to publications based on intersecting publication authors and study PIs.
Example usage: Rscript assign-studies-to-publications.R --input-publication-table=csbc-pson-publications-syn10923842-dec-9-2019.xls --input-study-pi-table=csbc-pson-pi-to-study-map.xls --output-publication-table=csbc-pson-publications-syn10923842-dec-9-2019-studies.xls --output-study-column=study.number
"

parser <- OptionParser(usage = "%prog [options] input.tsv", option_list=option_list, description=descr)

arguments <- parse_args(parser, positional_arguments = TRUE)
opt <- arguments$options

if( is.null(opt$`input-publication-table`) || is.null(opt$`output-publication-table`) || is.null(opt$`input-study-pi-table`)  ) {
  print_help(parser)
  q(status=1)
}



input.pub.tbl.file <- opt$`input-publication-table`
study.pi.file <- opt$`input-study-pi-table`
output.pub.tbl.file <- opt$`output-publication-table`

authors.col <- opt$`input-publication-table-authors-column`
study.pi.grant.col <- opt$`study-pi-table-grant-synapse-id-column`
pub.grant.col <- opt$`input-publication-table-grant-synapse-id-column`
pi.col <- opt$`study-pi-table-grant-pi-column`
input.study.col <- opt$`study-pi-table-grant-study-column`
output.study.col <- opt$`output-study-column`


pub.tbl <- read.xlsx(input.pub.tbl.file, sheetIndex = 1)
study.pi.tbl <- read.xlsx(study.pi.file, sheetIndex = 1)
pub.tbl$rowId <- 1:nrow(pub.tbl)

pub.to.authors <-
    ldply(1:nrow(pub.tbl),
          .fun = function(i) {
              authors <- as.character(pub.tbl[i, authors.col])
              authors <- unlist(strsplit(authors, split=",[ ]*"))
              authors <- unlist(lapply(authors, function(str) unlist(strsplit(str, split=" "))[1]))
              data.frame(rowId = pub.tbl[i, "rowId"], grantId = pub.tbl[i, pub.grant.col], authorLast = authors)
          })

tbl <- merge(pub.to.authors, study.pi.tbl, by.x = c("grantId", "authorLast"), by.y = c(study.pi.grant.col, pi.col))

tbl <-
    ddply(tbl, .variables = c("grantId", "authorLast", "rowId"),
          .fun = function(df) {
              data.frame(grantId = as.character(df$grantId[1]),
                         authorLast = as.character(df$authorLast[1]),
                         rowId = df$rowId[1],
                         studies = paste0(sort(df[, input.study.col]), collapse=","))
          })

tbl <-
    ddply(tbl, .variables = c("rowId", "grantId"),
          .fun = function(df) {
              study.vec <- lapply(df$studies, function(str) sort(unlist(strsplit(as.character(str), split=",[ ]*"))))
              studies <- Reduce("intersect", study.vec)
              if(length(studies) != 1) { studies = NA }
              ret <- data.frame(rowId = df$rowId[1], grantId = as.character(df$grantId[1]), study = studies)
              ret
          })

tbl <- tbl[order(tbl$rowId), ]

tbl <- tbl[, !(colnames(tbl) %in% output.study.col)]

if(output.study.col %in% colnames(pub.tbl)) {
    rand.col <- stri_rand_strings(1, 10, pattern="[A-Za-z]")[1]
    colnames(tbl)[3] <- rand.col
    merged <- merge(pub.tbl, tbl[, c("rowId", rand.col)], by = c("rowId"), all.x = TRUE)
    flag <- is.na(merged[, output.study.col])
    merged[flag, output.study.col] <- merged[flag, rand.col]
    merged <- merged[, !(colnames(merged) %in% c("rowId", rand.col))]    
} else {
    colnames(tbl)[3] <- output.study.col
    merged <- merge(pub.tbl, tbl[, c("rowId", output.study.col)], by = c("rowId"), all.x = TRUE)
    merged <- merged[order(merged$rowId),]
    merged <- merged[, !(colnames(merged) == "rowId")]
}

## foo <- subset(merged, Consortium == "CSBC" & grantType == "U54")
## cat(paste0("CSBC U54s: ", length(which(!is.na(foo[, output.study.col]))), " of ", nrow(foo), " CSBC U54 pubs automated for study\n"))

## foo <- subset(merged, Consortium == "PS-ON" & grantType == "U54")
## cat(paste0("PS-ON U54s: ", length(which(!is.na(foo[, output.study.col]))), " of ", nrow(foo), " PS-ON U54 pubs automated for study\n"))

write.xlsx(merged, file = output.pub.tbl.file)
q(status = 0)
