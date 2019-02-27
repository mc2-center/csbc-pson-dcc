## Instantiate the grant (aka project) schema for CSBC described here:
## https://sagebionetworks.jira.com/wiki/spaces/PS/pages/742785034/Projects+Grants

## This script parses a CSBC/PS-ON center's wiki and expects it to the be formatted as:
## **Center Title**\n<center title>\n\n
## **Overall Project Title\n<project title>\n\n
## **Center Website\n[blah](<center url>)\n\n
## **Center Summary\n<abstract>

## where

## <center title>, <project title>, <center url>, and <abstract> are the text parsed out of the Wiki.

library(synapser)
synLogin()

## Synapse ID of the CSBC/PS-ON Project View
tbl.synId <- "syn10142562"

tbl <- as.data.frame(synTableQuery(sprintf("SELECT * FROM %s", tbl.synId)))

## Only keep U54s and U01s
tbl <- subset(tbl, grantType %in% c("U54", "U01"))

## Extract the following columns (schema name -> table column name)
## Grant/Project Name (none)
## Center Name (name)
## Grant Number (grantNumber)
## Program (consortium)
## Key Investigators (none)
## Institutions (institution)
## Link to Center Site (none)
## Link to Synapse Site (id)
## Abstract (none)
## Key Data Contributors (none)
## Grant Type (grantType)
## Funding Agency (fundingAgency)
## Synapse Team ID (teamProfileId)

col.translations <- list(
    "Project Name" = "name",
    "Grant Number" = "grantNumber",
    "Program" = "consortium",
    "Institutions" = "institution",
    "Link to Synapse Site" = "id",
    "Grant Type" = "grantType",
    "Funding Agency" = "fundingAgency",
    "Synapse Team ID" = "teamProfileId"
)

tmp <- unlist(col.translations)

new.tbl <- tbl[, as.vector(tmp)]
colnames(new.tbl) <- names(tmp)

## Extract the Abstract from the Wiki.


## Retrieve the wiki (markdown)
retrieve.wiki <- function(synId) {
    wiki <- synGetWiki(synId)
    wiki$markdown
}

new.tbl$wiki <- unlist(lapply(new.tbl$`Link to Synapse Site`, retrieve.wiki))

## Parse out the project title, center website, and abstract from the Wiki.
parse.wiki <- function(wiki) {
    project.title <- NA
    center.title <- NA    
    center.website <- NA
    abstract <- NA
    if(grepl(pattern="Center Title", wiki)) {
        center.title <- gsub(pattern=".+Center Title[^\n]*\n(.+)\n\n\\*\\*Overall Project Title.*", x=wiki,
                    replacement="\\1")
    }
    if(grepl(pattern="Project Title", wiki)) {
        project.title <- gsub(pattern=".+Project Title[^\n]*\n(.+)\n\n\\*\\*Center Website.*", x=wiki,
                    replacement="\\1")
    }
    if(grepl(pattern="Center Website", wiki)) {
        sub <- gsub(pattern=".+Center Website[^\n]*\n(.+)\n\n\\*\\*Center Summary.*", x=wiki,
                    replacement="\\1")
        center.website <- gsub(pattern=".+\\(([^\\)]+).+", x=sub, replacement="\\1")
    }
    if(grepl(pattern="Center Summary", wiki)) {
        sub <- gsub(pattern=".+Center Summary[^\n]*\n\n(.+)", x=wiki,
                    replacement="\\1")
        if(grepl(pattern="\n\n&nbsp;\n\n->.+<-", x=sub)) {
            sub <- gsub(pattern="(.+)\n\n&nbsp;\n\n->.+<-", x=sub, replacement="\\1")
        }
        abstract <- sub
    }
    df <- data.frame(project.title = as.character(project.title),
                     center.website = as.character(center.website),
                     abstract = as.character(abstract), stringsAsFactors = FALSE)
    df
}

wikis <- new.tbl$wiki
names(wikis) <- new.tbl$`Link to Synapse Site`

library(plyr)
tmp <- ldply(wikis, .fun = function(wiki) parse.wiki(wiki))

colnames(tmp) <- c("Link to Synapse Site", "Grant Name", "Link to Center Site", "Abstract")

new.tbl <- merge(tmp, new.tbl)

## Write the table out as an excel spreadsheet
library(openxlsx)
write.xlsx(new.tbl, file="csbc-pson-grant-table.xlsx")
