# Publications Manifest Generator

## Setup
1. Create a file called `.env`, then copy-paste the following.  Replace
values in the file with your Synapse PAT and Entrez credentials.

```
###########################
# Login Credentials       #
###########################

## Used for logging in to Synapse
# For security purposes, you should provide your personal access token
# (PAT) rather than your password. You can generate PATs on Synapse by
# going to Account Settings > Manage Personal Access Tokens.
SYNAPSE_AUTH_TOKEN="PAT"


###########################
# Entrez Credentials     #
###########################
# Providing a valid email and API key will allow up to 10 requests
# per second. You may experience errors if this information is not
# provided.
ENTREZ_EMAIL=email
ENTREZ_API_KEY=apikey
```

It is ideal to have an Entrez account when scraping information from PubMed
(NCBI in general); providing credentials will enable you to send up to 10
requests per second (as opposed to only 3).


2. If you do not have [Docker](https://www.docker.com/get-started) yet, install
it onto your machine and let it run.

3. Once you have Docker installed on your machine, open a terminal and log into
the Synapse Docker hub with `docker login docker.synapse.org`.


## Usage

### Docker
Open a terminal and (optional) switch to the directory containing your .env
file.  Run the command:

```bash
docker run --rm -ti \
  --env-file /path/to/.env \
  --volume $PWD/output:/output:rw \
  docker.synapse.org/syn7080714/pubmed_crawler
```

If this is your first time running the command, Docker will first pull the image. 
Note that the `-ti` flags are not required, if you do not wish to get STDOUT in 
real-time, e.g.

```bash
...
Total unique publications: 1866

Querying for grant numbers... 77 found

Getting PMIDs from NCBI...
  Total unique publications: 2524
...
```

Depending on how many new publications have been added to PubMed since the last
scrape (and NCBI’s current requests traffic), this step could take anywhere from
10 seconds to 15ish minutes.

Once complete, a manifest will be found in `output/`, with a name like 
`publications_manifest_yyyy-mm-dd.xlsx`, as well as manifest templates
for datasets, files, and tools.

## Next Steps
1. Fill out the manifest by completing the annotations for:

* assay
* tumorType
* tissue

if applicable.

If there are accompanying datasets, data files, and/or tools for the
publications, fill out the relevant manifests.

2. Validate and upload the manifest with the Data Curator App (coming soon!).
