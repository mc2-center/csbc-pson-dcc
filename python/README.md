# Publications Manifest Generator

## Setup
1. Download the csbc-pson-dcc repository to your local machine:

```bash
git clone https://github.com/Sage-Bionetworks/csbc-pson-dcc.git
```

2. Open the `csbc-pson-dcc` directory with your editor of choice, then switch to the `python/` directory.  Copy `.envTemplate` as `.env`, then update `.env` to include your Synapse and Entrez credentials.

It is ideal to have an Entrez account when scraping information from PubMed (NCBI in general); providing credentials will enable you to send up to 10 requests per second (as opposed to only 3).  More information about that and how to create an account here.


3. If you don't have [Docker](https://www.docker.com/get-started) yet, install it onto your machine and let it run.


4. Once you have Docker installed and running, open a terminal and switch to the `csbc-pson-dcc/python/` directory.  Enter the following command into your terminal (including that period at the end):

```bash
docker build -t pubmed_crawler .
```

This will build an image called `pubmed_crawler` -- now you’re ready to start generating!

---

#### Note
Alternatively, if you rather run the crawler with the Python interpreter, install all of its dependencies first with `pip` (or `pip3` if you have multiple Python interpreters on your machine):

```bash
pip install -r requirements.txt
```

Afterward, source the environment variables from `.env` to the environment, so that the script will have access to the credentials:

```bash
source .env
```

## Usage

### Docker
Open a terminal and switch to the `csbc-pson-dcc/python/` directory.  Run the command:

```bash
docker run --rm -ti \
  --env-file .env  \
  --volume $PWD/output:/output:rw \
  pubmed_crawler
```

The `-ti` flags are not required, if you do not wish to get STDOUT in real-time, e.g.

```bash
...
Total unique publications: 1866

Comparing with table syn21868591...
      New publications found: 118

Pulling information from publications...
|██████████████▌                         | ▃▅▇ 43/118 [36%] in 1:15 (0.6/s, eta: 2:11)
```

Depending on how many new publications have been added to PubMed since the last scrape (and NCBI’s current requests traffic), this step could take anywhere from 10 seconds to 15ish minutes.

Once complete, a manifest should be found in `csbc-pson-dcc/python/output/`, with a name like `publications_manifest_yyyy-mm-dd.xlsx`. 

### Python
Open a terminal and switch to the csbc-pson-dcc/python/ directory.  Run the command:

```bash
python pubmed_crawler.py -t syn21868591
```

(You may need to use `python3` instead of `python` if you have multiple versions of Python.)

This will pull all grant numbers from the Portal - Grants Merged table (syn21918972), use them as the search terms in PubMed, then compare the PMIDs found in PubMed with the existing ones in the Portal - Publications Merged table (syn21868591).

Similar to the Docker approach, a new manifest should be found in `csbc-pson-dcc/python/output/`, with a name like `publications_manifest_yyyy-mm-dd.xlsx`. 

## Next Steps
1. Fill out the manifest by completing the annotations for:

* assay
* tumorType
* tissue

if applicable.

If there are accompanying datasets, data files, and/or tools for the publications, fill out the relevant manifests.  Templates are available under `csbc-pson-dcc/manifest_templates/`.

2. Validate and upload the manifest with the [CSBC/PS-ON curator tool](https://shinypro.synapse.org/users/vchung/csbc-pson-manifest/).
