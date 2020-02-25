"""PubMed Crawler of CSBC/PS-ON Publications.

author: nasim.sanati
author: milen.nikolov
author: verena.chung
"""

import os
import re
import argparse
import getpass
import ssl
from datetime import datetime
import requests

from Bio import Entrez
from bs4 import BeautifulSoup
import synapseclient
import pandas as pd
from alive_progress import alive_bar


def login():
    """Log into Synapse. If cached info not found, prompt user.

    Returns:
        syn: Synapse object
    """

    try:
        syn = synapseclient.login(silent=True)
    except Exception:
        print("Cached credentials not found; please provide",
              "your Synapse username and password.")
        username = input("Synapse username: ")
        password = getpass.getpass("Synapse password: ").encode("utf-8")
        syn = synapseclient.login(
            username=username, password=password,
            rememberMe=True, silent=True)
    return syn


def get_args():
    """Set up command-line interface and get arguments."""

    parser = argparse.ArgumentParser(
        description="Scrap PubMed information from a list of grant numbers"
        + " and put the results into a CSV file.  Table ID can be provided"
        + " if interested in only scrapping for new publications.")
    parser.add_argument("-g", "--grantview_id",
                        type=str, default="syn10142562",
                        help="Synapse table/view ID containing grant numbers in"
                        + " 'grantNumber' column. (Default: syn10142562)")
    parser.add_argument("-t", "--table_id",
                        type=str,
                        help="Current Synapse table holding PubMed info.")
    parser.add_argument("-c", "--consortium_name",
                        type=str, required=True,
                        help="Name of consortium, e.g. CSBC")
    parser.add_argument("-o", "--output",
                        type=str, default="publications_"
                        + datetime.today().strftime('%m-%d-%Y'),
                        help="Filename for output CSV. (Default:"
                        + " publications_<current-date>)")
    return parser.parse_args()


def get_view(syn, table_id):
    """Get Synapse table/data view containing grant numbers.

    Assumptions:
        Syanpse table/view has column called 'grantNumber'

    Returns:
        dataframe: consortiums and their project descriptions.
    """

    results = syn.tableQuery(
        f"select * from {table_id}").asDataFrame()
    return results[~results['grantNumber'].isnull()]


def get_grants(df):
    """Get list of grant numbers from dataframe.

    Assumptions:
        Dataframe has column called 'grantNumber'

    Returns:
        set: valid grant numbers, e.g. non-empty strings
    """

    print(f"Querying for grant numbers...", end="")
    grants = set(df.grantNumber.dropna())
    print(f"{len(grants)} found\n")
    return grants


def get_pmids(grants):
    """Get list of PubMed IDs using grant numbers as search param.

    Returns:
        set: PubMed IDs
    """

    print("Getting PMIDs from NCBI...")
    all_pmids = set()

    # Brian's request: add check that pubs. are retreived for each grant number
    count = 1
    for grant in grants:
        print(f"  {count:02d}. Grant number {grant}...", end="")
        handle = Entrez.esearch(db="pubmed", term=grant,
                                retmax=1_000_000, retmode="xml", sort="relevance")
        pmids = Entrez.read(handle).get('IdList')
        handle.close()
        all_pmids.update(pmids)
        print(f"{len(pmids)} found")
        count += 1
    print(f"Total unique publications: {len(all_pmids)}\n")
    return all_pmids


def parse_grant(grant):
    """Parse for grant number from grant annotation."""

    grant_info = re.search(r"(CA\d+)[ /-]", grant, re.I)
    return grant_info.group(1).upper()


def get_related_info(pmid):
    """Get related information associated with publication.

    Entrez will be used for optimal retrieval (since NCBI will kick
    us out if we web-scrap too often).

    Returns:
        dict: XML results for GEO, SRA, and dbGaP
    """

    handle = Entrez.elink(dbfrom="pubmed", db="gds,sra,gap", id=pmid,
                          remode="xml")
    results = Entrez.read(handle)[0].get('LinkSetDb')
    handle.close()

    related_info = {}
    for result in results:
        db = re.search(r"pubmed_(.*)", result.get('LinkName')).group(1)
        ids = [link.get('Id') for link in result.get('Link')]

        handle = Entrez.esummary(db=db, id=",".join(ids))
        soup = BeautifulSoup(handle, "lxml")
        handle.close()
        related_info[db] = soup

    return related_info


def parse_geo(info):
    """Parse and return GSE IDs."""

    gse_ids = []
    if info:
        tags = info.find_all('item', attrs={'name': "GSE"})
        gse_ids = ["GSE" + tag.text for tag in tags]
    return gse_ids


def parse_sra(info):
    """Parse and return SRX/SRP IDs."""

    srx_ids = srp_ids = []
    if info:
        tags = info.find_all('item', attrs={'name': "ExpXml"})
        srx_ids = [re.search(r'Experiment acc="(.*?)"', tag.text).group(1)
                   for tag in tags]
        srp_ids = {re.search(r'Study acc="(.*?)"', tag.text).group(1)
                   for tag in tags}
    return srx_ids, srp_ids


def parse_dbgap(info):
    """Parse and return study IDs."""

    gap_ids = []
    if info:
        tags = info.find_all('item', attrs={'name': "d_study_id"})
        gap_ids = [tag.text for tag in tags]
    return gap_ids


def make_urls(url, accessions):
    """Create NCBI link for each accession in the iterable.

    Returns:
        str: list of URLs
    """

    url_list = [url + accession for accession in list(accessions)]
    return ", ".join(url_list)


def scrap_info(pmids, curr_grants, consortium, name):
    """Create dataframe of publications and their pulled data.

    Returns:
        df: publications data
    """

    first_colname = "CSBC PSON Center" if re.search(
        r"csbc", name, re.I) else "Consortium Center"
    columns = [first_colname, "Consortium", "PubMed", "Journal",
               "Publication Year", "Title", "Authors", "Grant",
               "SRX", "SRX Link", "SRP", "SRP Link", "dbGaP", "dbGaP Link",
               "Data Location", "Synapse Location", "Keywords"]

    if not os.environ.get('PYTHONHTTPSVERIFY', '') \
            and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

    table = []
    with alive_bar(len(pmids)) as progress:
        for pmid in pmids:
            session = requests.Session()
            url = f"https://www.ncbi.nlm.nih.gov/pubmed/?term={pmid}"
            soup = BeautifulSoup(session.get(url).content, "lxml")

            # TITLE
            title = soup.find(attrs={"class": "rprt abstract"})
            title = title.h1.text.rstrip(".")

            # CITATION (including JOURNAL and PUB. YEAR)
            citation = soup.find(attrs={"class": "cit"}).text
            journal = citation[:citation.find(".")]

            date_start = citation.find(".") + 1
            date_stop = citation.find(";") or citation.find(".")
            year = citation[date_start:date_stop].split()[0]

            # AUTHORS
            authors = [a.contents[0] for a in soup.find(
                'div', attrs={"class": "auths"}).find_all('a')]
            authors = ", ".join(authors)

            # GRANTS
            grants = [g.contents[0] for g in soup
                      .find('div', attrs={"class": "rprt_all"})
                      .find_all('a', attrs={"abstractlink": "yes", "alsec": "grnt"})]

            # Filter out grant annotations not in consortia.
            grants = {parse_grant(grant) for grant in grants
                      if re.search(r"CA\d", grant, re.I)}
            grants = list(filter(lambda x: x in curr_grants, grants))

            # Nasim's note: match and get the grant center Synapse ID from
            # its view table by grant number of this journal study.
            center_id = center_name = consortium_grants = ""
            if grants:
                center = consortium.loc[consortium['grantNumber'].isin(grants)]
                center_id = ", ".join(list(set(center.id)))
                center_name = ", ".join(list(set(center.consortium)))
            consortium_grants = ", ".join(
                [center.grantType.iloc[0] + " " + grant for grant in grants])

            # KEYWORDS
            keywords = soup.find(attrs={"class": "keywords"})
            keywords = keywords.find("p").text if keywords else ""

            # RELATED INFORMATION
            related_info = get_related_info(pmid)

            #   GEO
            gse_ids = parse_geo(related_info.get('gds'))
            gse_url = make_urls(
                "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=", gse_ids)

            #   SRA
            srx, srp = parse_sra(related_info.get('sra'))
            srx_url = make_urls("https://www.ncbi.nlm.nih.gov/sra/", srx)
            srp_url = make_urls(
                "https://trace.ncbi.nlm.nih.gov/Traces/sra/?study=", srp)

            #   DBGAP
            dbgaps = parse_dbgap(related_info.get('gap'))
            dbgap_url = make_urls(
                "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=", dbgaps)

            row = pd.DataFrame(
                [[center_id, center_name, url, journal, year, title, authors,
                  consortium_grants, ", ".join(srx), srx_url, ", ".join(srp),
                  srp_url, ", ".join(dbgaps), dbgap_url, gse_url,
                  "", keywords]],
                columns=columns)
            table.append(row)
            session.close()

            # Increment progress bar animation.
            progress()

    return pd.concat(table)


def find_publications(syn, args):
    """Get list of publications based on grants of consortia."""

    consortium = get_view(syn, args.grantview_id)
    grants = get_grants(consortium)
    pmids = get_pmids(grants)

    # If user provided a table ID, only scrap info from
    # publications not already listed in the provided table.
    if args.table_id:
        print(f"Comparing with table {args.table_id}...")
        current_publications = syn.tableQuery(
            f"select * from {args.table_id}").asDataFrame()
        current_pmids = {re.search(r"[/=](\d+)$", i).group(1)
                         for i in list(current_publications.PubMed)}
        pmids -= current_pmids
        print(f"  New publications found: {len(pmids)}\n")

    print(f"Pulling information from publications...")

    table = scrap_info(pmids, grants, consortium, args.consortium_name)
    table.to_csv(args.output + ".csv", index=False, sep="\t", encoding="utf-8")
    print("DONE")


def main():
    """Main function."""

    syn = login()
    args = get_args()

    # In order to make >3 Entrez requests/sec, 'email' and 'api_key'
    # params need to be set.
    Entrez.email = os.getenv('ENTREZ_EMAIL')
    Entrez.api_key = os.getenv('ENTREZ_API_KEY')

    find_publications(syn, args)


if __name__ == "__main__":
    main()
