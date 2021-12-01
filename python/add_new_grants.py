"""Add Grants to the Cancer Complexity Knowledge Portal.

This script will iterate through a CSV file (see new_grants_template.csv)
and create a new Synapse Project and Wiki pages for each listed grant.
The `Portals - Grants Merged` table will also be updated to include their
metadata.

TODO:
  - include step of validating the metadata annotations
  - auto-add institutions not currently listed in `institutions`?

author: verena.chung
"""

import argparse
import getpass

import synapseclient
from synapseclient import Table, Project, Wiki
import pandas as pd


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
        syn = synapseclient.login(username, password, silent=True)
    return syn


def get_args():
    """Set up command-line interface and get arguments."""

    parser = argparse.ArgumentParser(
        description="Add new grants to the CSBC Knowledge Portal.")
    parser.add_argument("grants_file",
                        type=str,
                        help="CSV file of grants")
    parser.add_argument("-t", "--grants_table",
                        type=str, default="syn21918972",
                        help="Add grants to this specified table. "
                        + "(Default: syn21918972)")
    return parser.parse_args()


def create_wiki_pages(syn, project_id, grant_info):
    """Create Wiki pages for the grant.

    A total of three Wiki pages will be created:
        - main project page (inc. websites, institutions, abstract, etc.)
        - `Project Investigators` (list of PIs and their descriptions)
        - `Data and Tools` (currently empty)

    Assumptions:
        `piDesc` is already using the Synapse markdown language
    """

    # Main Wiki page
    consortium = grant_info["consortium"]
    grant_type = grant_info["grantType"]
    title = grant_info["institutionAlias"].lstrip(
        "[").rstrip("]").replace("\"", "")
    websites = grant_info["website"] or ""
    institutions = grant_info["grantInstitution"].lstrip(
        "[").rstrip("]").replace("\"", "")
    desc = grant_info["abstract"] or ""

    content = f"""### The {consortium} {grant_type} Research Project \@ {title}

#### Project Website
{websites}

#### List of Collaborating Institutions
{institutions}

#### Project Description
{desc}

"""
    content += "->${buttonlink?text=" + \
        "Back to CSBC PS%2DON Data Coordinating Center" + \
        "&url=https%3A%2F%2Fwww%2Esynapse%2Eorg%2F%23%21Synapse%3Asyn7080714%2F}<-"
    main_wiki = Wiki(title=grant_info["grantName"],
                     owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Project Investigators
    pis = grant_info["piDesc"]
    pi_wiki = Wiki(title="Project Investigators", owner=project_id,
                   markdown=pis, parentWikiId=main_wiki.id)
    pi_wiki = syn.store(pi_wiki)

    # Sub-wiki page: Data and Tools
    data_wiki = Wiki(title="Data and Tools", owner=project_id,
                     markdown="", parentWikiId=main_wiki.id)
    data_wiki = syn.store(data_wiki)


def add_grants(syn, input_file):
    """Create a new Synapse project for each grant and populate its Wiki.

    Returns:
        df: grants information (including their new Project IDs)
    """

    grants = pd.read_csv(input_file)
    grants.dropna(how="all", inplace=True)

    for _, row in grants.iterrows():
        project = Project(row["grantName"])
        project = syn.store(project)

        # Update grants table with new synId
        grants.loc[_, "grantId"] = project.id

        create_wiki_pages(syn, project.id, row)

    return grants


def upload_metadata(syn, grants, table):
    """Add grants metadata to the Synapse table.

    Assumptions:
        `grants` matches the same schema as `table` (with the exception
        of two columns; more below)
    """

    # `website` and `piDesc` was only needed for the Wiki page creation;
    # they do not need to be uploaded to the Synapse table.
    try:
        grants.drop(columns=['website', 'piDesc'], inplace=True)
    except KeyError:
        pass

    # Add annotations to Merged table
    schema = syn.get(table)
    new_rows = grants.values.tolist()
    table = syn.store(Table(schema, new_rows))


def main():
    """Main function."""

    syn = login()
    args = get_args()

    grants = add_grants(syn, args.grants_file)
    upload_metadata(syn, grants, args.grants_table)


if __name__ == "__main__":
    main()
