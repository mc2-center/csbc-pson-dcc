"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will iterate through a grants fileview and create a new
Synapse Project for each new grant not currently in a Grants table.
Metadata of new grants will also be added to the Grants table.

author: verena.chung
"""

import os
import argparse
import getpass

import synapseclient
from synapseclient import Table, Project, Wiki, Folder
import pandas as pd


def login():
    """Log into Synapse. If env variables not found, prompt user.

    Returns:
        syn: Synapse object
    """
    try:
        syn = synapseclient.login(
            authToken=os.getenv('SYNAPSE_AUTH_TOKEN'),
            silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Credentials not found; please manually provide your",
              "Synapse username and password.")
        username = input("Synapse username: ")
        password = getpass.getpass("Synapse password: ")
        syn = synapseclient.login(username, password, silent=True)
    return syn


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new grants to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, default="syn32134242",
                        help="Synapse ID to grants manifest fileview. (Default: syn32134242)")
    parser.add_argument("-t", "--grants_table",
                        type=str, default="syn21918972",
                        help="Add grants to this specified table. (Default: syn21918972)")
    return parser.parse_args()


def create_wiki_pages(syn, project_id, grant_info):
    """Create main Wiki page for the Project."""

    # Main Wiki page
    consortium = grant_info["GrantConsortiumName"]
    grant_type = grant_info["GrantType"]
    title = grant_info["GrantInstitutionAlias"]
    institutions = grant_info["GrantInstitutionName"]
    desc = grant_info["GrantAbstract"] or ""

    content = f"""### The {consortium} {grant_type} Research Project \@ {title}

#### List of Collaborating Institutions
{institutions}

#### Project Description
{desc}

"""
    content += (
        "->"
        "${buttonlink?text="
        "Back to CSBC PS%2DON Data Coordinating Center"
        "&url=https%3A%2F%2Fwww%2Esynapse%2Eorg%2F%23%21Synapse%3Asyn7080714%2F}"
        "<-"
    )
    main_wiki = Wiki(title=grant_info["GrantName"],
                     owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Project Investigators
    pis = [pi.lstrip(" ").rstrip(" ")
           for pi
           in grant_info["GrantInvestigator"].split(",")]
    pi_markdown = "* " + "\n* ".join(pis)
    pi_wiki = Wiki(title="Project Investigators", owner=project_id,
                   markdown=pi_markdown, parentWikiId=main_wiki.id)
    pi_wiki = syn.store(pi_wiki)


def create_folders(syn, project_id):
    """Create top-levels expected by the DCA.

    Folders:
        - projects
        - publications
        - datasets
        - tools
    """
    syn.store(Folder("projects", parent=project_id))
    syn.store(Folder("publications", parent=project_id))
    syn.store(Folder("datasets", parent=project_id))
    syn.store(Folder("tools", parent=project_id))


def syn_prettify(name):
    """Prettify a name that will conform to Synapse naming rules.

    Names can only contain letters, numbers, spaces, underscores, hyphens,
    periods, plus signs, apostrophes, and parentheses.
    """
    valid = {38: 'and', 58: '-', 59: '-', 47: '_'}
    return name.translate(valid)


def create_grant_projects(syn, grants):
    """Create a new Synapse project for each grant and populate its Wiki.

    Returns:
        df: grants information (including their new Project IDs)
    """
    for _, row in grants.iterrows():
        name = syn_prettify(row["GrantName"])
        try:
            project = Project(name)
            project = syn.store(project)
            syn.setPermissions(
                project.id, principalId=3450948,
                accessType=['CREATE', 'READ', 'UPDATE', 'DELETE', 'DOWNLOAD',
                            'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS', 'MODERATE'],
            )

            # Update grants table with new synId
            grants.at[_, "GrantId"] = project.id

            # Update `GrantId` annotation for ent.
            # annots = syn.get_annotations(row['id'])
            # annots['GrantId'] = project.id
            # syn.set_annotations(annots)

            create_wiki_pages(syn, project.id, row)
            create_folders(syn, project.id)
        except synapseclient.core.exceptions.SynapseHTTPError:
            print(f"Skipping: {name}")
            grants.at[_, "GrantId"] = ""

    return grants


def upload_metadata(syn, grants, table):
    """Add grants metadata to the Synapse table.

    Assumptions:
        `grants` matches the same schema as `table`
    """
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'GrantId', 'GrantName', 'GrantNumber', 'GrantAbstract', 'GrantType',
        'GrantThemeName', 'GrantInstitutionAlias', 'GrantInstitutionName',
        'GrantInvestigator', 'GrantConsortiumName'
    ]
    grants = grants[col_order]

    # Convert columns into STRINGLIST.
    grants.loc[:, 'GrantThemeName'] = grants.GrantThemeName.str.split(", ")
    grants.loc[:, 'GrantInstitutionName'] = grants.GrantInstitutionName.str.split(", ")
    grants.loc[:, 'GrantInstitutionAlias'] = grants.GrantInstitutionAlias.str.split(", ")

    new_rows = grants.values.tolist()
    table = syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = login()
    args = get_args()

    manifest = syn.tableQuery(f"SELECT * FROM {args.manifest}").asDataFrame()
    curr_grants = (
        syn.tableQuery(f"SELECT grantNumber FROM {args.grants_table}")
        .asDataFrame()
        .grantNumber
        .to_list()
    )

    # Only add grants not currently in the Grants table.
    new_grants = manifest[~manifest.GrantNumber.isin(curr_grants)]

    if new_grants.empty:
        print("No new grants found!")
    else:
        print(f"{len(new_grants)} new grants found!\nAdding new grants...")
        added_grants = create_grant_projects(syn, new_grants)
        upload_metadata(syn, added_grants, args.grants_table)

    print("DONE ✓")


if __name__ == "__main__":
    main()
