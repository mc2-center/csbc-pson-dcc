"""Create Synapse Dataset Entities

TODO:
  - use Python client once Dataset support becomes available
  - get Folder annotations and add them to Dataset entity

author: verena.chung
"""

import os
import json
import getpass

import synapseclient
from synapseclient import Column

PARENT_ID = "syn21498902"
FOLDERS_ID = "syn22047105"


def login():
    """Log into Synapse. If env variables not found, prompt user.

    Returns:
        syn: Synapse object
    """
    try:
        syn = synapseclient.login(
            os.getenv('SYN_USERNAME'),
            authToken=os.getenv('SYN_PAT'),
            silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Credentials not found; please manually provide your",
              "Synapse username and password.")
        username = input("Synapse username: ")
        password = getpass.getpass("Synapse password: ")
        syn = synapseclient.login(username, password, silent=True)
    return syn


def create_dataset_schema(syn):
    """Create schema for creating a Dataset entity.

    Returns:
        list of ColumnModel IDS
    """
    cols = syn.createColumns(
        columns=[
            Column(name="id", columnType="ENTITYID"),
            Column(name="fileName", columnType="STRING", maximumSize=256),
            Column(name="name", columnType="STRING", maximumSize=256),
            Column(name="species", columnType="STRING", maximumSize=25),
            Column(name="dataFormat", columnType="STRING", maximumSize=15),
            Column(name="assay", columnType="STRING", maximumSize=500),
            Column(name="tumorType", columnType="STRING", maximumSize=500),
            Column(name="gender", columnType="STRING", maximumSize=11)
        ]
    )
    return [col.get('id') for col in cols]


def _get_data_files(syn, folder_id):
    """Get `File` entities from given Folder ID and format them as
    `DatasetItem`s.

    Assumptions:
        Immediate children of given Folder ID are File entities.

    Returns:
        list of `DatasetItem`s
    """
    files = syn.getChildren(folder_id)
    return [
        {'entityId': f.get('id'), 'versionNumber': f.get('versionLabel')}
        for f in files
    ]


def _create_dataset(syn, dataset_name, schema, dataset_items):
    """Create `Dataset` entity.

    Assumptions:
        dataset_items contains IDs of File entities only; otherwise,
        SynapseHTTPError is thrown.

    TODO:
        Replace REST call with Dataset service function once available.
    """
    req_body = {
        'name': dataset_name,
        'parentId': PARENT_ID,
        'concreteType': "org.sagebionetworks.repo.model.table.Dataset",
        'columnIds': schema,
        'items': dataset_items
    }
    syn.restPOST("/entity", json.dumps(req_body))


def _reset_datasets(syn):
    """Remove all current datasets."""
    curr_datasets = [i.get('id') for i in syn.restPOST(
        "/entity/children", json.dumps({'parentId': PARENT_ID, "includeTypes": ["dataset"]})).get('page')]
    while curr_datasets:
        for dataset_id in curr_datasets:
            syn.restDELETE(f"/entity/{dataset_id}")
        curr_datasets = [i.get('id') for i in syn.restPOST(
            "/entity/children", json.dumps({'parentId': PARENT_ID, "includeTypes": ["dataset"]})).get('page')]


def main():
    """Main function."""
    syn = login()

    # Keep count of conversions done for summary report.
    folder_count = 0
    file_count = 0
    needs_review = []

    # Convert "datasets" in portal DB project.
    col_ids = create_dataset_schema(syn)
    for folder in syn.getChildren(FOLDERS_ID):
        folder_id = folder.get('id')
        files = _get_data_files(syn, folder_id)
        try:
            _create_dataset(syn,
                            dataset_name=folder.get('name'),
                            schema=col_ids,
                            dataset_items=files)
            folder_count += 1
            file_count += len(files)
        except synapseclient.core.exceptions.SynapseHTTPError:
            needs_review.append(folder_id)

    # Print summary report.
    print(f"      Number of folders converted: {folder_count}")
    print(f"Number of files added to datasets: {file_count}")
    with open('needs_review.txt', 'w') as out:
        for i in needs_review:
            out.write(i + "\n")


if __name__ == "__main__":
    main()
