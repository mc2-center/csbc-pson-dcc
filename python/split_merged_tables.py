"""Split Merged Tables by Grant Number

This script will split a Merged table by grant number and output
results into individual CSVs, ready for upload into schematic/DCA.

author: verena.chung
"""

import os
import argparse
import getpass

import synapseclient
from data_model import TABLE_ID, COLNAMES, DROP, ADD


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
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--type",
                        type=str, required=True,
                        choices=["publication", "dataset",
                                 "tool", "project"],
                        help="Table to split, e.g. `publication`")
    return parser.parse_args()


def get_table(syn, args):
    """Get Merged table and modify the columns to match the data model."""
    table = (
        syn.tableQuery(f"SELECT * FROM {TABLE_ID.get(args.type)}")
        .asDataFrame()
        .fillna("")
        .rename(columns=COLNAMES.get(args.type))
        .drop(columns=DROP.get(args.type))
    )
    for new in ADD.get(args.type):
        table.insert(new['index'], new['colname'], new['value'])
    return table


def split_table(table, parent):
    """Split table by grant number and output to CSV."""
    grouped = table.explode('grantNumber').groupby('grantNumber')
    print(f"Found {len(grouped.groups)} grant numbers in table "
          "- splitting now...")
    os.makedirs(f"{parent}")
    for grant in grouped.groups:
        filepath = os.path.join(parent, grant + ".csv")
        grouped.get_group(grant).to_csv(filepath, index=False)


def main():
    """Main function."""
    syn = login()
    args = get_args()

    table = get_table(syn, args)
    split_table(table, args.type)

    print("DONE ✓")


if __name__ == "__main__":
    main()
