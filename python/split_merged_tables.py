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
    parser.add_argument("type", type=str,
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


def extract_url_columns(table):
    """Extract just the URLs from columns that contain links in markdown.

    Assumptions:
        This only applies to the "Dataset Url" column
    """
    if "Dataset Url" in table.columns:
        table["Dataset Url"] = (
            table["Dataset Url"]
            .str.extractall(r'\((.*?)\)')
            .unstack()
            .fillna("")
            .apply(", ".join, axis=1)
            .str.rstrip(", ")
        )
    return table


def reformat_table(table, colname):
    """Reformat values in the table."""
    table = extract_url_columns(table)

    # Find columns in tables that are lists (other than Grant Number) and
    # convert them to strings.
    list_cols = (
        table.columns[table.applymap(type).eq(list).any()]
        .drop(colname)
    )
    table[list_cols] = table[list_cols].applymap(lambda lst: ", ".join(lst))
    return table


def split_table(table, parent):
    """Split table by grant number and output to CSV."""

    # Column name for grant numbers depend on the manifest template type.
    colname = f"{parent.capitalize()} Grant Number"

    # Before splitting table, reformat and clean up the table first.
    table = reformat_table(table, colname)

    # Some rows may have multiple grants, so "explode" them up into separate
    # rows so that each row is only associated with one grant number. All
    # other column values will remain the same.
    grouped = table.explode(colname).groupby(colname)
    print(f"Found {len(grouped.groups)} grant numbers in table "
          "- splitting now...")

    # Iterate through each grant number group, filtering out rows with any
    # columns that have 500+ characters -- these will need to be further QC'd.
    for grant_number in grouped.groups:
        df = grouped.get_group(grant_number)

        # Only create files if information is found.
        valid_rows = df[~df.applymap(lambda x: len(str(x)) > 500).any(axis=1)]
        if not valid_rows.empty:
            if not os.path.isdir(os.path.join("ready", grant_number)):
                os.makedirs(os.path.join("ready", grant_number))
            valid_filepath = os.path.join(
                "ready", grant_number, parent + "_manifest.csv")
            valid_rows.to_csv(valid_filepath, index=False)

        invalid_rows = df[df.applymap(lambda x: len(str(x)) >= 500).any(axis=1)]
        if not invalid_rows.empty:
            if not os.path.isdir(os.path.join("not_ready", grant_number)):
                os.makedirs(os.path.join("not_ready", grant_number))
            invalid_filepath = os.path.join(
                "not_ready", grant_number, parent + "_manifest.csv")
            invalid_rows.to_csv(invalid_filepath, index=False)


def main():
    """Main function."""
    syn = login()
    args = get_args()

    table = get_table(syn, args)
    split_table(table, args.type)

    print("DONE ✓")


if __name__ == "__main__":
    main()
