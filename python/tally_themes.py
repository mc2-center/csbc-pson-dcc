"""Tally Themes in Portal Tables

This script will get a count of themes across the grants and
consortiums in the CCKP. (Based on James' `nbs/portal_summary.Rmd`)

author: verena.chung
"""
import os

import synapseclient
import pandas as pd

# Source table IDs
GRANTS = "syn21918972"
THEMES = "syn35369899"
PUBS = "syn21868591"
DATASETS = "syn21897968"
TOOLS = "syn26127427"

# Destination table IDs
CONSORTIUM_CTS = "syn21641485"
CON_THEME_CTS = "syn21649281"
THEME_CTS = "syn21639584"

def tally_by_consortium(grants):
    """Portal - Consortium Counts (syn21641485)"""
    return (
        grants[['grantId', 'consortium']]
        .groupby('consortium')
        .count()
        .rename(columns={'grantId': "totalCount"})
        .assign(groupBy="grants")
        .reset_index()
        .reindex(columns=['consortium', 'groupBy', 'totalCount'])
    )


def tally_by_theme_consortium(grants, themes):
    """Portal - Consortium-Theme Counts (syn21649281)"""
    res = (
        grants[['grantId', 'consortium', 'theme']]
        .explode('theme')
        .groupby(['theme', 'consortium'])
        .count()
        .rename(columns={'grantId': "totalCount"})
        .assign(groupBy="grants")
        .join(themes)
        .fillna("")
        .reset_index()
        .reindex(columns=['theme', 'themeDescription', 'consortium',
                          'groupBy', 'totalCount'])
    )
    return res[~res['theme'].isin(['Computational Resource'])]


def tally_by_group(syn, grants, themes):
    """Portal - Theme Counts (syn21639584)"""

    # get theme counts in publications
    publications = syn.tableQuery(
        f"SELECT pubMedId, theme FROM {PUBS}").asDataFrame()
    theme_pubs = (
        publications
        .explode('theme')
        .groupby('theme')
        .count()
        .rename(columns={'pubMedId': "totalCount"})
        .assign(groupBy="publications")
    )

    # get theme counts in datasets
    datasets = syn.tableQuery(
        f"SELECT pubMedId, theme FROM {DATASETS}").asDataFrame()
    theme_datasets = (
        datasets
        .explode('theme')
        .groupby('theme')
        .count()
        .rename(columns={'pubMedId': "totalCount"})
        .assign(groupBy="datasets")
    )

    # get theme counts in tools
    tools = syn.tableQuery(
        f"SELECT toolName, grantNumber FROM {TOOLS}").asDataFrame()
    theme_tools = (
        tools
        .explode('grantNumber')
        .set_index('grantNumber')
        .join(grants[['grantNumber', 'theme']].set_index('grantNumber'))
        .explode('theme')
        .groupby('theme')
        .count()
        .rename(columns={'toolName': "totalCount"})
        .assign(groupBy="tools")
    )

    # concat results together
    res = (
        pd.concat([theme_pubs, theme_datasets, theme_tools])
        .join(themes)
        .reset_index()
        .rename(columns={'index': "theme"})
        .sort_values(['groupBy', 'theme'])
        .reindex(columns=['theme', 'themeDescription',
                          'groupBy', 'totalCount'])
    )
    return res[~res['theme'].isin(['Computational Resource'])]


def update_table(syn, table_id, updated_table):
    """Truncate table then add new rows."""
    current_rows = syn.tableQuery(f"SELECT * FROM {table_id}")
    syn.delete(current_rows)
    updated_table.to_csv("rows.csv")
    updated_rows = synapseclient.Table(table_id, "rows.csv")
    syn.store(updated_rows)
    os.remove("rows.csv")


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)

    # Table of theme names and their descriptions.
    themes = (syn.tableQuery(
        f"SELECT displayName, description FROM {THEMES}")
        .asDataFrame()
        .rename(columns={'displayName': 'theme', 'description': 'themeDescription'})
        .set_index('theme'))
    grants = (syn.tableQuery(
        f"SELECT grantId, grantNumber, consortium, theme FROM {GRANTS}")
        .asDataFrame())

    consortium_counts = tally_by_consortium(grants)
    theme_consortium_counts = tally_by_theme_consortium(grants, themes)
    theme_counts = tally_by_group(syn, grants, themes)

    update_table(syn, CONSORTIUM_CTS, consortium_counts)
    update_table(syn, CON_THEME_CTS, theme_consortium_counts)
    update_table(syn, THEME_CTS, theme_counts)


if __name__ == "__main__":
    main()
