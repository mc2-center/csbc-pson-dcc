# Preliminaries
import pandas as pd
import argparse
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, build_table, Row, RowSet, as_table_columns

syn = synapseclient.Synapse()
syn.login()

# Get arguments
parser = argparse.ArgumentParser(description='Get synapse project id and filepath')
parser.add_argument("--p", "--project_id", help="Synapse Project ID")
# parser.add_argument("--f", "--filepath", help="path to access file")

args=parser.parse_args()

print(args.project_id)

# get project
project = syn.get(project_id)   

# Create a table from a DataFrame and use only necessary columns
# Insert filepath, desired column names, and desired name of table
df = pd.read_csv('/Users/bzalmanek/Desktop/List_Practice.csv', usecols=['key',
'value','columnType','ontologyId','ontologySource','ontologyUrl'],
index_col=False)

table = build_table('Controlled Vocabulary Master List', project, df)

table = syn.store(table)
