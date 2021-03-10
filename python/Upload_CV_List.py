# Preliminaries
import pandas as pd
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, build_table, Row, RowSet, as_table_columns

syn = synapseclient.Synapse()
syn.login()

# Get project
project = syn.get('syn1234')

# Create a table from a DataFrame and use only necessary columns
# Insert filepath, desired column names, and desired name of table
df = pd.read_csv("filepath", usecols=['key','value','columnType','ontologyId','ontologySource','ontologyUrl'], index_col=False)
table = build_table('Controlled Vocabulary Master List', project, df)
table = syn.store(table)

