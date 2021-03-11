# Preliminaries
import pandas as pd
import argparse
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, build_table, Row, RowSet, as_table_columns
from pathlib import Path

# Login to  Synapse
syn = synapseclient.Synapse()
syn.login()

# Get Arguments - Synapse project id and file path
parser = argparse.ArgumentParser(description = 'Get synapse project id')
parser.add_argument('project_id',type=str,help='Synapse Project ID')
parser.add_argument('path',type=Path,help='Path to file')
    
args = parser.parse_args()
    
project_id = args.project_id
path = args.path
    
# get project in Synapse
project = syn.get(project_id)   

# Create a table from a DataFrame and use only necessary columns
# Insert filepath, desired column names, and desired name of table
df = pd.read_csv(path, 
    usecols=['key','value','columnType',
    'ontologyId','ontologySource','ontologyUrl'],
    index_col=False)

table = build_table('Controlled Vocabulary Master List', project, df)

table = syn.store(table)

print("Table uploaded to Synapse")



