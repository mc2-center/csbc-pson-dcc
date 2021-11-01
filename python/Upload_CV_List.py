# Preliminaries
import pandas as pd
import argparse
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, build_table, Row, RowSet, as_table_columns
from pathlib import Path

def main():

    # Login to  Synapse
    syn = synapseclient.Synapse()
    syn.login()

    # Get Arguments - Synapse project id and file path
    parser = argparse.ArgumentParser(
        description='Get synapse project id, file path, and name of table')
    parser.add_argument('project_id', type=str , help='Synapse Project ID')
    parser.add_argument('path', type=Path, help='Path to file')
    parser.add_argument('table_name', type=str, help='Synapse table name')
    
    args = parser.parse_args()
    
    project_id = args.project_id
    path = args.path
    table_name = args.table_name
    
    # get project in Synapse
    project = syn.get(project_id)   

    # Create a table from a DataFrame and use only necessary columns
    # Insert filepath and table name
    df = pd.read_csv(path, 
        usecols=['key', 'value', 'existing', 'description', 'columnType',
        'ontologyId', 'ontologySource', 'ontologyUrl', 'notes'],
        index_col=False)

    table = build_table(table_name, project, df)

    table = syn.store(table)

    print("Table uploaded to Synapse")

if __name__ == "__main__":
    main()



