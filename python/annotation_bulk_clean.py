# Preliminaries
import synapseclient
from synapseclient import Table, Schema
import argparse

# login to synapse
syn = synapseclient.login(silent=True)

# Get arguments for table id to be changed and column to focus on
parser = argparse.ArgumentParser(
    description='Get synapse table ids, column, old annotation, and new annotation')
parser.add_argument('annotation_table_id', type=str , help='Synapse Table id for the annotations to be changed')
parser.add_argument('cv_table_id', type=str , help='Synapse Table id for controlled vocabulary to be referenced')
parser.add_argument('column', type=str, help='Name of column in annotation table to reference, i.e. "assay"')

args = parser.parse_args()

annotation_table_id = args.annotation_table_id
cv_table_id=args.cv_table_id
column = args.column

# Create query for Controlled Vocabulary table using column specified
cv_query = (f"SELECT key, value, existing FROM {cv_table_id} "
             f"WHERE ((key = ('{column}')))")

# Query the CV table using tableQuery() and convert the results into a dataframe.
cv_view = syn.tableQuery(cv_query).asDataFrame()

# Iterate through publications using query
annotations_query = (f"SELECT publicationId, pubMedId, {column} FROM {annotation_table_id}")

# Query the publications table using tableQuery() and convert the results into a dataframe
annotations_view = syn.tableQuery(annotations_query).asDataFrame()

# Create empty list, then iterate though each type of value
a_list = []
for i, row in annotations_view.iterrows():
    a_list = row[column]
    # Iterate through each item in list
    for term in a_list:
        if term in cv_view['existing'].values:
            # Does not work:
            a_list[term] = cv_view['value']
    print(a_list)
            

        
        









