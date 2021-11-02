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

annotation_table_id = args.table_id
cv_table_id=args.cv_table_id
column = args.column

# Create query for Controlled Vocabulary table using column specified
cv_query = (f"SELECT key, value, existing FROM {cv_table_id} "
             f"WHERE ((key HAS ('{column}')))")

# Query the CV table using tableQuery() and convert the results into a dataframe.
cv_view = syn.tableQuery(cv_query).asDataFrame()
print(cv_view)






