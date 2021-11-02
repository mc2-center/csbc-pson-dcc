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

# Create variable for old annotation and new annotation based on "existing" column values of controlled vocabulary data frame
for i, row in cv_view.iterrows():
    old_annotation = row['existing']
    new_annotation = row['value']
    # Create query for annotations table specified in agrument
    annotations_query = (f"SELECT publicationId, pubMedId, {column} FROM {annotation_table_id} "
             f"WHERE (({column} HAS ('{old_annotation}')))")
    # query the table with tableQuery() and convert the results into a data frame.
    annotations_view = syn.tableQuery(annotations_query).asDataFrame()
    # Change old annotation to new annotation
    for i, row in annotations_view.iterrows():
        annots = row[column]
        new_annots = [new_annotation if x ==
                      old_annotation else x for x in annots]
        annotations_view.at[i, column] = new_annots
    print(annotations_view)






