import synapseclient
from synapseclient import Table, Schema
import argparse

# A script to compare distinct terms in annotations table to distinct terms in controlled vocabulary table.
def main():

   # login to synapse
    syn = synapseclient.login(silent=True)

    # Get arguments for CV table id, annotations table id, and column name
    parser = argparse.ArgumentParser(
    description='Get synapse table ids and column')
    parser.add_argument('annotation_table_id', type=str , help='Synapse Table id the annotations are stored in')
    parser.add_argument('cv_table_id', type=str , help='Synapse Table id for controlled vocabulary to be referenced')
    parser.add_argument('column', type=str, help='Name of column in annotation table to reference, i.e. "assay"')

    args = parser.parse_args()

    annotation_table_id = args.annotation_table_id
    cv_table_id=args.cv_table_id
    column = args.column

    ### Get distinct list of terms from the annotations table
    # query to get list of terms from annotations table
    annotations_query = (f"SELECT {column} FROM {annotation_table_id}")
    # Create a data frame 
    df_annotations = syn.tableQuery(annotations_query).asDataFrame()
    # Pandas explode() to separate assay list elements into separate rows
    term_explode = df_annotations.explode(column)
    # Drop duplicates
    annotation_term_list = term_explode.drop_duplicates()
    print(f"Number of distinct annotation terms: {len(annotation_term_list.index)}")

    ### Get distinct list of terms from the controlled vocabulary table
    # query for list of terms from CV table
    cv_query = (f"SELECT value FROM {cv_table_id} WHERE key = '{column}'")
    # Create a data frame from query
    df_cv = syn.tableQuery(cv_query).asDataFrame()
    # Drop duplicate terms
    cv_term_list = df_cv.drop_duplicates()
    print(f"Number of distinct controlled vocabulary terms: {len(cv_term_list)}")

    ### Compare annotation table list to CV table list to find missing terms
    # Iterate through each row in annotation_term_list
    update_terms = []
    counter = 0
    for i, row in annotation_term_list.iterrows():
        term = row[column]
        if term in cv_term_list['value'].values:
            print(f"{term} is okay")
        else:
            update_terms.append(term)
            counter += 1
            print(f"{term} is not in CV")
    print(f"There are {counter} terms in the annotations table that are not in the CV list, and they are {update_terms}")
       
### Compare CV list terms to annotation table list terms to find if there are any missing terms
# Iterate through each row in cv_term_list and compare
    missing_terms = []
    cv_counter = 0
    for i, row in cv_term_list.iterrows():
        cv_term = row['value']
        if cv_term in annotation_term_list.values:
            print(f"{cv_term} is okay")
        else:
            missing_terms.append(cv_term)
            cv_counter += 1 
            print(f"{cv_term} is not in the annotations table")
    print(f"There are {cv_counter} terms in the CV table that are not in the annotations table, and they are{missing_terms}")



if __name__ == "__main__":
    main()