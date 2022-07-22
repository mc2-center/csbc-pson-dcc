import synapseclient
import argparse
import pandas as pd


### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


### Get arguments ###
def get_args():

    parser = argparse.ArgumentParser(description='Get synapse grants table id')
    parser.add_argument('table_id',
                        type=str,
                        help='Synapse grants merged table id')

    return parser.parse_args()


### Retrieve grants merged table and turn into data frame ###
def get_grant_table(syn, table):

    grants_query = (
        f"SELECT grantNumber, theme, consortium, grantInstitution FROM {table}"
    )
    grants_df = syn.tableQuery(grants_query).asDataFrame()

    return grants_df


def grant_dictionary(grants_df):

    consortium_dict = dict(zip(grants_df.grantNumber, grants_df.consortium))
    theme_dict = dict(zip(grants_df.grantNumber, grants_df.theme))
    institution_dict = dict(
        zip(grants_df.grantNumber, grants_df.grantInstitution))

    # Make themes strings instead of lists
    for key, value in theme_dict.items():
        value = str(value)
        value = value.strip('["').strip('"]').replace("'", "")
        theme_dict.update({key: value})

    print(consortium_dict)
    print(theme_dict)
    print(institution_dict)


def main():

    syn = login()
    args = get_args()
    grants_df = get_grant_table(syn, args.table_id)

    grant_dictionary(grants_df)


if __name__ == "__main__":
    main()