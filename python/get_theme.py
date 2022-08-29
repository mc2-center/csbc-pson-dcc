import synapseclient
import pandas as pd
import argparse

# Script to fill Theme Name for Publication and Dataset manifests (with all grant nubmers combined)


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
    parser.add_argument(
        'file',
        type=str,
        help='File path to manifest to add theme information to')

    return parser.parse_args()


### Retrieve grants merged table and turn into data frame ###
def get_themes(syn, table):

    grants_query = (f"SELECT grantNumber, theme FROM {table}")
    grants_df = syn.tableQuery(grants_query).asDataFrame()

    themes = dict(zip(grants_df.grantNumber, grants_df.theme))

    # Make themes strings instead of lists
    for key, value in themes.items():
        value = str(value)
        value = value.strip('["').strip('"]').replace("'", "")
        themes.update({key: value})

    return themes


def get_consortiums(syn, table):

    grants_query = (f"SELECT grantNumber, consortium FROM {table}")
    grants_df = syn.tableQuery(grants_query).asDataFrame()

    consortiums = dict(zip(grants_df.grantNumber, grants_df.consortium))

    return (consortiums)


def edit_manifest(theme_dict, consortium_dict, manifest_path):

    datatypes = ['Publication', 'Dataset']

    df = pd.read_csv(manifest_path)

    for item in datatypes:
        if item in manifest_path:
            grant_col = f'{item} Grant Number'
            theme_col = f'{item} Theme Name'
            consortium_col = f'{item} Consortium Name'
            df[theme_col].fillna('', inplace=True)
            df[theme_col] = df[grant_col].map(theme_dict)
            df[consortium_col] = df[grant_col].map(consortium_dict)

            # Map theme name for rows with multiple grants listed
            for i, r in df.iterrows():
                if "," in r[grant_col]:
                    r[grant_col] = r[grant_col].split(", ")
                    theme_list = []
                    consortium_list = []
                    for item in r[grant_col]:
                        theme_list.append(theme_dict[item])
                        df.loc[i, theme_col] = ", ".join(theme_list)
                        df.loc[i, grant_col] = ", ".join(r[grant_col])
                        consortium_list.append(consortium_dict[item])
                        df.loc[i, consortium_col] = ", ".join(consortium_list)
                        df.loc[i, grant_col] = ", ".join(r[grant_col])

    return df


def save_csv(df, file):

    df.to_csv(file, index=False)


def main():

    syn = login()
    args = get_args()
    themes = get_themes(syn, args.table_id)
    consortiums = get_consortiums(syn, args.table_id)
    df = edit_manifest(themes, consortiums, args.file)

    save_csv(df, args.file)


if __name__ == "__main__":
    main()
