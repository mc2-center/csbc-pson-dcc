# Preliminaries
import pandas as pd
import argparse
import glob
from grant_dicts import CONSORTIUM, THEME


### Get arguments ###
def get_args():

    parser = argparse.ArgumentParser(
        description='Get directory path with files')
    parser.add_argument('directory_path',
                        type=str,
                        help='Path to directory that houses manifests')

    return parser.parse_args()


# List all files paths to the csvs in the directory
def get_file_paths(directory):

    files = glob.glob(f'{directory}**/**/*.csv', recursive=True)

    return (files)


### Function to edit manifest if more than one values is listed for consortium, theme, and institution
def edit_csv(manifest_csv):

    # Convert to data frame, get rid of index column and read empty values as empty strings
    df = pd.read_csv(manifest_csv, index_col=0, keep_default_na=False)

    # Check file name for publication, dataset, tool, or project and iterate accordingly
    if 'publication' in manifest_csv:
        # replace consortium values with multiple consortia listed.
        for value in df['Publication Consortium Name'].values:
            if ',' in value:
                grant_number = df[df['Publication Consortium Name'] ==
                                  value]['Publication Grant Number'].values[0]
                consortium = CONSORTIUM.get(grant_number)
                # replace consortium
                df['Publication Consortium Name'] = df[
                    'Publication Consortium Name'].replace([value], consortium)
                # replace theme - errors in theme despite having mulitple consortia, so replacing all themes
                theme = str(THEME.get(grant_number))
                theme = theme.strip('["').strip('"]').replace('"', "")
                df['Publication Theme Name'] = theme
        df.to_csv(manifest_csv)

    elif 'dataset' in manifest_csv:
        # replace consortium values with multiple consortia listed.
        for value in df['Dataset Consortium Name'].values:
            if ',' in value:
                grant_number = df[df['Dataset Consortium Name'] ==
                                  value]['Dataset Grant Number'].values[0]
                consortium = CONSORTIUM.get(grant_number)
                df['Dataset Consortium Name'] = df[
                    'Dataset Consortium Name'].replace([value], consortium)
                # replace theme - errors in theme despite having mulitple consortia, so replacing all themes
                theme = str(THEME.get(grant_number))
                theme = theme.strip('["').strip('"]').replace('"', "")
                df['Dataset Theme Name'] = theme
        df.to_csv(manifest_csv)

    elif 'tool' in manifest_csv:
        # replace consortium values with multiple consortia listed.
        for value in df['Tool Consortium Name'].values:
            if ',' in value:
                grant_number = df[df['Tool Consortium Name'] ==
                                  value]['Tool Grant Number'].values[0]
                consortium = CONSORTIUM.get(grant_number)
                df['Tool Consortium Name'] = df[
                    'Tool Consortium Name'].replace([value], consortium)
        df.to_csv(manifest_csv)


# Function to iterate through files list and edit manifests
def manifest_iterate(files_list):

    for file in files_list:
        edit_csv(file)


def main():

    # syn = login()
    args = get_args()
    list_of_files = get_file_paths(args.directory_path)

    manifest_iterate(list_of_files)


if __name__ == "__main__":
    main()
