# Preliminaries
import pandas as pd
import argparse
import os
import glob
from grant_dicts import CONSORTIUM, THEME


### Get arguments ###
def get_args():

    parser = argparse.ArgumentParser(
        description='Get file path of manifest csv')
    parser.add_argument('directory_path',
                        type=str,
                        help='Path to directory that houses the manifest csvs')

    return parser.parse_args()


### Get list of csv files ###
def get_files(directory):

    files = glob.glob(f'{directory}**/**.csv')

    return (files)


def split_manifests(files, directory):

    data_types = ['Publication', 'Dataset', 'File', 'Tool']
    for item in data_types:
        # Create directories
        os.mkdir(f'{directory}/{item}sSplit')
        for file in files:
            if item in file:
                df = pd.read_csv(file, index_col=0, keep_default_na=False)
                grant_col = f'{item} Grant Number'
                consortium_col = f'{item} Consortium Name'
                theme_col = f'{item} Theme Name'
                # Change column grant type to list
                df[grant_col] = df[grant_col].apply(lambda x: x.split(', '))
                # Separate out rows with multiple grants
                df = df.explode(grant_col)
                # Make consortium and themes match grant
                df[consortium_col] = df[grant_col].map(CONSORTIUM)
                df[theme_col] = df[grant_col].map(THEME)
                # Split into multiple manifests
                grouped = df.groupby([grant_col])
                print(f"Found {len(grouped.groups)} grant numbers in table "
                      "- splitting now...")
                # Save dataframes as csvs
                for grant_number in grouped.groups:
                    df = grouped.get_group(grant_number)
                    df.to_csv(f'{directory}/{item}sSplit/{grant_number}.csv',
                              index=False)


def main():

    args = get_args()
    file_list = get_files(args.directory_path)

    split_manifests(file_list, args.directory_path)

    print("Done. Manifests split by grant number.")


if __name__ == "__main__":
    main()