import os
import re
import argparse
import json

import pandas as pd
from Bio import Entrez


def getTreeNum(term, tree_memo={}):
    """
    From a MeSH term String, returns a comma separated string with all
    tree numbers associated
    """

    # If we've already seen the term, it should be in the tree_memo dict
    if term in tree_memo:
        return tree_memo[term]

    # If we haven't seen the term, perform an exact search on the MeSH term
    term_id = json.loads(
        "".join(Entrez.esearch(db="MeSH", term=term+"[MeSH Terms]", retmode="json")
                .readlines())).get('esearchresult').get('idlist')

    if term_id is None:
        return None
    s = "".join(Entrez.efetch(db="MeSH", id=term_id).readlines())
    if len(term_id) != 0:
        tmp = re.search(r'Tree Number\(s\): (.*?)\n', s)
        if tmp is not None:
            tree_num = tmp.group(1)
            tree_memo[term] = tree_num
            return tree_num

    # If we haven't found any tree numbers for that exact search (possibly
    # due to special characters), do a non-exact search
    else:
        term_id = json.loads(
            "".join(Entrez.esearch(db="MeSH", term=term, retmode="json")
                    .readlines())).get('esearchresult').get('idlist')
        s = "".join(Entrez.efetch(db="MeSH", id=term_id).readlines())
        if len(term_id) != 0:
            tmp = re.search(r'Tree Number\(s\): (.*?)\n', s)
            if tmp is not None:
                tree_num = tmp.group(1)
                tree_memo[term] = tree_num
                return tree_num

        # If we don't find any associated tree numbers, return a null
        else:
            tree_memo[term] = None
            return None


def getStdNameAndUnkCUIFromMeSHList(mesh_list):
    """
    Returns a tuple of two lists, the first containing known standard
    names from our controlled vocabulary, and the second containing
    CUIs which do not have a mapping in our standard ontology
    """
    if mesh_list == None:
        return ([], [])
    known_std = []
    unk_cui = []
    for mesh in mesh_list:
        cui = mesh_cui_map.get(mesh)
        std = cont_vocab_map.get(cui)
        if std:
            known_std += [std]
        else:
            unk_cui += [cui]
    return (known_std, unk_cui)


def getAllCUIFromMeSHList(mesh_list):
    """
    Returns a list containing CUIs for the MeSHs in mesh_list (provided
    one exists)
    """
    if mesh_list == None:
        return ([], [])
    cuis = []
    for mesh in mesh_list:
        cui = mesh_cui_map.get(mesh)
        cuis += [cui]
    return cuis


def getUniqueDiseaseTerms(head_list, keep_nested=False):
    """
    From a list of MeSH headings, returns those terms which have tree
    numbers starting with C, and which have no tree numbers which are
    substrings of any other tree numbers in the list of headings (if
    keep_nested is False)
    """
    if head_list is None:
        return []

    # List of tree_num, term tuples for each term in the heading list
    # However, this line results in a number of problems. Because of
    # list comprehension, sometimes this results in HTTP Error 429:
    # Too Many Requests (even with the rate limit increased to 10 with an api_key)
    tree_list = [(getTreeNum(term), term) for term in head_list]

    # Filter for terms which have a tree_num list containing at least
    # one tree number starting with C
    diseases = []
    for ind, pair in enumerate(tree_list):
        if pair[0] is not None:
            if any([x.strip()[0] == 'C' for x in pair[0].split(',')]):
                diseases += [pair]
    if keep_nested:
        return [pair[1] for pair in diseases]

    # From this list, return the terms which do not have a tree number
    # which a substring of any other term's tree number
    result = []
    for ind, pair in enumerate(diseases):
        redund = False

        # For each term tuple, called 'pair', we check every other
        # 'alt_pair' to see if any of the listed tree numbers in the
        # first element of pair's first element is a substring of any
        # of the listed tree numbers in alt_pair's first element
        if pair[0] is not None:
            for alt_pair in [x for i, x in enumerate(diseases) if i != ind]:
                if alt_pair[0] is not None:
                    if any([any([sup.strip().startswith(sub.strip()) for sup in alt_pair[0].split(',')]) for sub in pair[0].split(',')]):
                        redund = True
                        break
        if not redund:
            result += [pair[1]]
    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "data_path",
        help=("Location for input file in CSV format with header. Each row "
              "corresponds to a manuscript, whose corresponding PubMed ID "
              "is provided in column, 'input_column'."))
    parser.add_argument(
        "output_path",
        help=("Location for output file, will be a CSV with a header row. It "
              "will be identical to the input CSV, with an added column "
              "containing a String for each row/publication, with the String "
              "containing semicolon-separated (if there is more than one) "
              "disease annotations. The column will be named either 'Disease' "
              "or the value of the -output_column option."))
    parser.add_argument(
        "cont_vocab_path",
        help=("Location for controlled vocabulary CSV file. This has been "
              "derived from the Google Sheet at "
              "https://docs.google.com/spreadsheets/d/1_Ho4tjgdxSHo19qFhlLJhZAyQSPPDW-W8JN910GizDs/edit?usp=sharing, "
              "which can be used as an example. It is required to have a "
              "column 'UMLS_CUI' containing Concept Unique IDs (from the "
              "Unified Medical Language System metathesaurus) and a column "
              "'standard_name' containing a standard term to annotate the "
              "input table with if a publication is mapped to the corresponding "
              "CUI. The rows/standard names do not all need CUIs, but those "
              "terms without CUIs will never be mapped to in this script"))
    parser.add_argument(
        "-i", "-input_column",
        type=str, default="Pubmed",
        help=("Column name containing PubMed IDs (for example, '31442407') "
              "of the query articles.Can also contain links to PubMed articles "
              "(for example, 'https://www.ncbi.nlm.nih.gov/pubmed/31442407', "
              "as the script will extract the PubMed ID from the URL. Will "
              "default to 'PubMed'"))
    parser.add_argument(
        "-o", "-output_column",
        type=str, default="tumorType",
        help=("Column name for the disease annotation. Will be a "
              "semicolon-separated string containing disease annot(s) "
              "for each row/publication. Will default to 'Disease'"))
    parser.add_argument(
        "-m", "-meshtocui_map_path",
        type=str, default="../data/mesh_cui_map_total.csv",
        help=("Relative path of CSV with two columns, one containing "
              "possible MeSH terms in a column called 'STR', and their "
              "corresponding Concept Unique IDs(according to UMLS"
              "metathesaurus) in a column called 'CUI'. Will default to "
              "'../data/mesh_cui_map_total.csv'"))
    parser.add_argument(
        "-c", "-cuitoterms_map_path",
        type=str, default="../data/cuitoterms_map.csv",
        help=("relative path of CSV with (at least) two columns, one "
              "containing Concept Unique IDs(CUIs) we may encounter in a "
              "column called 'CUI', one containing possible terms for our "
              "controlled vocabulary in a columns called 'term'. Any "
              "additional columns or structure can be added include "
              "information which will be output in the case that there are "
              "CUIs which do not have standard names in our controlled "
              "vocabulary. This CSV will be filtered for those CUIs and output "
              "to the location of the option 'needed_cuis'. The default for "
              "'-cuitoterms_map' is '../data/cuitoterms_map.csv', and it "
              "contains columns for 'pref_name', or the most preferred name "
              "for a CUI(according to UMLS), 'edit_score' or the edit score "
              "between a possible term and the pref_name, 'ont_source' which "
              "tells the ontology providing the term to be considered. The "
              "terms have been filtered for those in ICD10CM and NCI thesaurus "
              "ontologies, and ordered by edit score from the most preferred term"))
    parser.add_argument(
        "-n", "-needed_cuis",
        type=str, default="needed_cuis.csv",
        help=("Relative path for possible output, if there are Concept Unique "
              "IDs without a standard name in the controlled vocabulary"))
    parser.add_argument(
        "-u", "-CUI_column", type=str,
        help=("Name of column where all disease CUIs will be listed for each "
              "publication. If not included, the column will not be added to "
              "the dataframe"))

    args = parser.parse_args()
    pub_data = pd.read_csv(args.data_path, sep="\t")

    Entrez.email = os.getenv('ENTREZ_EMAIL')
    Entrez.api_key = os.getenv('ENTREZ_API_KEY')

    cont_vocab_df = pd.read_csv(args.cont_vocab_path)
    cont_vocab_map = dict(
        zip(cont_vocab_df.UMLS_CUI, cont_vocab_df.standard_name))
    del cont_vocab_df

    mesh_cui_df = pd.read_csv(args.m)
    mesh_cui_map = dict(zip(mesh_cui_df.STR, mesh_cui_df.CUI))
    del mesh_cui_df

    cui_pref_terms_df = pd.read_csv(args.c)

    # This reads the input column of the manifest, (splitting on
    # forward slash and query strings to find PubMed IDs in links)
    # and applies the getMeSHHeadingList function to each PubMed ID
    mesh_series = pub_data["mesh"].apply(
        lambda x: x[1:-1].replace("'", "").split(', '))

    # Applies "getUniqueDiseaseTerms" to each PubMed ID's MeSH heading
    # lists, filtering for Disease MeSHs and redundant MeSHs
    disease_series = mesh_series.apply(getUniqueDiseaseTerms)

    # Applies "getStdNameAndUnkCUIFromMeSHList" to each PubMed ID's unique
    # disease terms, retrieving both standard names for concepts we know
    # and CUIs for concepts we do not have in our controlled vocabulary
    translated = disease_series.apply(
        getStdNameAndUnkCUIFromMeSHList).apply(pd.Series)
    known_std = translated[0]
    unk_cui = set(translated[1].sum())
    if len(unk_cui) > 0:
        print("There are MeSH terms corresponding to Concept Unique IDs "
              "(CUIs) which do not have defined standard names in your "
              "controlled vocabulary. They are the following (with a possible "
              "term for that CUI aside): ")
        pd.set_option('display.max_rows', None)
        print(cui_pref_terms_df[cui_pref_terms_df['CUI'].apply(
            lambda x: x in unk_cui)][['CUI', 'term']].drop_duplicates(
                subset='CUI').to_string(index=False))
        pd.set_option('display.max_rows', 10)

        print("You may re-run the script after adding those to the controlled "
              f"vocabulary at {args.cont_vocab_path}")
        cui_pref_terms_df[cui_pref_terms_df['CUI'].apply(
            lambda x: x in unk_cui)].to_csv(args.n, index=False)
        print("Writing the table of possible terms to " + args.n)

    # Add a column with known disease standard names in a string
    # separated by semicolons. If the annotations contain 'Not Specified'
    # along with any other disease, remove all 'Not Specified' annotations
    pub_data[args.o] = known_std.apply(lambda x: ";".join(
        [anno for anno in set(x) if not (len(set(x)) > 1 and
                                         anno == 'Not Specified')]))

    if args.u:
        pub_data[args.u] = disease_series.apply(
            getAllCUIFromMeSHList).apply(lambda x: ";".join(x))

    print(f"Adding known disease annotations and saving at {args.output_path}")
    pub_data.to_csv(args.output_path, index=False)
