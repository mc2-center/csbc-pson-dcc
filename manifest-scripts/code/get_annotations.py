import pandas as pd
from Bio import Entrez
import json
import argparse
import sys
import re

def getMeSHHeadingList(pm):
    # From  PubMed ID (not link), returns a (possibly empty) list of MeSH headings associated

    # Retrieves an XML "handle" from NCBI's Entrez Utilities
    handle = Entrez.efetch(db="PubMed", id=pm, retmode='xml', api_key=api_key)

    # Parses the XMl handle into python objects (a nested dictionary/list structure)
    record = Entrez.read(handle)

    # This only occurs when the article no longer exists in Pubmed, but I ran into it once!
    if len(record['PubmedArticle']) == 0:
        return None

    # Traversing through the dictionarys/lists (want to handle 'None's along the way) to get to the list of headings
    MeSH_list = record['PubmedArticle'][0]
    for key in ['MedlineCitation', 'MeshHeadingList']:
        MeSH_list = MeSH_list.get(key)
        if MeSH_list is None:
            return None
    MeSHs = []

    # Only access the DescriptorName from the headings, not the  MeSH qualifiers
    for heading in MeSH_list:
        desc = str(heading['DescriptorName'])
        MeSHs += [desc]
    return MeSHs

def getTreeNum(term, tree_memo={}):
    # From a MeSH term String, returns a comma separated string with all tree numbers associated

    # If we've already seen the term, it should be in the tree_memo dictionary
    if term in tree_memo:
        return tree_memo[term]

    # If we haven't seen the term, perform an exact search on the MeSH term
    term_id = json.loads("".join(Entrez.esearch(db="MeSH", term=term+"[MeSH Terms]", retmode="json", api_key=api_key)\
        .readlines())).get('esearchresult').get('idlist')

    if term_id is None:
        return None
    s = "".join(Entrez.efetch(db="MeSH", id=term_id, api_key=api_key).readlines())
    if len(term_id) != 0:
        tree_num = re.search(r'Tree Number\(s\): (.*?)\n', s).group(1)
        tree_memo[term] = tree_num
        return tree_num

    # If we haven't found any tree numbers for that exact search (possibly due to special characters), do a non-exact search
    else:
        term_id = json.loads("".join(Entrez.esearch(db="MeSH", term=term, retmode="json", api_key=api_key)\
            .readlines())).get('esearchresult').get('idlist')
        s = "".join(Entrez.efetch(db="MeSH", id=term_id, api_key=api_key).readlines())
        if len(term_id) != 0:
            tree_num = re.search(r'Tree Number\(s\): (.*?)\n', s).group(1)
            tree_memo[term] = tree_num
            return tree_num

        # If we don't find any associated tree numbers, return a null
        else:
            tree_memo[term] = None
            return None

def getStdNameAndUnkCUIFromMeSHList(mesh_list):
    # Returns a tuple of two lists, the first containing known standard names from our controlled vocabulary, and the second containing CUIs which do not have a mapping in our standard ontology
    if mesh_list == None:
        return ([],[])
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
    # Returns a list containing CUIs for the MeSHs in mesh_list (provided one exists)
    if mesh_list == None:
        return ([],[])
    cuis = []
    for mesh in mesh_list:
        cui = mesh_cui_map.get(mesh)
        cuis += [cui]
    return cuis

def getUniqueRelevantTerms(head_list, keep_nested = False):
    # From a list of MeSH headings, returns those terms which have tree numbers starting with C, and which have no tree numbers which are substrings of any other tree numbers in the list of headings (if keep_nested is False)
    if head_list is None:
        return []

    # List of tree_num, term tuples for each term in the heading list
    # However, this line results in a number of problems. Because of list comprehension,
    # sometimes this results in HTTP Error 429: Too Many Requests (even with the rate limit increased to 10 with an api_key)
    tree_list = [(getTreeNum(term), term) for term in head_list]

    # Filter for terms which have a tree_num list containing at least one tree number starting with C
    relevant_meshs = []
    for ind, pair in enumerate(tree_list):
        if any([x.strip().startswith(prefix) for x in pair[0].split(',')]):
            relevant_meshs += [pair]
    if keep_nested:
        return [pair[1] for pair in relevant_meshs]

    # From this list, return the terms which do not have a tree number which a substring of any other term's tree number
    result = []
    for ind, pair in enumerate(relevant_meshs):
        redund = False

        # For each term tuple, called 'pair', we check every other 'alt_pair' to see if any of the listed tree numbers in the first element of pair's first element is a substring of any of the listed tree numbers in alt_pair's first element
        for alt_pair in [x for i, x in enumerate(relevant_meshs) if i != ind]:
            if any([any([sup.strip().startswith(sub.strip()) for sup in alt_pair[0].split(',')]) for sub in pair[0].split(',')]):
                redund = True
                break
        if not redund:
            result += [pair[1]]
    return result

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("data_path", help="location for input file in CSV format with header. Each row corresponds to a manuscript, whose corresponding PubMed ID is provided in column 'input_column'.")
    parser.add_argument("output_path", help="location for output file, will be a CSV with a header row. It will be \
        identical to the input CSV, with an added column containing a String for each row/publication, with the String \
        containing semicolon-separated (if there is more than one) relevant annotations. The column will be named either \
        'Disease' or the value of the -output_column option.")
    parser.add_argument("cont_vocab_path", help="location for controlled vocabulary CSV file. This has been derived from the Google Sheet at \
        https://docs.google.com/spreadsheets/d/1_Ho4tjgdxSHo19qFhlLJhZAyQSPPDW-W8JN910GizDs/edit?usp=sharing, which \
        can be used as an example. It is required to have \
        a column 'UMLS_CUI' containing Concept Unique IDs (from the Unified Medical Language System metathesaurus) \
        and a column 'standard_name' containing a standard term to annotate the input table with if a publication \
        is mapped to the corresponding CUI. The rows/standard names do not all need CUIs, but those terms without CUIs will never be mapped to in this script")
    parser.add_argument("-i", "-input_column", type=str, help="column name containing PubMed IDs (for example, '31442407') of the query articles. \
        Can also contain links to PubMed articles (for example, 'https://www.ncbi.nlm.nih.gov/pubmed/31442407', as the script will extract the PubMed ID from the URL. Will default to 'PubMed'")
    parser.add_argument("-o", "-output_column", type=str, help="column name for the annotation. Will be a \
        semicolon-separated string containing annotation(s) for each row/publication. Will default to 'Disease'")
    parser.add_argument("-m", "-meshtocui_map_path", type=str, help="relative path of CSV with two columns, one containing \
        possible MeSH terms in a column called 'STR', and their corresponding Concept Unique IDs (according to UMLS \
        metathesaurus) in a column called 'CUI'. Will default to '../data/mesh_cui_map_total.csv'")
    parser.add_argument("-c", "-cuitoterms_map_path", type=str, help="relative path of CSV with (at least) two columns, \
        one containing Concept Unique IDs (CUIs) we may encounter in a column called 'CUI', one containing possible \
        terms for our controlled vocabulary in a columns called 'term'. Any additional columns or structure can be \
        added include information  which will be output in the case that there are CUIs which do not have standard \
        names in our controlled  vocabulary. This CSV will be filtered for those CUIs and output to the location of \
        the option 'needed_cuis'. The default for '-cuitoterms_map' is '../data/cuitoterms_map.csv', and it contains \
        columns for 'pref_name', or the most preferred name for a CUI (according to UMLS), 'edit_score' or the edit \
        score between a possible term and the pref_name, 'ont_source' which tells the ontology providing the term to \
        be considered. The terms have been filtered for those in ICD10CM and NCI thesaurus ontologies, and ordered by\
        edit score from the most preferred term")
    parser.add_argument("-n", "-needed_cuis", type=str, help="relative path for possible output, if there are Concept \
        Unique IDs without a standard name in the controlled vocabulary")
    parser.add_argument("-a", "-api_key", type=str, help="API key for Entrez. Increases the rate limit from 3 requests \
        per second of the E-utilities (used to retrieve PubMed data) to 10 requests per second. Users can obtain an API \
        key from the settings page of an NCBI account at https://www.ncbi.nlm.nih.gov/account/settings/")
    parser.add_argument("-e", "-email", type=str, help="Email of individual running script, to be passed to Entrez in \
        order to access E-utilities. Will be  used if NCBI observes requests that violate their policies. Will give \
        a warning if omitted, and an IP address can be blocked by NCBI if a violating request is made without an email \
        address included")
    parser.add_argument("-p", "-mesh_type", type=str, help="Type of MeSH term to consider, or prefix of MeSH tree numbers to consider.\
        This can be one of 'disease' for Diseases MeSHs (prefix 'C') or 'exp_strat' for Investigative Technique MeSHs \
        (prefix 'E05'), or it can be any prefix in for a MeSH tree number one would like to filter the MeSH terms for. \
        These can be viewed in the MeSH Browser Tree View at https://meshb.nlm.nih.gov/treeView by clicking the (+) next \
        to sections in the tree. This will default to 'disease', with a prefix of 'C'.")
    parser.add_argument("-u", "-CUI_column", type=str, help="Name of column where all relevant CUIs will be listed for each publication. If not included, the column will not be added to the dataframe")
    #parser.add_argument("-k", "-keep_nested", type=bool, help="Whether or not to keep less specific nested mesh terms \
    #   (i.e. one being a more specific term for another, such as 'Cancer' and 'Breast Cancer'. Default is to remove")
    args = parser.parse_args()
    pub_data = pd.read_csv(args.data_path)
    if not args.i:
        args.i = "PubMed"
    if not args.o:
        args.o = "Disease"
    if not args.m:
        args.m = "../data/mesh_cui_map_total.csv"
    if not args.c:
        args.c = "../data/cuitoterms_map.csv"
    if not args.n:
        args.n = "needed_cuis.csv"
    if args.e:
        Entrez.email = args.e
    if args.e:
        api_key = args.a
    else:
        api_key = None
    #Creating a dictionary defining mappings between inputs for -p and prefixes
    prefix = {'disease':'C', 'exp_strat':'E05'}.get(args.p)
    # If the argument isn't found in the dicitonary, try using the argument as the prefix (if they enter a prefix rather than a category)
    # if this doesn't work, default to prefix 'C' for diseases.
    if not prefix:
        prefix = args.p
        if not prefix:
            prefix = 'C'

    cont_vocab_df = pd.read_csv(args.cont_vocab_path)
    cont_vocab_map = dict(zip(cont_vocab_df.UMLS_CUI, cont_vocab_df.standard_name))
    del cont_vocab_df

    mesh_cui_df = pd.read_csv(args.m)
    mesh_cui_map = dict(zip(mesh_cui_df.STR, mesh_cui_df.CUI))
    del mesh_cui_df

    cui_pref_terms_df = pd.read_csv(args.c)

    # This reads the input column of the manifest, (splitting on forward slash and query strings to find PubMed IDs in links) and applies the getMeSHHeadingList function to each PubMed ID
    mesh_series = pub_data[args.i].apply(lambda x: [field for field in x.split("/") if field][-1].split("?term=")[-1])\
        .apply(getMeSHHeadingList)

    # Applies "getUniqueRelevantTerms" to each PubMed ID's MeSH heading lists, filtering for relevant MeSHs and removing redundant MeSHs
    relevant_mesh_series = mesh_series.apply(getUniqueRelevantTerms)

    # Applies "getStdNameAndUnkCUIFromMeSHList" to each PubMed ID's unique relevant mesh terms, retrieving both standard names for concepts we know and CUIs for concepts we do not have in our controlled vocabulary
    translated = relevant_mesh_series.apply(getStdNameAndUnkCUIFromMeSHList).apply(pd.Series)
    known_std = translated[0]
    unk_cui = set(translated[1].sum())
    if len(unk_cui) > 0:
        print("There are MeSH terms corresponding to Concept Unique IDs (CUIs) which do not \
have defined standard names in your controlled vocabulary. They are the following \
(with a possible term for that CUI aside): ")
        pd.set_option('display.max_rows', None)
        print(cui_pref_terms_df[cui_pref_terms_df['CUI'].apply(lambda x: x in unk_cui)]\
            [['CUI', 'term']].drop_duplicates(subset='CUI').to_string(index=False))
        pd.set_option('display.max_rows', 10)

        print("You may re-run the script after adding those to the controlled vocabulary at " + args.cont_vocab_path)
        cui_pref_terms_df[cui_pref_terms_df['CUI'].apply(lambda x: x in unk_cui)].to_csv(args.n, index=False)
        print("Writing the table of possible terms to " + args.n)

    # Add a column with known standard names in a string separated by semicolons. If the annotations contain 'Not Specified' along with any other annotation, remove all 'Not Specified' annotations
    pub_data[args.o] = known_std.apply(lambda x: ";".join([anno for anno in set(x) if not (len(set(x)) > 1 and anno == 'Not Specified')]))

    if args.u:
        pub_data[args.u] = relevant_mesh_series.apply(getAllCUIFromMeSHList).apply(lambda x: ";".join(x))

    print("Adding known annotations and saving at " + args.output_path)
    pub_data.to_csv(args.output_path, index=False)



