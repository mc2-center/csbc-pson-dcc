import pandas as pd

def edit_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]



if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("umls_path", help="location for UMLS metathesaurus \
        input file. This should be the 'MRCONSO.RRF' file from the UMLS \
        Knowledge Sources: File Downloads page (currently \
		https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html).\
		It is marked as 'The most widely used UMLS file.'")
    parser.add_argument("mesh_path", help="location for MeSH tree input file. \
        This should be the 'Descriptor Tree with Headings' XLSX file the MeSH \
        'Whats New in MeSH' page \
        (currently https://www.nlm.nih.gov/mesh/whatsnew.html).")
    parser.add_argument("-m", "-meshtocui_map_path", type=str, help="path for \
        output CSV mapping possible MeSH terms in a column called 'STR', and \
        their corresponding Concept Unique IDs (according to UMLS \
        metathesaurus) in a column called 'CUI'. Will default to \
        'mesh_cui_map_total.csv'.")
    parser.add_argument("-c", "-cuitoterms_map_path", type=str, help="path for \
        output CSV mapping Concept Unique IDs (CUIs) we may encounter in a \
        column called 'CUI' to possible terms for our controlled vocabulary in \
        a columns called 'term'. Will default to 'cuitoterms_map.csv'")
	args = parser.parse_args()

    if args.m is None:
        args.m = "mesh_cui_map_total.csv"
    if args.c is None:
        args.c = "cuitoterms_map.csv"

    col_names=['CUI', 'LAT', 'TS', 'LUI', 'STT', 'SUI',
               'ISPREF', 'AUI', 'SAUI', 'SCUI', 'SDUI',
               'SAB', 'TTY', 'CODE', 'STR', 'SRL', 'SUPPRESS',
               'CVF']

    # This creates an iterable object in python, which does not contain the
    # whole CSV but allows me to iterate through chunks of 5000 lines, since I
    # am going to be filtering them, and not have the entire MRCONSO.RRF file in
    # memory at once
    iter_csv = pd.read_csv(args.data_path, sep="|", names=col_names,
                       iterator=True, chunksize=5000, index_col=False)

    # Iterates through the file 'chunk's and filters for English terms in either
    # the MeSH, ICD10CM or NCI thesaurus ontologies.
    thes = pd.concat([chunk[(chunk['LAT']=='ENG') & chunk['SAB'].isin(['MSH',
        'ICD10CM', 'NCI'])] for chunk in iter_csv])

    mesh_cui_map = thes.loc[thes['SAB']=='MSH', ['CUI', 'STR']]. \
    drop_duplicates()

    mesh_cui_map.to_csv(args.m, index=False)

    all_meshs = pd.read_excel(args.mesh_path, skiprows=1, \
        names=['tree', 'mesh'])


    pref_terms = thes.drop_duplicates(['CUI', 'STR'], keep='first')
    icd_terms = thes[thes['SAB']=='ICD10CM']
    nci_terms = thes[thes['SAB']=='NCI']
    concept_map = pd.DataFrame(columns=['CUI', 'pref_name', 'ont_source',
        'term', 'edit_score', 'mesh'])

    for i, row in all_meshs.iterrows():
        concepts = thes[(thes['STR']==row['mesh']) & (thes['SAB'] == 'MSH')]
        if len(concepts) == 0:
            print(row['mesh'])
            print('Has NO concepts associated!!')
            concept_map = concept_map.append(pd.DataFrame({'CUI':[None],
                'pref_name':[''], 'ont_source':[''], 'term':[row['mesh']],
                'edit_score':[None], 'mesh':row['mesh']}))
            continue
        if len(concepts) > 1:
            print(row['mesh'])
            print('Has multiple concepts associated!!')

        for cui in concepts.CUI.values:
            pref_names = pref_terms[pref_terms['CUI']==cui].\
            sort_values(['TS', 'LUI', 'STT', 'SUI', 'ISPREF'],
                ascending=[True, True, True, True, False])
            if len(pref_names) >= 1:
                pref_name = pref_names.STR.values[0]
            else:
                #print(cui)
                #print(row['mesh'])
                #print('Has no preferred name in our thesaurus')
                pref_name = ''
            icds = icd_terms[icd_terms['CUI'] == cui].STR
            icd_df = pd.DataFrame({'term':icds, 'edit_score':icds. \
                apply(lambda x: edit_distance(x, pref_name))})
            icd_df['ont_source'] = 'ICD10CM'
            ncis = nci_terms[nci_terms['CUI'] == cui].STR
            nci_df = pd.DataFrame({'term':ncis, 'edit_score':ncis. \
                apply(lambda x: edit_distance(x, pref_name))})
            nci_df['ont_source'] = 'NCI'
            temp = icd_df.append(nci_df)
            temp['CUI'] = cui
            temp['pref_name'] = pref_name
            temp['mesh'] = row['mesh']

            concept_map = concept_map.append(temp)
        if(i%500 == 0):
            print(i)

    concept_map = concept_map.drop_duplicates()
    concept_map.to_csv(args.mc index=False)
