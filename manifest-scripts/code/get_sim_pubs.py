import pandas as pd
from Bio import Entrez
import json
import argparse
import sys

def get_sim_pubs_from_ids(pmids, titles, csbc_pson_pmids, api_key=None):
    elink_result = Entrez.elink(dbfrom='pubmed', id=pmids, retmode='json', 
                                linkname='pubmed_pubmed', api_key=api_key)
    linksets_dic = json.loads("".join(list(elink_result)))['linksets']
    intersect = lambda links, pmid: [link for link in links if link != pmid and 
                                     link in csbc_pson_pmids]
    sim_articles = pd.Series([intersect(linkset['linksetdbs'][0]['links'], 
    	linkset['ids'][0]) for linkset in linksets_dic])
    id_title_map = dict(zip(pmids, titles))
    sim_titles = sim_articles.apply(lambda ids: ";".join([id_title_map[pmid] for pmid in ids]))
    sim_links = sim_articles.apply(lambda x: ";".join(['https://www.ncbi.nlm.nih.gov/pubmed/' + pmid for pmid in x]))
    return sim_links, sim_titles

def add_sim_pubs_to_df(df, pmid_col="PubMed", input_title_col="Title", output_link_col="sim_links", output_title_col="sim_titles",
                       api_key=None, in_place=True):
    pmids = df[pmid_col].apply(lambda x: [field for field in x.split("/") if field][-1].split("?term=")[-1])
    csbc_pson_pmids = set(pmids)
    if in_place:
    	sim_links, sim_titles = get_sim_pubs_from_ids(pmids, df[input_title_col], csbc_pson_pmids, api_key)
    	df[output_link_col] = sim_links
    	df[output_title_col] = sim_titles
    	return df
    else:
        df_copy = df.copy()
        sim_links, sim_titles = get_sim_pubs_from_ids(pmids, df_copy[input_title_col], csbc_pson_pmids, api_key)
        df_copy[output_link_col] = sim_links
        df_copy[output_title_col] = sim_titles
        return df_copy

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("data_path", help="location for file containing manifest data. Assumed to be in CSV form \
		with a header row.")
	parser.add_argument("output_path", help="location for output file, will be a CSV with a header row. It will be \
		identical to the input CSV, with two added columns. One containing a String for each row/publication, with the String \
		containing semicolon-separated links to similar articles. The column will be named either \
		'sim_links' or the value of the -link_column option. Additionally, there will be a column containing a String with \
		semicolon-separated titles of the similar articles, in the same order as the links.")
	parser.add_argument("-i", "-input_id_column", type=str, help="column name containing PubMed IDs (for example, '31442407') of the query articles. \
		Can also contain links to PubMed articles (for example, 'https://www.ncbi.nlm.nih.gov/pubmed/31442407', as the script will extract the PubMed ID from the URL. Will default to 'PubMed'")	
	parser.add_argument("-t", "-input_title_column", type=str, help="column name containing article titles, which will be used to \
		populte the column named 'sim_titles' or the value of -output_title_col. Will default to 'Title'")
	parser.add_argument("-l", "-link_column", type=str, help="column name for the similar publications links. Will be a \
		comma separated list of PubMed links for each row/publication. Will default to 'sim_links'")
	parser.add_argument("-o", "-output_title_col", type=str, help="column name for the similar publications. Will be a \
		comma separated list of titles for each row/publication. Will default to 'sim_titles'")
	parser.add_argument("-a", "-api_key", type=str, help="API key for Entrez. Increases the rate limit from 3 requests \
		per second of the E-utilities (used to retrieve PubMed data) to 10 requests per second. Users can obtain an API \
		key from the settings page of an NCBI account at https://www.ncbi.nlm.nih.gov/account/settings/")
	parser.add_argument("-e", "-email", type=str, help="Email of individual running script, to be passed to Entrez in \
		order to access E-utilities. Will be  used if NCBI observes requests that violate their policies. Will give \
		a warning if omitted, and an IP address can be blocked by NCBI if a violating request is made without an email \
		address included")
	args = parser.parse_args()
	pub_data = pd.read_csv(args.data_path)
	if args.i is None:
		args.i = "PubMed"
	if args.t is None:
		args.t = "Title"
	if args.l is None:
		args.l = "sim_links"
	if args.o is None:
		args.o = "sim_titles"
	if args.e is not None:
		Entrez.email = args.e
	add_sim_pubs_to_df(pub_data, args.i, args.t, args.l, args.o, args.a).to_csv(args.output_path, index=False)