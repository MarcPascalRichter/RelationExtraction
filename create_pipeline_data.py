import sqlite3
import pickle
import re
import os
from csv import reader
from tqdm import tqdm
import pandas as pd


DATASETS_PATH = '/datasets/'
CROCODILE_TEXT_PATH = '/datasets/crocodile_text/'



def get_wikidata_ids_in_wikipedia_abstracts():
    wikidata_ids = set()
    pattern = re.compile(r'wikidata="(.*?)"')

    for root, _, files in os.walk(CROCODILE_TEXT_PATH):
        for file_name in files:
            with open(os.path.join(root, file_name), 'r') as file:
                for line in file:
                    for match in re.finditer(pattern, line):
                        wikidata_ids.add(match.group(1))
    return wikidata_ids



def create_wikidata_relations(wikidata_ids = None):
    wikidata_relations = {}
    
    with open(DATASETS_PATH + 'wikidata/wikidata-triples.csv', 'r') as read_obj:
        csv_reader = reader(read_obj)
		
		if wikidata_ids == None:
        
			for i, row in tqdm(enumerate(csv_reader)):
				split = row[0].split('\t')
				subj, rel, obj = split[0], split[1], split[2]
				
				if (subj.startswith('Q') or 'http://www.w3.org/' in subj) and (obj.startswith('Q') or 'http://www.w3.org/' in obj):
					subjobj = subj + '\t' + obj
					wikidata_relations.setdefault(subjobj, []).append(rel)  

		else:
		
			for i, row in tqdm(enumerate(csv_reader)):
				split = row[0].split('\t')
				subj, rel, obj = split[0], split[1], split[2]
				
				if subj in wikidata_ids and (subj.startswith('Q') or 'http://www.w3.org/' in subj) and (obj.startswith('Q') or 'http://www.w3.org/' in obj):
					subjobj = subj + '\t' + obj
					wikidata_relations.setdefault(subjobj, []).append(rel)
                    
    with open(DATASETS_PATH + "wikidata_relations.data", "wb") as f:
        pickle.dump(wikidata_relations, f)


        
def create_title_ids_mapping():
    title_ids_mapping = {}
    title_ids_expanded_mapping = {}
    
    with sqlite3.connect(os.path.join(DATASETS_PATH, 'wikidata/index_dewiki-latest.db')) as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT wikidata_id, wikipedia_title FROM mapping")
        results = c.fetchall()
    
    for result in results:
        wikidata_id = results[0]
        wikipedia_title = results[1].replace('_',' ')
        wikipedia_title_lower = wikipedia_title.lower()
		
		title_entry = (wikidata_id, wikipedia_title)
		
		title_ids_mapping.setdefault(wikipedia_title_lower, []).append(title_entry)
        title_ids_expanded_mapping.setdefault(wikipedia_title_lower, []).append(title_entry)
        
        if "(" in wikipedia_title_lower and ")" in wikipedia_title_lower:
            search_string = re.sub('\(.*\)', '', wikipedia_title_lower).strip()
            title_ids_expanded_mapping.setdefault(search_string, []).append(title_entry)
    
    with open(DATASETS_PATH + "title_ids_mapping.data", "wb") as f1:
        pickle.dump(title_ids_mapping, f1)
            
    with open(DATASETS_PATH + "title_ids_expanded_mapping.data", "wb") as f2:
        pickle.dump(title_ids_expanded_mapping, f2)
		        
		
        
def create_subclass_relations(wikidata_ids = None):
    subclass_relations = {}
    
    with open(DATASETS_PATH + 'wikidata/wikidata-triples.csv', 'r') as read_obj:
        csv_reader = reader(read_obj)
		
		if wikidata_ids == None:
		
			for i, row in tqdm(enumerate(csv_reader)):
				split = row[0].split('\t')
				subj, rel, obj = split[0], split[1], split[2]
				
				if subj.startswith('Q') and obj.startswith('Q') and rel == 'P279':
					subclass_relations.setdefault(obj, []).append(subj)
					
		else:
		
			for i, row in tqdm(enumerate(csv_reader)):
				split = row[0].split('\t')
				subj, rel, obj = split[0], split[1], split[2]
				
				if subj in wikidata_ids and subj.startswith('Q') and obj.startswith('Q') and rel == 'P279':
					subclass_relations.setdefault(obj, []).append(subj)
		
            
    with open(DATASETS_PATH + "subclass_relations.data", "wb") as f:
        pickle.dump(subclass_relations, f)
        
		
        
def create_country_mappings():
    with open(DATASETS_PATH + "title_ids_mapping.data", "rb") as file:
        mapping = pickle.load(file)
    
    countries = pd.read_csv(DATASETS_PATH + 'countries.tsv', sep='\t', names=['country', 'adjective', 'nation'])

    adj_list = []
    nation_list = []
    for index, row in countries.iterrows():
        country = row['country']
        adjectives = row['adjective'].split(', ')
        nations = row['nation'].split(', ')
        wiki_ids = mapping.get(country.lower(), [])        
        wiki_id = next((c[0] for c in wiki_ids if c[1] == country), None)

        for adj in adjectives:
            adj_list.append([adj.lower(), country, wiki_id])
        for nation in nations:
            nation_list.append([nation.lower(), country, wiki_id])

    adj_df = pd.DataFrame(adj_list, columns=['adjective', 'country', 'wiki_id'])
    nation_df = pd.DataFrame(nation_list, columns=['nation', 'country', 'wiki_id'])

    adj_df.to_csv(DATASETS_PATH + 'country_adj_mapping.tsv', sep='\t')
    nation_df.to_csv(DATASETS_PATH + 'country_nation_mapping.tsv', sep='\t') 



def main():
	wikidata_ids = None
    #wikidata_ids = get_wikidata_ids_in_wikipedia_abstracts()

    create_wikidata_relations(wikidata_ids)
	
    create_title_ids_mapping()
    create_subclass_relations(wikidata_ids)
    create_country_mappings()



if __name__ == '__main__':
    main()    