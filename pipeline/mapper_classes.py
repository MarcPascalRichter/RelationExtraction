import pickle
from utils.utils import read_tsv
from german_nouns.lookup import Nouns


class WikiMapper:
    def __init__(self, path_to_title_ids_dict, path_to_title_ids_expanded_dict, path_to_subclass_file):
		'''
		Initialisierung eines WikiMapper Objekts
		
		:param path_to_title_ids_dict: Pfad zum Titel-ID-Dict
		:param path_to_title_ids_expanded_dict: Pfad zum erweiterten Titel-ID-Dict
		:param path_to_subclass_file: Pfad zum Unterklassen-Mapping
		'''
        with open(path_to_title_ids_expanded_dict, 'rb') as file:
            self.title_ids_expanded_mapping = pickle.load(file)   
            
        with open(path_to_title_ids_dict, 'rb') as file:
            self.title_ids_mapping = pickle.load(file)
            
        with open(path_to_subclass_file, 'rb') as file:
            self.subclass_mapping = pickle.load(file)
        
		
    def title_to_ids_expanded(self, title):
		'''
		Gibt die Liste der Wikidata IDs zurück, die anhand der Klammer-Erweiterung mit dem vorgegebenen Begriff übereinstimmen

        :param title: Der Begriff, für den die erweiterte Suche durchgeführt wird
        :return: Eine Liste von Wikidata IDs
		'''
        return self.title_ids_expanded_mapping.get(title, [])
    
	
    def title_to_ids(self, title):
		'''
		Gibt die Liste der Wikidata IDs zurück, die mit dem vorgegebenen Begriff übereinstimmen

        :param title: Der Begriff, für den die Suche durchgeführt wird
        :return: Eine Liste von Wikidata IDs
		'''
        return self.title_ids_mapping.get(title, [])


    def get_subclass_ids(self, wiki_id):
        '''
        Gibt die Liste der Wikidata IDs zurück, die eine Unterklasse der angegebenen Wikidata ID sind

        :param wiki_id: Die Wikidata ID, für die die Unterklassen Wikidata IDs abgerufen werden sollen
        :return: Eine Liste von Wikidata IDs
        '''
        return self.subclass_mapping.get(wiki_id, [])
    
    
class CountryMapper:
    def __init__(self, country_adj_mapping_file, country_nation_mapping_file):
		'''
		Initialisierung eines CountryMapper Objekts
		
		:param country_adj_mapping_file: Pfad zum Länder-Adjektiv-Mapping
		:param country_nation_mapping_file: Pfad zum Nation-Länder-Mapping
		'''
        country_adj_mapping = read_tsv(country_adj_mapping_file, ['adjective', 'country', 'wiki_id'])
        country_nation_mapping = read_tsv(country_nation_mapping_file, ['nation', 'country', 'wiki_id'])

        self.country_dict_adj = {}
        for index, row in country_adj_mapping.iterrows():
            self.country_dict_adj[row['adjective']] = (row['wiki_id'], row['country'])
			
        self.country_dict_nation = {}
        for index, row in country_nation_mapping.iterrows():
            self.country_dict_nation[row['nation']] = (row['wiki_id'], row['country'])
            
			
    def is_country_adj(self, adj):
        return adj in self.country_dict_adj
    
	
    def is_nation(self, nation):
        return nation in self.country_dict_nation
    
	
    def get_wiki_id_from_country_adj(self, adj):
        return self.country_dict_adj[adj][0]
    
	
    def get_wiki_id_from_nation(self, nation):
        return self.country_dict_nation[nation][0]
    
	
    def get_country_from_country_adj(self, adj):
        return self.country_dict_adj[adj][1]
    
	
    def get_country_from_nation(self, nation):
        return self.country_dict_nation[nation][1]
    
    
class NounMapper:
    def __init__(self):
		'''
		Initialisierung eines NounMapper Objekts
		'''
        self.nouns = Nouns()
		
    
    def get_noun(self, text):
        '''
        Gibt mithilfe eines deutschen Lookup Datensatzes die Lemma Form für den vorgegebenen Begriff zurück

        :param text: Der Begriff, für den das Lemma abgerufen werden soll
        :return: Das Lemma für den vorgegebenen Begriff
        '''
        if text.lower() != 'us':
            word = self.nouns[text]
            if word:
                return word[0]['lemma']
        return text
		