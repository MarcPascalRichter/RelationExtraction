from collections import defaultdict
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import pickle


# Code teilweise von https://github.com/Babelscape/crocodile/blob/main/utils/triplereader.py
class TripleReader:
    
    def __init__(self, triple_file, language):
        with open(triple_file, "rb") as file:
            self.trip_dict = pickle.load(file)
        
        self.d_properties = defaultdict(list)
        endpoint_url = "https://query.wikidata.org/sparql"
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        query = f'SELECT (STRAFTER(STR(?property), "entity/") AS ?pName) ?propertyLabel ?propertyDescription ?propertyAltLabel WHERE {{?property wikibase:propertyType ?propertyType. SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"{language}\". }}}}'
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            results = sparql.query().convert()
        except FileNotFoundError:
            print(query)
        for result in results["results"]["bindings"]:
            self.d_properties[result['pName']['value']] = result['propertyLabel']['value']
                   
                
    def get_label(self, p):
        p = self.d_properties[p]
        return p
        
        
    def get(self, suri, objuri):
        try:
            return self.trip_dict[suri+'\t'+objuri]
        except:
            return []
        
        
    def get_exists(self, suri, rel, objuri):
        try:
            rel_list = self.trip_dict[suri+'\t'+objuri]
            if rel in rel_list:
                return True
            else:
                return False
        except:
            return False
        
        
    def subject_has_relation(self, suri, rel):
        try:
            subject_relations = {key: value for key, value in self.trip_dict.items() if key.startswith(suri + '\t')}
            relations = next(iter(subject_relations.values()))
            if rel in relations:
                return True
            else:
                return False
        except:
            return False
