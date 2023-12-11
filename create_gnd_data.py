import pickle
import re
import pandas as pd
from datetime import datetime
import dateparser
import json


DATASETS_PATH = '/datasets/'



def get_date(text):
	regex_dates = fr'((\d{{1,2}})[.-] ?(\d{{1,2}}[.-]|{"|".join(months)}) ?(\d{{4}}|\d{{2}})|(\d{{4}}))'
	matches_dates = re.finditer(regex_dates, text, re.MULTILINE)

	for match in matches_dates:
		text = match.group()
		break
		
	stdform = None
	try:
		date = dateparser.parse(text, languages=['de'], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime(2020, 1, 1), 'TIMEZONE': 'UTC'})
		stdform = date.strftime('%Y-%m-%dT00:00:00Z^^http://www.w3.org/2001/XMLSchema#dateTime')

	except Exception as e:
		pass

	return stdform
		
		

def create_gnd_files():
	for i in range(5):
		gnd_list = []
		file = open(DATASETS_PATH + 'gnd/gnd0' + str(i) + '.jsonld', 'r')
		
		while True:
			line = file.readline()
			if not line:
				break
			else:
				line = line[1:]
				try:
					line_json = json.loads(line)
				except:
					line_json = json.loads(line[:-1])
				try:
					entry = {'id' : line_json["@id"],
							 'name' : line_json["preferredName"],
							 'type' : line_json["@type"],
							 'json' : line_json}      
					gnd_list.append(entry)
				except:
					pass
		file.close()

		gnd_df = pd.DataFrame(gnd_list)

		with open(DATASETS_PATH + "gnd/gnd_df_" + str(i) + ".data", "wb") as f:
			pickle.dump(gnd_df, f)
			
			
			
def create_gnd_wikidata_mapping():
	gnd_wiki_dict = {}

    for i in range(5):
        with open(DATASETS_PATH + 'gnd/gnd_df_' + str(i) + '.data', "rb") as file:
            data = pickle.load(file)
            
        for index, row in data.iterrows():
            try:
                gnd_id = row['json']['@id']
                for entry in row["json"]["sameAs"]:
                    if entry["collection"]["name"] == "Wikidata":
                        gnd_wiki_dict[gnd_id] = entry["@id"].split('/')[-1]
            except:
                continue
				
	return gnd_wiki_dict
			
			
	
def create_gnd_relations(gnd_wiki_mapping):	
	attributes = ['founder', 'relatedSubject', 'isPartOf', 'associatedCountry', 'predecessor', 
              'successor', 'organizerOrHost', 'placeOfEvent', 'relatedPerson', 'associatedPlace', 'topic', 
              'placeOfDeath', 'placeOfBusiness', 'professionOrOccupation', 'placeOfBirth', 
              'relatedEvent', 'architect', 'isA', 'location', 'relatedOrganisation']

	date_attributes = ['dateOfEstablishment', 'dateOfTermination', 'dateOfProduction', 'associatedDate', 'dateOfEvent']

	birth_death_attributes = ['dateOfBirth', 'dateOfDeath']

	all_attributes = attributes + date_attributes + birth_death_attributes + ['familialRelationship']

	months = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August','September', 'Oktober', 'November', 'Dezember', 
			'januar', 'februar', 'märz', 'april', 'mai', 'juni', 'juli', 'august' 'september', 'oktober', 'november', 'dezember',
			'Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez',                   
			'jan', 'feb', 'mär', 'apr', 'mai', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dez']
        
    gnd_triples = []
    
    for i in range(5):
        with open(DATASETS_PATH + 'gnd/gnd_df_' + str(i) + '.data', "rb") as file:
            data = pickle.load(file)
            
        for index, value in data.iterrows():
            json = value['json']
            subject_id = json['@id']
            subject_type = json['@type']
            subject_wiki_id = gnd_wikidata_mapping.get(subject_id, None)
            if subject_wiki_id == None:
                continue

            preferredName = json['preferredName']
            for attr in all_attributes:
                try:
                    if attr == 'familialRelationship':
                        for entry in json[attr]:
                            object_id = entry['@id']
                            object_wiki_id = gnd_wikidata_mapping.get(object_id, None)
                            if object_wiki_id == None:
                                continue
                            gnd_triples.append([subject_id, subject_type, subject_wiki_id, preferredName, entry['relationship'], object_id, object_wiki_id, entry['preferredName']])
                    elif attr in birth_death_attributes:
                        date = get_date(json[attr])
                        if date == None:
                            continue
                        gnd_triples.append([subject_id, subject_type, subject_wiki_id, preferredName, attr, date, date, date])
                    elif attr in date_attributes:
                        for entry in json[attr]:
                            date = get_date(entry)
                            if date == None:
                                continue
                            gnd_triples.append([subject_id, subject_type, subject_wiki_id, preferredName, attr, date, date, date])
                    else:
                        for entry in json[attr]:
                            object_id = entry['@id']
                            object_wiki_id = gnd_wikidata_mapping.get(object_id, None)
                            if object_wiki_id == None:
                                continue
                            gnd_triples.append([subject_id, subject_type, subject_wiki_id, preferredName, attr, object_id, object_wiki_id, entry['preferredName']]) 
                except:
                    continue
                    
                    
    columns = ['gnd_subject_id', 'gnd_subject_type', 'wiki_subject_id', 'gnd_subject_name', 'gnd_predicate', 'gnd_object_id', 'wiki_object_id', 'gnd_object_name']
    result_df = pd.DataFrame(gnd_triples, columns=columns)
    result_df.to_csv(DATASETS_PATH + 'gnd/gnd_relations.tsv', sep='\t')
		
		
		
def main():
	create_gnd_files()
	gnd_wiki_mapping = create_gnd_wikidata_mapping()
	create_gnd_relations(gnd_wiki_mapping)



if __name__ == '__main__':
    main()  