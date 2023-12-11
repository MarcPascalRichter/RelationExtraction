import re
from pipeline.data_classes import *
from pipeline.mapper_classes import *
from utils.triple_reader import *
from itertools import combinations
from thefuzz import fuzz
import requests
from utils.utils import text_in_brackets
from bs4 import BeautifulSoup
import json
from datetime import datetime
import dateparser

    
DATSETS_PATH = '/datasets/'

months = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August','September', 'Oktober', 'November', 'Dezember', 
          'januar', 'februar', 'märz', 'april', 'mai', 'juni', 'juli', 'august' 'september', 'oktober', 'november', 'dezember',
          'Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez',                   
          'jan', 'feb', 'mär', 'apr', 'mai', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dez']


def help_func(doc, w):
    if w.dep_ == 'cj' and w.head.dep_ == 'cd':
        if w.head.head.dep_ == 'pd':
            for c in w.head.head.head.children:
                if c.dep_ == 'sb' and c.pos_ in ('NOUN', 'PROPN'):
                    for ent in doc.ents:
                        if c.text == ent.text:
                            return c.text + '_' + ent.label_
        else:
            return help_func(doc, w.head.head)
    else:
        for c in w.children:
            if c.dep_ == 'nk' and c.pos_ in ('NOUN', 'PROPN'):
                for ent in doc.ents:
                    if c.text == ent.text:
                        return c.text + '_' + ent.label_ 
    return ''


def is_in_range(ent, lst):
    '''
    Überprüft, ob eine gegebene Entität im Bereich anderer Entitäten liegt

    :param ent: Die zu überprüfende Entität
    :param lst: Eine Liste von Entitäten, deren Bereiche überprüft werden sollen
    :return: True, wenn die Entität im Bereich anderer Entitäten liegt, sonst False
    '''
    for ent2 in lst:
        if (
            (ent.boundaries[0] > ent2.boundaries[0] and ent.boundaries[1] < ent2.boundaries[1]) or
            (ent.boundaries[0] == ent2.boundaries[0] and ent.boundaries[1] < ent2.boundaries[1]) or
            (ent.boundaries[0] > ent2.boundaries[0] and ent.boundaries[1] == ent2.boundaries[1])
        ):
            return True
    return False



def expand_hyphen_words(word):
    '''
    Erweitert Bindestrich-Wörter zu allen möglichen kürzeren Kombinationen
	Die Kombinationen bei 'Science-Fiction-Autor' wären z.B. ['Science', 'Fiction', 'Autor', 'Science-Fiction', 'Science-Autor', 'Fiction-Autor']

    :param word: Das Wort, das verarbeitet werden soll
    :return: Eine Liste aller möglichen Kombinationen
    '''
    result = []
    word_list = word.split('-')
    for i in range(1, len(word_list)):
        result.extend(['-'.join(t) for t in combinations(word_list, i)])
    return result



class NER:
    def __init__(self, wiki_mapper, country_mapper, triple_reader):
        self.wiki_mapper = wiki_mapper
        self.country_mapper = country_mapper
        self.triple_reader = triple_reader
        self.nouns = NounMapper()
        self.disambiguation_dict = {}
        
        with open(DATASETS_PATH + 'disambiguation_dict.data', 'rb') as f:
            self.disambiguation_dict = pickle.load(f)
            
        with open(DATASETS_PATH + 'stopwords-de.json', 'r') as file:
            self.stopwords = json.load(file)
			


	def run(self, input_data): 
		'''
		Ausführung der NER
		
		:param input_data: Alle benötigte Daten als Liste [Index, Datum, URL, Text Objekt)
		:return: Document Objekt mit Entitäten
		'''
        index = input_data[0]
        date = input_data[1]
        url = input_data[2]
        text = input_data[3]
        ents = []
        ex_ents = []
        
        if len(text.doc_merged_entities) > 100:
            return Document(index, date, url, '', text, ents, ex_ents)
        
		# Titel-Extraktion
        title = self.get_title(text)
        if title.uri != None:
            ents.append(title)
  
        ents.extend(self.get_ents_from_ents(text))
        ents.extend(self.get_ents_from_tokens(text))
        ents.extend(self.get_ents_from_spotlight(text))
        ents.extend(self.get_ents_from_noun_chunks(text))
        ents.extend(self.get_ents_from_brackets(text))
        ents.extend(self.get_ents_from_quotation_marks(text))
        ents.extend(self.get_ents_from_adjectives(text))
        ents.extend(self.get_ents_from_dates(text))
        
        ents = [ent for ent in ents if ent is not None]
        ex_ents = [ex_ent for ex_ent in ex_ents if ex_ent is not None]

        ents, ex_ents = self.filter_entities1(ents)
        
        ex_ents.extend(self.expand_ents(text, ents, title))
        
        ents, ex_ents = self.filter_entities2(ents, ex_ents, text)
        
        ents = [ent for ent in ents if ent is not None]
        ex_ents = [ex_ent for ex_ent in ex_ents if ex_ent is not None]
        
        ents = list(set(ents))
        ex_ents = list(set(ex_ents))

        return Document(index, date, url, title, text, ents, ex_ents)
		
		

    def get_title(self, text):
        '''
        Extrahiert den Titel aus einem Text und erstellt für diesen ein Entity Objekt

        :param text: Ein Text Objekt
        :return: Der Titel als Entity Objekt
        '''
        doc = text.doc_merged_entities
        clean_text = text.clean_text

        title = ''
        start = end = 0

        # Subjekt des Satzes und seine Position finden
        for token in doc:
            if token.dep_ == 'sb':
                try:
                    title = token.text
                    start = clean_text.index(title)
                    end = start + len(title) - 1
                    break
                except:
                    title = ''

        # Prüfen, ob es eine Wikidata-URI gibt
        uri = next((x[0] for x in self.wiki_mapper.title_to_ids_expanded(title.lower()) if x[1].lower() == title.lower()), None) 
        
        if uri is None and len(title) > 0:
            title = self.nouns.get_noun(title)
            uri = next((x[0] for x in self.wiki_mapper.title_to_ids_expanded(title.lower()) if x[1].lower() == title.lower()), None) 
            
        annotator = next((f'_subject_{ent.label_}' for ent in doc.ents if ent.text == title), '_subject')
        
        return Entity(uri, (start, end), title, title, annotator, text_in_brackets(clean_text, (start, end)))
		
		
		
    def get_ents_from_ents(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der Spacy Entities

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        doc = text.doc_merged_entities
        t = text.clean_text
        
        for ent in doc.ents:
			# für jede Entität prüfen, ob sie in einem Länder-Mapping vorkommt
            ents_country = self.get_ents_from_countries(text, ent)
            if len(ents_country) == 0:
                old_len = len(ents)
                ents.extend(self.add_ent1(ent.text, ent.text, f'ent_{ent.label_}', ent.start_char, ent.end_char - 1, t))
                ents.extend(self.add_ent1(ent.lemma_, ent.text, f'ent_{ent.label_}', ent.start_char, ent.end_char - 1, t))
                
                s = ent.lemma_.split()
				# falls Begriff aus mehr als 3 Wörtern besteht, 
				# wird er auch einmal ohne das erste und einmal ohne das letzte Wort getestet
                if len(ents) == old_len and len(s) > 3:
                    search_terms = [' '.join(s[1:]), ' '.join(s[:-1])]
                    for search_term in search_terms:
                        ents.extend(self.add_ent1(search_term, ent.text, f'ent_{ent.label_}', ent.start_char, ent.end_char - 1, t))

            else:
                ents.extend(ents_country)
                
        return ents 
		
		
		
	def get_ents_from_countries(self, text, word):
		'''
        Extrahiert Länder-Entitäten auf Grundlage der Länder-Mappings

        :param text: Text Objekt des zu analysierenden Textes
		:param word: Wort, das analysiert wird
		:return: Liste von Enititäten
        '''
        doc = text.doc_merged_entities
        t = text.clean_text
        search_term_lower = word.lemma_.lower()
        ents = []
        
        if self.country_mapper.is_country_adj(search_term_lower):
            label = self.get_country_dependency(doc, search_term_lower)
            wiki_id = self.country_mapper.get_wiki_id_from_country_adj(search_term_lower)
            country_name = self.country_mapper.get_country_from_country_adj(search_term_lower)
            label = ('countries_' + label).strip() if label else 'countries'
            ents.append(self.add_ent3(wiki_id, word.text, country_name, label, word.start_char, word.end_char - 1, t))
                
        if self.country_mapper.is_nation(search_term_lower):
            wiki_id = self.country_mapper.get_wiki_id_from_nation(search_term_lower)
            country_name = self.country_mapper.get_country_from_nation(search_term_lower)
            ents.append(self.add_ent3(wiki_id, word.text, country_name, 'countries', word.start_char, word.end_char - 1, t))
            
        return ents 



	def get_country_dependency(self, doc, country):
        '''
        Gibt Begriff (inkl. Entitätstyp) zurück, auf den sich das Länderadjektiv bezieht

        :param doc: Das Spacy Dokument, in dem die Abhängigkeit gesucht wird
        :param country: Das Länderadjektiv
        :return: Abhängiger Begriff (inkl. Entitätstyp) oder leerer String, wenn nicht gefunden
        '''
        for i, word in enumerate(doc):
            lemma = word.lemma_.lower()
            if lemma == country and self.country_mapper.is_country_adj(lemma):
                if word.dep_ == 'nk':
                    if word.head.dep_ == 'pd':
                        for child in word.head.head.children:
                            if child.dep_ == 'sb' and child.pos_ in ('NOUN', 'PROPN'):
                                for ent in doc.ents:
                                    if child.text == ent.text:
                                        return child.text + '_' + ent.label_
                    elif word.head.dep_ == 'app' and word.head.head.pos_ in ('NOUN', 'PROPN'):
                        for ent in doc.ents:
                            if word.head.head.text == ent.text:
                                return word.head.head.text + '_' + ent.label_
                    else:
                        return help_func(doc, word.head)
        return ''		
		
		
		
	def get_ents_from_tokens(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der Substantiv Tokens eines gegebenen Texts

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        doc = text.doc_merged_entities
        t = text.clean_text
        last_token = None
        
        for token in doc:            
            if token.pos_ in ['PROPN', 'NOUN'] and token.dep_ not in ['par', 'pnc']:
                token_start_idx = token.idx
                token_end_idx = token.idx + len(token.text) - 1
                
                ents.extend(self.add_ent1(token.lemma_, token.text, 'sub_' + token.pos_, token_start_idx, token_end_idx, t))
                ents.extend(self.add_ent1(token.text, token.text, 'sub_' + token.pos_, token_start_idx, token_end_idx, t))
                
				# Doppel-Substantiv-Extraktion
                if last_token and last_token.pos_ in ['PROPN', 'NOUN'] and last_token.dep_ not in ['par', 'pnc']:
                    combined_search_term = last_token.lemma_ + ' ' + token.lemma_
                    combined_text = last_token.text + ' ' + token.text
                    ents.extend(self.add_ent1(combined_search_term, combined_text, 'noun_double', token_start_idx, token_end_idx, t))
                    ents.extend(self.add_ent1(combined_text, combined_text, 'noun_double', token_start_idx, token_end_idx, t))
                
            last_token = token
        
        return ents
		
		
		
	def get_ents_from_spotlight(self, text):
		'''
        Extrahiert Entitäten mithilfe von DBpedia Spotlight

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        t = text.clean_text
        doc = text.doc
        ents = []
		
        for ent in doc.ents:
            try:
                dbpedia_uri = ent.kb_id_
                similarity_score = ent._.dbpedia_raw_result['@similarityScore']
				
				# Threshold festlegen
                if float(similarity_score) > 0.9:
                    search_term = dbpedia_uri.split('/')[-1]
                    s = search_term.split('_')
                    search_terms = [search_term]
                    if len(s) > 3:
                        search_terms.extend([search_term, ' '.join(s[1:]), ' '.join(s[:-1])])

                    for search_term in search_terms:
                        ents.extend(self.add_ent1(search_term, ent.text, 'DBpedia_ent', ent.start_char, ent.end_char - 1, t))
						
            except Exception as e:
                continue
                
        return ents


    
    def get_ents_from_noun_chunks(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der Noun Chunks

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
		doc = text.doc_merged_entities
        t = text.clean_text
        
        old_nc = ''
        old_start = -1
        old_end = -1
        
        for n in doc.noun_chunks:
            chunk_text = n.text
            if len(chunk_text) > 3:
                try:
					# möglichen Artikel am Beginn entfernen
                    if n[0].tag_ == 'ART':
                        chunk_text = n[1:].text 

                    start_idx = t.index(chunk_text)
                    end_idx = start_idx + len(chunk_text) - 1
                    ents.extend(self.add_ent1(chunk_text, chunk_text, 'nc', start_idx, end_idx, t))
                    
					# falls 2 noun chunks direkt hintereinander stehen, die Kombination auch aufnehmen
                    if (old_end + 2) == n[0].idx:
                        chunk_text_combined = ' '.join([old_nc, n.text])
                        ents.extend(self.add_ent1(chunk_text_combined, chunk_text_combined, 'nc', old_start, end_idx, t))
                
                    old_nc = chunk_text
                    old_start = start_idx
                    old_end = n[-1].idx + len(n[-1].text) - 1
                    
                except Exception as e:
                    continue

        return ents

      
	  
    def get_ents_from_brackets(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der Texte in Klammern

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        t = text.clean_text
        
        a = re.findall(r'\([^()]*\)', t)
        b = re.findall(r'\([^()]*\)', re.sub(r'\([^()]*\)','', t))
        brackets = [bracket[1:-1].strip() for bracket in a + b]
                
        for bracket in brackets:
            try: 
                start_idx = t.index(bracket)
                end_idx = start_idx + len(bracket) - 1
                ents.extend(self.add_ent1(bracket, bracket, 'brackets', start_idx, end_idx, t))
            except:
                continue
                
        return ents

        
		
    def get_ents_from_quotation_marks(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der Texte in Anführungszeichen

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        t = text.clean_text
        data = re.findall('\'[^\']*\'', t)
        
        for d in data:
            try:
                search_term = d[1:-1].strip()
                start_idx = t.index(search_term)
                end_idx = start_idx + len(search_term) - 1
                ents.extend(self.add_ent1(search_term, search_term, 'quotation_marks', start_idx, end_idx, t))
            except:
                continue
                
        return ents                


        
    def get_ents_from_adjectives(self, text):
        '''
        Extrahiert Entitäten auf Grundlage der 'isch'-Adjektive

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        doc = text.doc_merged_entities
        t = text.clean_text
        
        for token in doc:
            if token.pos_ == 'ADJ' and 'isch' in token.text:
                adj_list = []
				# prüfen, ob 'isch' am Ende steht
                pattern = re.compile('^(.*?)(isch.*)$')
                match = pattern.match(token.text)
                if match:
                    result = match.group(1)
					# Endung und Umlaute ändern
                    adj_list.append(result)
                    adj_list.append(result.replace('ä','a').replace('ö','o').replace('ü','u'))
                    adj_list.append(result + 'en')
                    adj_list.append(result.replace('ä','a').replace('ö','o').replace('ü','u')+'en')
                    adj_list.append(result + 'n')
                    adj_list.append(result.replace('ä','a').replace('ö','o').replace('ü','u')+'n')
                    for adj in adj_list:
                        ents.extend(self.add_ent1(adj, token.text, 'ent_adj', token.idx, token.idx + len(token.text) - 1, t))

        return ents 
		
		
		
	def get_ents_from_dates(self, text):
		'''
        Extrahiert Entitäten auf Grundlage der Datums-Angaben

        :param text: Text Objekt des zu analysierenden Textes
		:return: Liste von Enititäten
        '''
        ents = []
        regex_dates = fr'((\d{{1,2}})[.-] ?(\d{{1,2}}[.-]|{"|".join(months)}) ?(\d{{4}}|\d{{2}})|(\d{{4}}))'
        matches_dates = re.finditer(regex_dates, text.clean_text, re.MULTILINE)

        for match in matches_dates:
            date_str = match.group()
            try:
                date = dateparser.parse(date_str, languages=['de'], settings={'PREFER_DAY_OF_MONTH': 'first', 'RELATIVE_BASE': datetime(2020, 1, 1), 'TIMEZONE': 'UTC'})
                stdform = date.strftime('%Y-%m-%dT00:00:00Z^^http://www.w3.org/2001/XMLSchema#dateTime')
                entity = Entity(uri = stdform,
                                boundaries = (match.start(), match.end()),
                                title = date_str.lstrip(' of '),
                                surfaceform = date_str.lstrip(' of '),
                                annotator = 'Date_Linker')
                
                ents.append(entity)
                
            except Exception as e:
                continue

        return ents
		
		
		
	def expand_ents(self, text, ents, text_title):
		'''
		Erweiterung der extrahierten Entitäten
		
		:param text: Text Objekt des zu analysierenden Textes
		:param ents: Bisher extrahierte Entitäten
		:param text_title: Entity Objekt des Text-Titels
		:return: Liste von erweiterten Enititäten
		'''
        ex_ents = []
        doc_text = text.clean_text
		
		# Datumsangaben und DBpedia Entitäten werden nicht erweitert
        ent_list = [(ent.uri, ent.title, ent.surfaceform, ent.annotator, ent.boundaries) for ent in ents if ent.annotator not in ['Date_Linker', 'DBpedia_ent']]
        
        for ent in ent_list:  
            wiki_id = ent[0]
            title = ent[1]
            text = ent[2]
            annotator = ent[3]
            boundaries = ent[4]
            
            ex_ents.extend(self.expand_hyphen(text, boundaries, doc_text))
            ex_ents.extend(self.expand_subclass(wiki_id, title, text, annotator, boundaries, doc_text))
            ex_ents.extend(self.expand_brackets(title, text, annotator, boundaries, doc_text, text_title))
            ex_ents.extend(self.expand_disambiguation(wiki_id, title, text, annotator, boundaries, doc_text, text_title))
                
        return ex_ents
    
    
	
    def expand_hyphen(self, text, boundaries, doc_text):
		'''
		Bindestrich-Erweiterung
		
		:param text: Text der Ausgangs-Entität
		:param boundaries: Token-Grenzen der Ausgangs-Entität
		:param doc_text: Gesamter Text
		:return: Liste von erweiterten Enititäten
		'''
        ex_ents = []
        
        if '-' in text:
            texts = expand_hyphen_words(text)
            for text in texts:
                ex_ents.extend(self.add_ent1(text, text, 'hyphen expanded from ' + text, boundaries[0], boundaries[1], doc_text))
                
        return ex_ents
    
    
    def expand_subclass(self, wiki_id, title, text, annotator, boundaries, doc_text):
		'''
		Unterklassen-Erweiterung
		
		:param wiki_id: ID der Ausgangs-Entität
		:param title: Titel der Ausgangs-Entität
		:param text: Text der Ausgangs-Entität
		:param annotator: Annotator-Name der Ausgangs-Entität
		:param boundaries: Token-Grenzen der Ausgangs-Entität
		:param doc_text: Gesamter Text
		:return: Liste von erweiterten Enititäten
		'''
        ex_ents = []
        subclass_ids = self.wiki_mapper.get_subclass_ids(wiki_id)
        
        for subclass_id in subclass_ids:
            ex_ents.append(self.add_ent3(subclass_id, text, title, 'subclass expanded from ' + title + ' ' + annotator, boundaries[0], boundaries[1], doc_text))
            
        return ex_ents



    def expand_brackets(self, title, text, annotator, boundaries, doc_text, text_title):
		'''
		Klammer-Erweiterung
		
		:param title: Titel der Ausgangs-Entität
		:param text: Text der Ausgangs-Entität
		:param annotator: Annotator-Name der Ausgangs-Entität
		:param boundaries: Token-Grenzen der Ausgangs-Entität
		:param doc_text: Gesamter Text
		:param text_title: Entity Objekt des Text-Titels
		:return: Liste von erweiterten Enititäten
		'''
        ex_ents = []
        id_titles = []
        id_titles = self.wiki_mapper.title_to_ids_expanded(title.lower())
        
        for it in id_titles:
            if it[1].lower() != title.lower():
                start = doc_text.find(it[1])
                end = start + len(it[1]) - 1
				
				# Neue Entität ist entweder Subjekt des Satzes, im Satz vorkommende Entität oder Erweiterung einer Entität
                if it[1] in doc_text and text_title.surfaceform.lower() == title.lower():
                    ex_ents.append(self.add_ent3(it[0], text, it[1], '_subject', start, end, doc_text))
                elif it[1] in doc_text:
                    ex_ents.append(self.add_ent3(it[0], text, it[1], 'ent', start, end, doc_text))
                else:
                    ex_ents.append(self.add_ent3(it[0], text, it[1], 'expanded from ' + title + ' ' + annotator, boundaries[0], boundaries[1], doc_text))

			# Unterklassen-Erweiterung anwenden
            ex_ents.extend(self.expand_subclass(it[0], it[1], text, annotator, boundaries, doc_text))
            
        return ex_ents
    
    
    
    def expand_disambiguation(self, wiki_id, title, text, annotator, boundaries, doc_text, text_title):
		'''
		Disambiguierungs-Erweiterung
		
		:param wiki_id: ID der Ausgangs-Entität
		:param title: Titel der Ausgangs-Entität
		:param text: Text der Ausgangs-Entität
		:param annotator: Annotator-Name der Ausgangs-Entität
		:param boundaries: Token-Grenzen der Ausgangs-Entität
		:param doc_text: Gesamter Text
		:param text_title: Entity Objekt des Text-Titels
		:return: Liste von erweiterten Enititäten
		'''
        ex_ents = []
        
        if self.triple_reader.get_exists(wiki_id, 'P31', 'Q4167410'):
			# Disambiguierungs-Entitäten aus Dict nehmen oder neu ermitteln
            if wiki_id in self.disambiguation_dict:
                disambiguation_ents = self.disambiguation_dict[wiki_id]
            else:
                disambiguation_ents = self.get_disambiguation_ents(title)
                self.disambiguation_dict[wiki_id] = disambiguation_ents
                
            for ent in disambiguation_ents:
                start = doc_text.find(ent[1])
                end = start + len(ent[1]) - 1
				
				# Neue Entität ist entweder Subjekt des Satzes, im Satz vorkommende Entität oder Erweiterung einer Entität
                if ent[1] in doc_text and len(ent[1]) > len(text) and text_title.surfaceform.lower() == title.lower():
                    ex_ents.append(self.add_ent3(ent[0], ent[1], ent[1], '_subject', start, end, doc_text))
                elif ent[1] in doc_text and len(ent[1]) > len(text):
                    ex_ents.append(self.add_ent3(ent[0], ent[1], ent[1], 'ent', start, end, doc_text))
                else:
                    ex_ents.append(self.add_ent3(ent[0], text, ent[1], 'disambiguation expanded from ' + title + ' ' + annotator, boundaries[0], boundaries[1], doc_text))
                    
				# Unterklassen-Erweiterung anwenden
                ex_ents.extend(self.expand_subclass(ent[0], ent[1], text, annotator, boundaries, doc_text))
                    
        return ex_ents
                    
					
            
    def get_disambiguation_ents(self, ent_title):
		'''
		Ermittle alle Disambiguierungs-Entitäten zu einer Ausgangs-Entität
		
		:param ent_title: Titel der Ausgangs-Entität
		:return: Liste mit Disambiguierungs-Entitäten
		'''
        ents = []

        url = 'https://de.wikipedia.org/wiki/' + ent_title
        response = requests.get(url=url,)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        titles = [link['href'].replace('/wiki/', '').replace('_', ' ') for link in soup.select('#bodyContent a[href*="/wiki/"]')] 
             
        for title in titles:
            if title not in ['Wikipedia:Begriffsklärung', 'Wikipedia:Kategorien', 'Kategorien:Begriffsklärung'] and (fuzz.token_sort_ratio(ent_title, title) > 50 or (len(ent_title) <= 4 and ent_title.lower() not in self.stopwords)):
                id_titles = self.wiki_mapper.title_to_ids(title.lower())
                ents.extend([(it[0], title) for it in id_titles])

        return ents
		
		
		
    def filter_entities1(self, ents):
		'''
		Verschiebe Entitäten, die in anderen enthalten sind, in die erweiterte Entitätenliste
		
		:param ents: Bisher extrahierte Entitäten
		:return: Liste mit Entitäten und Liste mit erweiterten Entitäten		
		'''
        ents_new = []
        ex_ents = []
        ents = [ent for ent in ents if ent is not None]

        for ent in ents:
            if not is_in_range(ent, ents):
                if not ent.annotator.endswith('_no_match'):
                    ents_new.append(ent)
                else:
                    ex_ents.append(ent)
            else:
                ent.annotator = 'expanded from longer entity'
                ex_ents.append(ent)
                    
        return ents_new, ex_ents 
    
    
	
    def filter_entities2(self, ents, ex_ents):
		'''
		Verschiebt Entitäten von erweiterte Entitätenliste in Entitätenliste, wenn Annotator 'ent' oder 'subject' ist
		
		:param ents: Bisher extrahierte Entitäten
		:param ex_ents: Bisher ermittelte erweiterte Entitäten
		:return: Liste mit Entitäten und Liste mit erweiterten Entitäten		
		'''
        ents = [ent for ent in ents if ent is not None]
        ex_ents = [ex_ent for ex_ent in ex_ents if ex_ent is not None]
        
        ents_new = ents.copy()
        ex_ents_new = []
        
        for ex_ent in ex_ents:
            if ex_ent.annotator == 'ent':
                ents_new.append(ex_ent)
            elif ex_ent.annotator == '_subject':
                ents_new = [ent for ent in ents_new if not ent.annotator.startswith('_subject')]
                ents_new.append(ex_ent)
            else:
                ex_ents_new.append(ex_ent)
                
        return ents_new, ex_ents_new
                
    
        
    def add_ent1(self, ent_title, ent_text, annotator, start, end, text):
        ents = []
        ents.extend(self.add_ent2(ent_title, ent_text, annotator, start, end, text))
        if annotator != 'ent_LOC_adj':
            ents.extend(self.add_ent2(self.nouns.get_noun(ent_title), ent_text, annotator, start, end, text))

        if '-' in ent_title:
            ents.extend(self.add_ent2(ent_title.replace('-',' '), ent_text, annotator, start, end, text))
            ents.extend(self.add_ent2(self.nouns.get_noun(ent_title.replace('-',' ')), ent_text, annotator, start, end, text))
            
        pattern = r'^(\s\d+)'
        match = re.search(pattern, text[end + 1:])
        if match:
            numbers = match.group(1)
            ents.extend(self.add_ent2(ent_title + numbers, ent_text + numbers, annotator, start, end + len(numbers), text))
            ents.extend(self.add_ent2(self.nouns.get_noun(ent_title) + numbers, ent_text + numbers, annotator, start, end + len(numbers), text))
            
        return ents
            
    
    
    def add_ent2(self, ent_title, ent_text, annotator, start, end, text):
        ents = []
        id_titles = self.wiki_mapper.title_to_ids(ent_title.lower())
        
        for it in id_titles:
            ann = annotator + '_no_match' if it[1] != ent_title else annotator
            ents.append(self.add_ent3(it[0], ent_text, it[1], ann, start, end, text))
            
        if len(id_titles) == 0 and annotator == 'ent_PER' and 2 < len(ent_title.split()) < 7:
            for combi in self.get_name_combinations(ent_title):
                id_titles = self.wiki_mapper.title_to_ids(' '.join(combi).lower())
                for it in id_titles:
                    ents.append(self.add_ent3(it[0], ent_text, it[1], annotator, start, end, text))
                if len(ents) > 0:  
                    break
            
        return ents
    
	
    
    def get_name_combinations(self, ent_title):
        split = ent_title.split()
        combis = [c for i in range(len(split), 1, -1) for c in combinations(split, i)]

        combinations_with_first_last = []
        combinations_with_last = []
        combinations_with_first = []
        combinations_standard = []

        for s in combis:
            if split[0] in s and split[-1] in s:
                combinations_with_first_last.append(s)
            elif split[-1] in s:
                combinations_with_last.append(s)
            elif split[0] in s:
                combinations_with_first.append(s)
            else:
                combinations_standard.append(s)

        combinations_with_first_last.sort(key=lambda x: split.index(x[0]))
        combinations_with_last.sort(key=lambda x: split[::-1].index(x[-1]))
        combinations_with_first.sort(key=lambda x: split.index(x[0]))
        combinations_standard.sort()

        combinations_with_first_last.extend(combinations_with_last)
        combinations_with_first_last.extend(combinations_with_first)
        combinations_with_first_last.extend(combinations_standard)

        return combinations_with_first_last
		    
    
    
    def add_ent3(self, wiki_id, ent_text, ent_title, annotator, start, end, text):
        if wiki_id is not None and ent_title != '*' and not ent_title.endswith('(Begriffsklärung)') and len(re.findall(r'[A-ZÄÖÜa-zäöüß]', ent_title)) > 1:
            return Entity(wiki_id, (start, end), ent_text, ent_title.replace('_', ' '), annotator, text_in_brackets(text, (start, end)))
            