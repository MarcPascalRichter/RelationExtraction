class Text:
    def __init__(self, text, clean_text, doc, doc_merged_entities, sentences_boundaries):
        '''
        Initialisierung der Text Klasse.
		
        :param text: Der zu verarbeitende Text
        '''
        
        self.text = text
        self.clean_text = clean_text
        self.doc = doc
        self.doc_merged_entities = doc_merged_entities
        self.sentences_boundaries = sentences_boundaries
        
        

class Document:
    def __init__(self, docid, date, url, title, text, entities=None, expanded_entities=None, triples=None):
        '''
        Initialisierung der Document Klasse.
        
        :param docid: Document ID
		:param date: Datum des Texts
        :param title: Titel des Texts
        :param url: URL des Texts
        :param text: Text
        :param entities: Liste aller Entitäten
        :param expanded_entities: Liste aller erweiterten Entitäten
        :param triples: Liste aller Triples
        '''
        self.docid = docid
        self.date = date
        self.url = url
        self.status = 'okay' if len(entities) + len(expanded_entities) > 0 else 'no entities found -> no triples possible'
        self.title = title
        self.text = text
        self.entities = [] if entities is None else entities
        self.expanded_entities = [] if expanded_entities is None else expanded_entities
        self.triples = [] if triples is None else triples
        
    
    def toJSON(self):
        '''
		Document als JSON ausgeben
      
        :return: Document im JSON-Format
        '''
        j = self.__dict__.copy()
        j['entities'] = [i.toJSON() for i in j['entities']] if 'entities' in j and j['entities'] is not None else []
        j['expanded_entities'] = [i.toJSON() for i in j['expanded_entities']] if 'expanded_entities' in j and j['expanded_entities'] is not None else []
        j['triples'] = [i.toJSON() for i in j['triples']] if 'triples' in j and j['triples'] is not None else []
        return j
    
    
    
class Entity:
    def __init__(self, uri, boundaries, surfaceform, title=None, annotator=None, in_bracket=-1):
        '''
        Initialisierung der Entity Klasse.
        
        :param uri: ID der Entität
        :param boundaries: Start- und Enindex der Entität
        :param surfaceform: Textdarstellung der Entität
        :param title: Titel der Entität
        :param annotator: Annotator der Entität
        :param in_bracket: Optionale Nummer der Klammer, in der der Text steht
        '''
        self.uri = uri
        self.title = title
        self.boundaries = boundaries
        self.surfaceform = surfaceform
        self.annotator = annotator
        self.in_bracket = in_bracket
        

    def toJSON(self):
		'''
		Entität als JSON ausgeben
      
        :return: Entität im JSON-Format
        '''
        return self.__dict__.copy()
    
	
    def __hash__(self):
		'''
		Hash-Wert eines Entity Objekts berechnen
		
		:return: Hash-Wert
		'''
        return hash((self.uri, self.boundaries, self.surfaceform))
    
	
    def __eq__(self, other):
		'''
		Gleichheit zweier Entity Objekte prüfen
		
		:param other: Vergleichs-Entität
		:return: True falls gleich, False falls nicht gleich
		'''
        return self.uri == other.uri and self.boundaries == other.boundaries and self.surfaceform == other.surfaceform
    
    

class Triple:
    def __init__(self, subject, predicate, object, sentence_id, confidence=0.0, annotator=None):
        '''
        Initialisierung der Triple Klasse.
        
        :param subject: Subjekt des Triples als Entity Objekt
        :param predicate: Prädikat des Triples als Entity Objekt
        :param object: Objekt des Triples als Entity Objekt
        :param sentence_id: Satz-Nummer, in dem das Triple vorkommt
        :param confidence: Confidence Score des Triples
        :param annotator: Annotator des Triples
        '''
        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.sentence_id = sentence_id
        self.confidence = confidence
        self.annotator = annotator

        
    def toJSON(self):
		'''
		Triple als JSON ausgeben
      
        :return: Triple im JSON-Format
        '''
        j = self.__dict__.copy()
        j['subject'] = j['subject'].toJSON()
        j['predicate'] = j['predicate'].toJSON()
        j['object'] = j['object'].toJSON()
        return j
    
    
    def __eq__(self, other):
		'''
		Gleichheit zweier Triple Objekte prüfen
		
		:param other: Vergleichs-Triple
		:return: True falls gleich, False falls nicht gleich
		'''
        return self.subject == other.subject and self.object == other.object and self.predicate == other.predicate and self.sentence_id == other.sentence_id
