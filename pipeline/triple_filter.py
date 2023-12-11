import re


monatsnamen = ['Jänner', 'Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember']


class TripleFilter:         
    def __init__(self, conf_score):
		'''
		Initialisierung des TripleFilters
		
		:param conf_score: Threshold des Confidence-Score-Filters
		'''
        self.conf_score = float(conf_score)
		self.rebel = Rebel()

        
    def run(self, doc):
		'''
		Durchlaufe die Filter Schritte
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''        
        if doc.status != 'okay':
            return doc
        
        doc = self.bracket_filter(doc)
        doc = self.duplicate_filter(doc)
        doc = self.boundaries_filter(doc)
        doc = self.relation_type_filter(doc)
        doc = self.noun_filter(doc)
        doc = self.country_object_filter(doc)
        doc = self.country_subject_filter(doc)
        
        if len(doc.triples) > 0:
            doc.status = 'okay -> triples found'
        else:
            doc.status = 'no triples found'
            
        return doc
    
	
    def run_confidence_filter(self, doc):
		'''
		Durchlaufe den Confidence-Score-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        compare_triples = []

        if doc.status == 'okay -> triples found':

            doc = self.duplicate_filter(doc)
            doc = self.confidence_filter(doc)

            if len(doc.triples) > 0:
                doc.status = 'okay -> triples found'
            else:
                doc.status = 'no triples found'
        return doc
		
		
	def bracket_filter(self, doc):
		'''
		Klammer-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if not ((trip.subject.in_bracket != -1 and trip.object.in_bracket != trip.subject.in_bracket and trip.object.boundaries[0] > trip.subject.boundaries[0]) or (trip.object.in_bracket != -1 and trip.subject.in_bracket != trip.object.in_bracket and trip.subject.boundaries[0] > trip.object.boundaries[0])):
                triple_list.append(trip)
        doc.triples = triple_list
        return doc
		
		
	def duplicate_filter(self, doc):
        '''
		Duplikat-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
		# korrekte Sortierung für Annotator
        def sort_annotator(annotator):
            if annotator == "Simple-Aligner":
                return 0
            elif annotator.startswith("Subject"):
                return 1
            else:
                return 2

        triple_uris = []
        triple_list = []

        doc.triples.sort(key=lambda x: (-x.confidence, sort_annotator(x.annotator), x.subject.annotator, x.subject.surfaceform, x.object.surfaceform, x.subject.in_bracket != x.object.in_bracket, x.subject.in_bracket, x.object.in_bracket))
            
        for trip in doc.triples:
            if (trip.subject.uri, trip.predicate.uri, trip.object.uri) not in triple_uris:
                triple_list.append(trip)
                triple_uris.append((trip.subject.uri, trip.predicate.uri, trip.object.uri))
                
        doc.triples = triple_list
        return doc
    

    def boundaries_filter(self, doc):
		'''
		Wortgrenzen-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if (trip.subject.boundaries[0] < trip.object.boundaries[0] or trip.subject.boundaries[1] > trip.object.boundaries[1]) and (trip.object.boundaries[0] < trip.subject.boundaries[0] or trip.object.boundaries[1] > trip.subject.boundaries[1]):
                pattern = r'(^|\s)' + re.escape(trip.object.surfaceform) + r'($|\s)'
                treffer = re.finditer(pattern, trip.subject.surfaceform)
                if len(list(treffer)) == 0:
                    pattern = r'(^|\s)' + re.escape(trip.subject.surfaceform) + r'($|\s)'
                    treffer = re.finditer(pattern, trip.object.surfaceform)
                    if len(list(treffer)) == 0:
                        triple_list.append(trip)
        doc.triples = triple_list
        return doc
		
		
	def relation_type_filter(self, doc):
		'''
		Relationstyp-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if trip.predicate.uri in ['P2894', 'P407', 'P7959', 'P530', 'P2936', 'P172', 'P1001', 'P366', 'P279', 'P2341', 'P2578', 'P2579', 'P1889', 'P585', 'P734', 'P735', 'P937', 'P47', 'P551', 'P1321', 'P6886', 'P1412', 'P945']:
                continue
            if trip.predicate.uri == 'P31' and trip.object.uri == 'Q82799':
                continue
            if trip.predicate.uri == 'P527' and trip.subject.title in monatsnamen:
                continue
            if trip.predicate.uri == 'P361' and trip.object.title in monatsnamen:
                continue
            if trip.predicate.uri == 'P31' and trip.object.title in monatsnamen:
                continue
            if trip.predicate.uri == 'P31' and trip.object.title in ['Jahrhundert', 'Mensch', 'Sitz (juristische Person)']:
                continue
            if trip.subject.title in ['Gemeinde', 'Kanton', 'Stadt', 'Ortsteil', 'Kreisstadt', 'Region', 'Département', 'Kreisstadt', 'Einwohnergemeinde', 'Bundesstaat', 'Partei']:
                continue
            if re.match(r'^(\w+) \((\1)\)$', trip.subject.title) or re.match(r'^(\w+) \((\1)\)$', trip.object.title):
                continue
            if trip.subject.title.startswith('Gemeinde (') or trip.subject.title.startswith('Kanton (') or trip.subject.title.startswith('Stadt (') or trip.subject.title.startswith('Ortsteil (') or trip.subject.title.startswith('Kreisstadt (') or trip.subject.title.startswith('Region (') or trip.subject.title.startswith('Département (') or trip.subject.title.startswith('Kreisstadt (') or trip.subject.title.startswith('Einwohnergemeinde (') or trip.subject.title.startswith('Bundesstaat (') or trip.subject.title.startswith('Partei ('):
                continue
                
            triple_list.append(trip)
            
        doc.triples = triple_list
        return doc
           
            
    def noun_filter(self, doc):
		'''
		Substantiv-Filter
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if 'sub_NOUN' not in trip.subject.annotator:
                triple_list.append(trip)
        doc.triples = triple_list
        return doc
            
        
    def country_object_filter(self, doc):
		'''
		Länder-Filter für Objekte der Triples
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if "countries_" in trip.object.annotator:
                countries_text = trip.object.annotator.split('_')[1]
                countries_label = trip.object.annotator.split('_')[2]
            else:
                countries_text = None
                countries_label = None
            if "ent_" in trip.subject.annotator:
                ent_label = trip.subject.annotator.split('_')[1]
            elif "_subject_" in trip.subject.annotator:
                ent_label = trip.subject.annotator.split('_')[2]
            else:
                ent_label = None
            if countries_label == None or countries_label == ent_label or countries_text == trip.subject.surfaceform:
                triple_list.append(trip)
        doc.triples = triple_list
        return doc


    def country_subject_filter(self, doc):
		'''
		Länder-Filter für Subjekte der Triples
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []
        for trip in doc.triples:
            if "countries_" in trip.subject.annotator:
                countries_text = trip.subject.annotator.split('_')[1]
                countries_label = trip.subject.annotator.split('_')[2]
            else:
                countries_text = None
                countries_label = None
                
            if "ent_" in trip.object.annotator:
                ent_label = trip.object.annotator.split('_')[1]
            elif "_subject_" in trip.object.annotator:
                ent_label = trip.object.annotator.split('_')[2]
            else:
                ent_label = None
            if countries_label == None or countries_label == ent_label or countries_text == trip.object.surfaceform:
                triple_list.append(trip)
        doc.triples = triple_list
        return doc
    
       
    def confidence_filter(self, doc):
		'''
		Confidence-Score-Filter (inkl. REBEL)
		
		:param doc: Document Objekt
		:return: Document Objekt mit gefilterten Triples
		'''
        triple_list = []

        if doc.status == 'okay -> triples found':
            
            help_list = []
            for triple in doc.triples:
                conf_score = self.conf_score
                if (triple.confidence >= conf_score and triple.predicate.uri in ('P150')) or (triple.confidence >= 0.975 and triple.predicate.uri in ('P131')):
                    help_list.append((triple.subject.uri, triple.predicate.uri, triple.object.uri))
                    
            for triple in doc.triples:
                conf_score = self.conf_score
                
                if triple.annotator in ['Expanded-Entity-aligner', 'NoSubject-Expanded-Triple-aligner'] and not triple.subject.title.endswith(')') and not triple.object.title == triple.object.surfaceform:
                    conf_score = 0.995
                elif triple.predicate.uri in ['P36', 'P495']:
                    conf_score = 0.99 
                elif triple.annotator in ['Expanded-Entity-aligner', 'NoSubject-Expanded-Triple-aligner']:
                    conf_score = 0.985
                elif triple.predicate.uri == 'P31':
                    conf_score = 0.985
                elif 'NoSubject-Triple-aligner' in triple.annotator:
                    conf_score = 0.98
                elif triple.predicate.uri in ['P17', 'P131', 'P1376']:
                    conf_score = 0.975

                if (triple.predicate.uri == 'P131' and (triple.object.uri, 'P150', triple.subject.uri) in help_list) or (triple.predicate.uri == 'P150' and (triple.object.uri, 'P131', triple.subject.uri) in help_list):
                    conf_score = 0.0
                   
                confidence = triple.confidence 
  
                if confidence >= conf_score:
                    triple_list.append(triple)
                
				# REBEL
                elif confidence + 0.02 >= conf_score:
                    if self.rebel.filter(triple, doc):
                        triple_list.append(triple)
                
        doc.triples = triple_list
        return doc
		