from pipeline.data_classes import *
from utils.triple_reader import *
import itertools


def is_in_range(t, lst):
	'''
	Prüft für ein Start/End Index, ob sie innerhalb einer Start/End Kombination aus einer Liste liegt
	
	:param lst1: Vergleichsliste
	:param lst2: Start/End Index, das geprüft wird
	:return: True falls innerhalb, False falls nicht
	'''
    for tpl in lst:
        if (t[0] >= tpl.boundaries[0]) and (t[1] <= tpl.boundaries[1]):
            return True
    return False


def filter_tuples(lst1, lst2):
	'''
	Prüft für eine Liste von Start/End Indizes, ob sie innerhalb einer Start/End Kombination aus einer anderen Liste liegt
	
	:param lst1: Vergleichsliste
	:param lst2: Liste, die geprüft wird
	:return: korrigierte Liste
	'''
    filtered_lst = []
    for tpl in lst2:
        if not is_in_range(tpl.boundaries, lst1):
            filtered_lst.append(tpl)
    return filtered_lst



class TripleFinder:
    def __init__(self, triple_reader):
		'''
		Initialisierung des TripleFinders
		
		:param triple_reader: Tripleeader Objekt
		'''
        self.triple_reader = triple_reader
    
    
    def run(self, doc):
        '''
        Extrahiert aus den gefundenen Entitäten alle Triples und speichert sie im Document Objekt

        :param doc: Das Document Objekt ohne Triples
        :return: Das Document Objekt mit den Triples
        '''
        if doc.status == 'okay':
		
			# keine Triples möglich, falls weiger als 2 Entitäten gefunden wurden
            if len(doc.entities) + len(doc.expanded_entities) < 2:
                doc.status = 'only one entity found -> no triples possible'
                return doc

            doc = self.simple_aligner(doc)
            
            if doc.title.uri != None:
                doc = self.subject_aligner(doc, doc.entities, 'Subject-Triple-aligner')
            
            if len(doc.expanded_entities) > 0:
                doc = self.expanded_aligner(doc)
            
            if doc.title.uri != None:
                doc = self.subject_aligner(doc, doc.expanded_entities, 'Subject-Expanded-Triple-aligner')

            if len(doc.triples) < 1:
                doc.status = 'no triples found'
                return doc

        return doc
    
    
    def simple_aligner(self, doc):
        '''
		Findet Relationen zwischen einfachen Entitäten innerhalb einzelner Sätze und speichert sie im Document Objekt

        :param doc: Document Objekt
        :return: Das Document Objekt mit den Triples
        '''
        for sid, (start, end) in enumerate(doc.text.sentences_boundaries):
            
			# finde alle Entitäten des jeweiligen Satzes
            es = [j for j in doc.entities if j.boundaries[0] >= start and j.boundaries[1] <= end]

			# iteriere durch alle Entitäten-Kombinationen
            for o in itertools.permutations(es, 2):                
                if o[0].uri == o[1].uri or o[0].boundaries == o[1].boundaries:
                    continue
                
                predicates = self.triple_reader.get(o[0].uri, o[1].uri)

                for pred in predicates:
                    t = self.triple_reader.get_label(pred)
                    if isinstance (t, str):
                        pred = Entity(pred,
                                      boundaries=None,
                                      surfaceform=t,
                                      title=t,
                                      annotator='Simple-Aligner')

                        triple = Triple(subject=o[0],
                                        predicate=pred,
                                        object=o[1],
                                        sentence_id=sid,
                                        annotator='Simple-Aligner')

                        doc.triples.append(triple)
        
        return doc
    

    def expanded_aligner(self, doc):
		'''
		Findet Relationen zwischen erweiterten Entitäten innerhalb einzelner Sätze und speichert sie im Document Objekt

        :param doc: Document Objekt
        :return: Das Document Objekt mit den Triples
        '''
		# finde alle bereits an Triples beteiligten Entitäten
        if len(doc.triples) > 0:
            triple_subjects = [triple.subject for triple in doc.triples if triple.subject.boundaries != None]
            triple_objects = [triple.object for triple in doc.triples if triple.object.boundaries != None]
            triple_ents = triple_subjects + triple_objects
        else:
            triple_ents = []
            
        subject = [j for j in doc.entities if '_subject' in j.annotator]
        for sid, (start, end) in enumerate(doc.text.sentences_boundaries):
			# finde alle (erweiterten) Entitäten des jeweiligen Satzes
            ents = [j for j in doc.entities if j.boundaries[0] >= start and j.boundaries[1] <= end]
            ex_ents = [j for j in doc.expanded_entities if j.boundaries[0] >= start and j.boundaries[1] <= end]   

            if len(triple_ents) > 0:
                ex_ents = filter_tuples(triple_ents, ex_ents)

            for o1, o2 in itertools.product(subject + ents, ex_ents):

                if o1.uri == o2.uri or o1.boundaries == o2.boundaries:
                    continue
                    
                predicates = self.triple_reader.get(o1.uri, o2.uri)

                for pred in predicates:
                    t = self.triple_reader.get_label(pred)
                    if isinstance (t, str):
                        pred = Entity(pred,
                                      boundaries=None,
                                      surfaceform=t,
                                      title=t,
                                      annotator='Expanded-Entity-aligner')

                        triple = Triple(subject=o1,
                                        predicate=pred,
                                        object=o2,
                                        sentence_id=sid,
                                        annotator='Expanded-Entity-aligner')

                        doc.triples.append(triple)
                        
                        
                if o2.uri == o1.uri or o2.boundaries == o1.boundaries:
                    continue
                    
                predicates = self.triple_reader.get(o2.uri, o1.uri)   

                for pred in predicates:
                    t = self.triple_reader.get_label(pred)
                    if isinstance (t, str):
                        pred = Entity(pred,
                                      boundaries=None,
                                      surfaceform=t,
                                      title=t,
                                      annotator='Expanded-Entity-aligner')

                        triple = Triple(subject=o2,
                                        predicate=pred,
                                        object=o1,
                                        sentence_id=sid,
                                        annotator='Expanded-Entity-aligner')

                        doc.triples.append(triple)
        
        return doc
    
    
    
    def subject_aligner(self, doc, ents, annotator):
        '''
		Findet Relationen zwischen Entitäten unterschiedlicher Sätze (Subjekt des 1. Satzes muss beteiligt sein) und speichert sie im Document Objekt

        :param doc: Document Objekt
        :return: Das Document Objekt mit den Triples
        '''
		# Subjekt ermitteln, da nur Relationen gesucht werden, an denen das Subjekt beteiligt ist
        subjects = [ent for ent in doc.entities if '_subject' in ent.annotator]
        if len(subjects) == 0:
            return doc
			
        for subject in subjects:
            for sid, (start, end) in enumerate(doc.text.sentences_boundaries[1:]):
                es = [j for j in ents if j.boundaries[0] >= start and j.boundaries[1] <= end]

                for o in es:
                    if subject.uri == o.uri or subject.boundaries == o.boundaries:
                        continue

                    predicates = self.triple_reader.get(subject.uri, o.uri)

                    for pred in predicates:
                        t = self.triple_reader.get_label(pred)
                        if isinstance (t, str):
                            pred = Entity(pred,
                                          boundaries=None,
                                          surfaceform=t,
                                          title=t,
                                          annotator=annotator)

                            triple = Triple(subject=subject,
                                            predicate=pred,
                                            object=o,
                                            sentence_id=sid + 1,
                                            annotator=annotator)

                            doc.triples.append(triple)
                        
        return doc
    