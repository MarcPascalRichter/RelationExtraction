import pandas as pd

class Writer:
    
    def __init__(self, file):
		'''
		Initialisierung eines Writer Objekts
		
		:param file: TSV Datei Name, um Triples abzuspeichern
		'''
        self.file = file
        
        
    def run(self, data):
		'''
		Schreibt alle Triples in eine TSV Datei
		
		:param data: Dataframe mit gefilterten Triples in einer Spalte
		'''
        triple_list = []
        for doc in data['filtered_document_2'].tolist():
            text = doc.text.clean_text
            date = doc.date
            url = doc.url
            for triple in doc.triples:
                subject = triple.subject.title
                subject_uri = triple.subject.uri
                predicate = triple.predicate.title
                predicate_uri = triple.predicate.uri            
                object = triple.object.title
                object_uri = triple.object.uri
                confidence = triple.confidence
                triple_list.append([text, date, url, confidence, subject, subject_uri, predicate, predicate_uri, object, object_uri])
        
        triple_df = pd.DataFrame(triple_list, columns =['text', 'date', 'url', 'confidence', 'subject', 'subject_uri', 'predicate', 'predicate_uri', 'object', 'object_uri'])
        triple_df.to_csv(self.file, sep='\t')