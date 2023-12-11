from pipeline.data_classes import Text, Document, Entity, Triple
from utils.utils import remove_brackets
import transformers
import torch
import copy


class ConfidenceScoreCalculator:
    def __init__(self, model_name):
		'''
		Initialisierung des ConfidenceScoreCalculators
		
		:param model_name: Name des NLI-Modells
		'''
        self.tokenizer = transformers.AutoTokenizer.from_pretrained(model_name, use_auth_token=True)
        
        model_config = transformers.AutoConfig.from_pretrained(
            model_name,
            output_hidden_states=True,
            output_attentions=True,
            use_auth_token=True
        )
        
        self.model = transformers.AutoModelForSequenceClassification.from_pretrained(model_name, config = model_config, use_auth_token=True)
        self.model.cuda()
        self.model.eval()
        self.model.half()
    
    
    def run(self, input_data):
        '''
        Berechnet die Confidence Scores für alle Triples eines Document Objekts.

        :param input_data: Liste bestehend aus Document Objekt und Text Objekt
        :return: Das Document Objekt mit den Confidence Scores für die Triples
        '''

        doc = input_data[0]
        spacy_doc = input_data[1].doc_merged_entities

        if doc.status != 'okay -> triples found':
            return doc
        
        else:
            triples_list = []
            texts = indexes = [[] for _ in range(4)]
            sents = [str(s) for s in list(spacy_doc.sents)]
            sents_without_brackets = [remove_brackets(sent) for sent in sents]
            
            trip_new = []
            for triple in doc.triples:
                trip_new.append(triple)
                new_pred = None
                
				# Hinzufügen von alternativer Prädikate
                if triple.predicate.title == 'Land der Staatsangehörigkeit':
                    new_pred = 'Nationalität'
                elif triple.predicate.title == 'Tätigkeit':
                    new_pred = 'Beruf'
                elif triple.predicate.title == 'Geburtsort':
                    new_pred = 'geboren in'
                elif triple.predicate.title == 'Sterbeort':
                    new_pred = 'gestorben in'
                elif triple.predicate.title == 'Staat':
                    new_pred = 'Nation'
                
                if new_pred:
                    triple_copy = copy.deepcopy(triple)
                    triple_copy.predicate.surfaceform = new_pred
                    trip_new.append(triple_copy)
            
            doc.triples = trip_new

            for triple in doc.triples:
                # falls Triple durch Subjekt-Triple-Finder gefunden wurde, werden sowohl der erste Satz, 
				# als auch der aktuelle Satz geprüft, sonst nur der aktuelle Satz
                if triple.annotator == 'Subject-Triple-aligner':
                    text1 = sents[0] + ' ' + sents[triple.sentence_id]
                    text2 = sents_without_brackets[0] + ' ' + sents_without_brackets[triple.sentence_id]
                else:
                    text1 = sents[triple.sentence_id]
                    text2 = sents_without_brackets[triple.sentence_id]

                if triple.subject.boundaries is not None and triple.object.boundaries is not None:
                    triples_list.append(triple)
                    subject_text = triple.subject.title if 'expanded from' in triple.subject.annotator and not 'expanded from longer entity' in triple.subject.annotator else triple.subject.surfaceform
                    object_text = triple.object.title if 'expanded from' in triple.object.annotator and not 'expanded from longer entity' in triple.object.annotator else triple.object.surfaceform
					
					# erstelle 4 verschiedene Subjekt-Objekt-Text Kombinationen
                    texts[0].append(self.prepare_triple(subject_text, object_text, text1, triple.predicate.surfaceform))
                    texts[1].append(self.prepare_triple(subject_text, object_text, text2, triple.predicate.surfaceform))
                    texts[2].append(self.prepare_triple(triple.subject.title, triple.object.title, text1, triple.predicate.surfaceform))
                    texts[3].append(self.prepare_triple(triple.subject.title, triple.object.title, text2, triple.predicate.surfaceform))

            if len(texts) > 0:
                for i in range(len(indexes)):
                    indexes[i] = self.filter_triples(texts[i])
                final_indexes = list(range(len(indexes[0])))
				
				# übernehme jeweils den höchsten der 4 Scores
                for i in range(len(indexes[0])):
                    final_indexes[i] = max(indexes[0][i], indexes[1][i], indexes[2][i], indexes[3][i])
					
                for pred, trip in zip(final_indexes, triples_list):        
                    trip.confidence = pred.item()
					
                doc.triples = triples_list
                doc.status = 'okay -> triples found'
                
            else:
                doc.triples = []
                doc.status = 'no triples found'

            return doc
        

    def filter_triples(self, texts):
        if max([len(text) for text in texts])>256:
            range_length = 12
        else:
            range_length = 64
        result = []
        for batch in range(0,len(texts),range_length):
            encoded_input = self.tokenizer(
                    [ex[0] for ex in texts[batch: batch + range_length]], [ex[1] for ex in texts[batch: batch + range_length]],
                    return_tensors='pt',
                    add_special_tokens=True,
                    max_length=256,
                    padding='longest',
                    return_token_type_ids=False,
                    truncation='only_first')
            for tensor in encoded_input:
                encoded_input[tensor] = encoded_input[tensor].cuda()
            with torch.no_grad():
                outputs = self.model(**encoded_input, return_dict=True, output_attentions=False, output_hidden_states = False)
            result.append(outputs['logits'].softmax(dim=1))
            del outputs
        logits = torch.cat(result)
        value = next((value for dict_key, value in self.model.config.label2id.items() if dict_key.lower() == 'entailment'), None)
        return logits[:,value]


    def prepare_triple(self, subject_text, object_text, article_text, predicate_text):
        if len(article_text) > 0 and article_text.strip('\n')[-1].isalnum():
            return (article_text.strip('\n') + '.', ' '.join([subject_text, predicate_text, object_text]))
        else:
            return (article_text.strip('\n')[:-1] + '.', ' '.join([subject_text, predicate_text, object_text]))
