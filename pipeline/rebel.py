from thefuzz import fuzz
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import spacy
from spacy.language import Language
from spacy.tokens import Doc
import hashlib


Doc.set_extension("rebel_extraction", default=[], force=True)

tokenizer = AutoTokenizer.from_pretrained("/rebel_models/rebel_facebook_mbart")
model = AutoModelForSeq2SeqLM.from_pretrained("/rebel_models/rebel_facebook_mbart")

gen_kwargs = {
    "max_length": 256,
    "length_penalty": 0,
    "num_beams": 3,
    "num_return_sequences": 3,
}


@Language.component("rebel_extraction")
def rebel_extraction(doc):
	'''
	Anwendung von REBEL innerhalb der Spacy-Pipeline
	
	:param doc: Document Objekt
	:return: Document Objekt inkl. REBEL Extraktionen
	'''
    model_inputs = tokenizer(doc.text, max_length=256, padding=True, truncation=True, return_tensors = 'pt')

    generated_tokens = model.generate(
        model_inputs["input_ids"].to(model.device),
        attention_mask=model_inputs["attention_mask"].to(model.device),
        **gen_kwargs,
    )

    decoded_preds = tokenizer.batch_decode(generated_tokens, skip_special_tokens=False)

    triples = set()
    
    for idx, sentence in enumerate(decoded_preds):
        for trip in extract_triples(sentence):
            triples.add(trip)

    doc._.rebel_extraction = list(triples)
    return doc
	
	
def extract_triples(text):
	'''
	Extraktion von Triples aus einem REBEL-Text
	
	:param doc: REBEL-Text
	:return: Liste von Triples 
	'''
    triples = []
    relation, subject, relation, object_ = '', '', '', ''
    text = text.strip()
    current = 'x'
    for token in text.replace("<s>", "").replace("<pad>", "").replace("</s>", "").split():
        if token == "<triplet>":
            current = 't'
            if relation != '':
                triples.append(' '.join([subject.strip(), relation.strip(), object_.strip()]))
                relation = ''
            subject = ''
        elif token == "<subj>":
            current = 's'
            if relation != '':
                triples.append(' '.join([subject.strip(), relation.strip(), object_.strip()]))
            object_ = ''
        elif token == "<obj>":
            current = 'o'
            relation = ''
        else:
            if current == 't':
                subject += ' ' + token
            elif current == 's':
                object_ += ' ' + token
            elif current == 'o':
                relation += ' ' + token
    if subject != '' and relation != '' and object_ != '':
        triples.append(' '.join([subject.strip(), relation.strip(), object_.strip()]))
    return triples
	
	
class Rebel:         
    def __init__(self):
		'''
		Initialisierung des REBEL Objekts
		'''
		self.rebel_dict = {}        
        self.nlp_rebel = spacy.load('de_core_news_lg')
        self.nlp_rebel.add_pipe('rebel_extraction')
	
	
	def filter(self, triple, doc):
		'''
		REBEL-Filterung und Prüfung, ob Triple in REBEL-Extraktionen enthalten ist
		
		:param triple: Triple, das geprüft werden soll
		:param doc: Document Objekt
		:return: True wenn Triple in REBEL-Extraktion enthalten ist, False falls nicht
		'''
		# prüfen, ob Text bereits mit REBEL verarbeitet wurde, falls ja, können die Triples aus dict geladen werden
        hash_value = hashlib.sha256(doc.text.clean_text.encode('utf-8')).hexdigest()
        if hash_value in self.rebel_dict:
            rebel_extraction = self.rebel_dict[hash_value]
        else:
            d = self.nlp_rebel(doc.text.clean_text)
            rebel_extraction = d._.rebel_extraction
            self.rebel_dict[hash_value] = rebel_extraction
			
        triple_text = ' '.join([triple.subject.surfaceform, triple.predicate.title, triple.object.surfaceform])
        for rebel_ext in rebel_extraction:
            similarity = fuzz.token_sort_ratio(triple_text, rebel_ext)
            if similarity >= 95:
                return True
        return False
		