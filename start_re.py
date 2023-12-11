from utils.reader import *
from utils.writer import *
from utils.triple_reader import *
from pipeline.data_classes import *
from pipeline.mapper_classes import *
from pipeline.triple_finder import *
from pipeline.ner import *
from pipeline.conf_score_calculator import *
from pipeline.triple_filter import *
import pickle
import logging
import pandas as pd
import argparse
import os, sys
import swifter
from timeit import default_timer
from huggingface_hub import login
from tqdm import tqdm

from utils.utils import clean_text, remove_brackets, calculate_sentences_boundaries

tqdm.pandas()
login(token='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')

parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description=__doc__)

parser.add_argument('--input_file', default = None, 
                    help = 'tsv file with sentences')
parser.add_argument('--input_with_id', default = 'no', 
                    help = 'yes when the rows in the input_file already have ids')
parser.add_argument('--row_number', default = None, 
                    help = 'max number of records to process')
parser.add_argument('--output_file', default = None,
                    help = 'file to save the triples')
parser.add_argument('--enter_text', default = 'no', 
                    help = 'yes when entering and processing single sentences')
parser.add_argument('--confidence_score', default = '0.97', 
                    help = 'min. score to evaluate a triple as confident')
parser.add_argument('--pre', default = None, 
                    help = 'preprocessed file')
 
args = parser.parse_args()

SPACY_MODEL =                     'de_core_news_lg'
DATASETS_PATH =                   '/datasets/'
OUTPUT_PATH =					  '/output/'

subclass_relations_file =         DATASETS_PATH + 'subclass_relations.data'
wikidata_relations_file =         DATASETS_PATH + 'wikidata_relations.data'
title_ids_expanded_mapping_file = DATASETS_PATH + 'title_ids_expanded_mapping.data'
title_ids_mapping_file =          DATASETS_PATH + 'title_ids_mapping.data'
country_adj_mapping_file =        DATASETS_PATH + 'country_adj_mapping.tsv'
country_nation_mapping_file =     DATASETS_PATH + 'country_nation_mapping.tsv'
model_name =                      'joeddav/xlm-roberta-large-xnli'

trip_finder = nlp = nlp_merged_entities = conf_score_calc = ner = trip_filter = None


nlp = spacy.load(SPACY_MODEL)
nlp.add_pipe('dbpedia_spotlight')
nlp_merged_entities = spacy.load(SPACY_MODEL)
nlp_merged_entities.add_pipe('merge_entities')


def prepare(wikidata_relations_file, title_ids_mapping_file, title_ids_expanded_mapping_file, subclass_relations_file, country_adj_mapping_file, country_nation_mapping_file, model_name, conf_score):
    '''
    Bereite alle notwendigen Daten und Variablen vor.
    
    :param wikidata_relations_file: Die Datei mit den Triples
    :param title_ids_mapping_file: Die Datei mit dem Mapping Titel-ID
    :param title_ids_expanded_mapping_file: Die Datei mit dem erweiterten Mapping Titel-ID
    :param subclass_relations_file: Die Datei mit den Subclass Relations
    :param country_adj_mapping_file: Die Datei mit dem Mapping Country-Adjektive
    :param country_nation_mapping_file: Die Datei mit dem Mapping Country-Nationen
    :param model_name: Der Name des NLI-Modells
	:param conf_score: Der Threshold für den Confidence-Score 
    :return: Die für die Pipeline benötigten Daten und Variablen
    '''
    wiki_mapper = WikiMapper(title_ids_mapping_file, title_ids_expanded_mapping_file, subclass_relations_file)
    country_mapper = CountryMapper(country_adj_mapping_file, country_nation_mapping_file)
    
    triple_reader = TripleReader(wikidata_relations_file, 'de')
    
    ner = NER(wiki_mapper, country_mapper, triple_reader)
    trip_finder = TripleFinder(triple_reader)
    conf_score_calc = ConfidenceScoreCalculator(model_name)
    trip_filter = TripleFilter(conf_score)
        
    return ner, trip_finder, conf_score_calc, trip_filter



if __name__ == '__main__':
    
    format = '%(asctime)s: %(message)s'
    logging.basicConfig(format=format, level=logging.INFO, datefmt='%H:%M:%S')
    
    logging.info('Start preparation')
    start_time = default_timer()   
    ner, trip_finder, conf_score_calc, trip_filter = prepare(wikidata_relations_file, title_ids_mapping_file, title_ids_expanded_mapping_file, subclass_relations_file, country_adj_mapping_file, country_nation_mapping_file, model_name, args.confidence_score)
    logging.info(f'Preparation finished in {(default_timer() - start_time)}')
    
    if args.enter_text == 'yes':
        while True:
            sentence = input('Enter example sentence or text: ')
            try:                
                clean_text = clean_text(sentence)
                nlp_me_obj = nlp_merged_entities(clean_text)
                sentences_boundaries = calculate_sentences_boundaries(nlp_me_obj)
                nlp_obj = nlp(clean_text)
                data = pd.DataFrame({'text': Text(sentence, clean_text, nlp_obj, nlp_me_obj, sentences_boundaries)}, index=[0])
                
                data['document'] = [ner.run([0, '', '', data.values.tolist()[0][0]])]
                data['document_with_triples'] = trip_finder.run(data.values.tolist()[0][1])
                data['filtered_document'] = trip_filter.run(data.values.tolist()[0][2])
                data['document_with_confidences'] = conf_score_calc.run([data.values.tolist()[0][3], data.values.tolist()[0][0]])
                data['filtered_document_2'] = trip_filter.run_confidence_filter(data.values.tolist()[0][4])

                document = data.values.tolist()[0][5]
				
                print()
                print('Whole document:')
                print(document.toJSON())
                print()
                print('Status: ', document.status)
                print('Entities:')
                for ent in document.entities:
                    print('   ', ent.title + '\t' + ent.uri)
                print()
                print('Expanded entities:')
                for ent in document.expanded_entities:
                    print('   ', ent.title + '\t' + ent.uri)
                    
                if document.status == 'okay -> triples found':
                    document.triples.sort(key=lambda x: x.confidence, reverse=True)
                    print('Triples:')
                    for trip in document.triples:
                        print('   ' + trip.subject.title + ' ' + trip.subject.uri + '   ' + trip.predicate.title + '   ' + trip.object.title + ' ' + trip.object.uri)
						print('   Confidence: ' + str(trip.confidence) + ' -> confident')
                print()
            except Exception as error:
                logging.info(error)
                
    else:    
        if os.path.exists(DATASETS_PATH + args.pre):
            with open(DATASETS_PATH + args.pre,"rb") as file:
                data = pickle.load(file)

        else:
            interval_start = default_timer()
            logging.info('Reading tsv input file')
            reader = Reader(DATASETS_PATH + args.input_file, args.input_with_id)
            data = reader.run(args.row_number, False)
            logging.info(f'Finished in {(default_timer() - interval_start)}')
			
			interval_start = default_timer()
            logging.info('Creating Text objects')            
            data["clean_text"] = data["sentence"].swifter.apply(clean_text)
            docs = []
			docs_merged_entities = []
            sentences_boundaries = []
			for doc in tqdm(nlp.pipe(data["clean_text"].tolist(), n_process=16), total=len(data["clean_text"].tolist())):
                docs.append(doc)
            for doc in tqdm(nlp_merged_entities.pipe(data["clean_text"].tolist(), n_process=16), total=len(data["clean_text"].tolist())):
                docs_merged_entities.append(doc)
                sentences_boundaries.append(calculate_sentences_boundaries(doc))
            data['text'] = [Text(text, clean_text, doc, doc_merged_entities, sentence_boundaries) for text, clean_text, doc, doc_merged_entities, sentence_boundaries in zip(data['sentence'], data['clean_text'], docs, docs_merged_entities, sentences_boundaries)]
            logging.info(f'Finished in {(default_timer() - interval_start)}')
            
            with open(DATASETS_PATH + args.pre, "wb") as f:
                pickle.dump(data, f)
        
        start_time = default_timer()
        
        interval_start = default_timer()
        logging.info('NER')
        data["document"] = data[['id', 'date', 'url', 'text']].swifter.apply(ner.run, axis=1)            
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        with open(DATASETS_PATH + "disambiguation_dict.data", "wb") as f:
            pickle.dump(ner.disambiguation_dict, f)

        logging.info('Finding triples for documents')
        interval_start = default_timer()
        data['document_with_triples'] = data.progress_apply(lambda row: trip_finder.run(row['document']), axis=1)
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        logging.info('Filtering triples')
        interval_start = default_timer()
        data['filtered_document'] = data.progress_apply(lambda row: trip_filter.run(row['document_with_triples']), axis=1)
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        logging.info('Calculating confidences of triples')
        interval_start = default_timer()
        data["document_with_confidences"] = data[['filtered_document', 'text']].swifter.apply(conf_score_calc.run, axis=1)
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        logging.info('Confidence filtering triples')
        interval_start = default_timer()
        data['filtered_document_2'] = data.progress_apply(lambda row: trip_filter.run_confidence_filter(row['document_with_confidences']), axis=1)
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        logging.info('Writing triples to file')
        interval_start = default_timer()
        writer = Writer(OUTPUT_PATH + args.output_file)
        writer.run(data)
        logging.info(f'Finished in {(default_timer() - interval_start)}')
        
        logging.info(f'Complete relation extraction (w/o creating Text objects) finished in {(default_timer() - start_time)}')
        