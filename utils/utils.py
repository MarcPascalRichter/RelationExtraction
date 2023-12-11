import re
import pandas as pd


def read_tsv(file, column_names):
	'''
	Liest eine TSV Datei ein
	
	:param file: Dateiname
	:param column_names: Liste mit Namen der Spalten
	:return: Dataframe mit Inhalt der Datei
	'''
    return pd.read_csv(file, sep='\t', names=column_names, header=0)


def clean_text(text):
    """
    Bereinigt einen Text (Entfernen von Zeilenumbrüchen und mehreren hintereinander stehenden Leerzeichen).
    
    :param text: Der zu bereinigende Text
    :return: Der bereinigte Text
    """
    text = text.replace(';',',').replace('\n',' ').replace('<br>',' ')
    if text.endswith(' }}'):
        text = text.replace(' }}', '')
    return re.sub(r'\s+', ' ', text)


def remove_brackets(text):
	'''
	Entfernt alle Klammern inkl. Inhalt aus einem Text
	
	:param text: Der zu bereinigende Text
    :return: Der bereinigte Text 
	'''
    return re.sub(r'\([^()]*\)', '', text)


def text_in_brackets(text, boundaries):
	'''
	Berechnet die Nummer der Klammer, in der ein Text steht
	
	:param text: Der Text, in dem gesucht wird
	:param boundaries: Start- und Endindex des Textes, nach dem gesucht wird
    :return: Die Nummer der Klammer, -1 falls es in keiner Klammer steht 
	'''
    end = boundaries[1]
    bracket_count = 0
    bracket = -1

    if text.count('(') > 0 and text.count('(') == text.count(')'):
        for i in range(len(text)):
            if i == end + 1:
                return bracket
            if text[i] == "(":
                bracket_count += 1
                bracket = bracket_count
            elif text[i] == ")":
                bracket = -1
    return -1
    
    
def calculate_sentences_boundaries(doc):
    """
    Berechnet die Satzgrenzen eines Textes
    
    :param doc: Spacy Objekt des Textes
    :return: Liste mit allen Satzgrenzen, jeder Satz wird in der Liste durch ein Tupel mit (start, end) repräsentiert
    """
    token_list = []
    for sent_i, sent in enumerate(doc.sents):
        for token in sent:
            token_list.append([sent_i, token.idx, token.idx + len(token.text) - 1])
            
    token_df = pd.DataFrame(token_list, columns=['sent_id', 'start', 'end'])
    sentences_boundaries_list = token_df.groupby('sent_id').agg({'start': 'min', 'end': 'max'}).values.tolist()

    sentences_boundaries = [(start, end + 1) for start, end in sentences_boundaries_list]
    return sentences_boundaries
	