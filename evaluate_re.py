import pandas as pd
import argparse
import os, sys
import pickle
import re
from utils.utils import clean_text 

parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description=__doc__)

parser.add_argument("--result_file", default = None, 
                    help="result tsv file with the extracted triples")
parser.add_argument("--compare_file", default = None, 
                    help="tsv file with the expected triples")
parser.add_argument("--out_name", default = None, 
                    help="name for the output files")

args = parser.parse_args()


if __name__ == '__main__':
    result_data = pd.read_csv(args.result_file, sep='\t', names=['text', 'date', 'url', 'confidence', 'subject', 'subject_uri', 'predicate', 'predicate_uri', 'object', 'object_uri'], header=0)
    compare_data = pd.read_csv(args.compare_file, sep='\t', names=['text', 'confidence', 'subject', 'subject_uri', 'predicate', 'predicate_uri', 'object', 'object_uri'], header=0)
    
    fp = []
    fn = []
    tp = []
    
    result_dict = {}
    all_compare_dict = {}
    compare_dict = {}
    
    for index, row in result_data.iterrows():
        key = (row["text"], row["subject_uri"], row["predicate_uri"], row["object_uri"])
        result_dict.setdefault(key, row)
            
    for index, row in compare_data.iterrows():
        key = (clean_text(row["text"]), row["subject_uri"], row["predicate_uri"], row["object_uri"])
        all_compare_dict.setdefault(key, row)
        
    for index, row in compare_data.iterrows():
        key = (clean_text(row["text"]), row["subject_uri"], row["predicate_uri"], row["object_uri"])
        compare_dict.setdefault(key, row)

    for key in result_dict.keys():
        if key in all_compare_dict:
            tp.append(list(result_dict[key]))
        else:
            fp.append(list(result_dict[key]))
            
    for key in compare_dict.keys():
        if key in result_dict:
            continue
        else:
            fn.append(list(compare_dict[key]))

            
    with open("output/fp_" + args.out_name + ".data", "wb") as file:
        pickle.dump(fp, file)

    with open("output/fn_" + args.out_name + ".data", "wb") as file:
        pickle.dump(fn, file)
        
    with open("output/tp_" + args.out_name + ".data", "wb") as file:
        pickle.dump(tp, file)
        
        
    false_positives = len(fp)
    false_negatives = len(fn)
    true_positives = len(tp)
    
    precision = true_positives / (true_positives + false_positives)
    recall = true_positives / (true_positives + false_negatives)
    f1_score = (2 * precision * recall) / (precision + recall) 
    
    print("Number of predicted triples: ", len(result_dict))
    print("Number of actual triples: ", len(compare_dict))
    print("Number of triples in both datasets (True Positives): ", true_positives)
    print("Number of triples only in result datasets (False Positives): ", false_positives)
    print("Number of triples only in compare datasets (False Negatives): ", false_negatives)
    print("Precision: ", precision)
    print("Recall: ", recall)
    print("F1-Score: ", f1_score)
    