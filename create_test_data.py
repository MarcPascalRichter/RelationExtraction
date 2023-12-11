import json
import pandas as pd
import argparse
import os, sys


DATASETS_PATH = '/datasets/'
CROCODILE_OUTPUT_PATH = DATASETS_PATH + 'crocodile_out/'


parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description=__doc__)

parser.add_argument("--size", default = 50000, 
                    help="size of test dataset")

args = parser.parse_args()
    
if __name__ == '__main__':

    doc_list = []
    trip_list = []
    count = 0

    in_file = open(CROCODILE_OUTPUT_PATH, 'r')

    while True and count < args.size:
        line = in_file.readline()
        if not line:
            break
        else:
            doc_json = json.loads(json.loads(line))
            doc_list.append([doc_json["docid"],doc_json["text"],"",doc_json["uri"]])
            for trip in doc_json["triples"]:
                trip_list.append([doc_json["text"], trip["confidence"], trip["subject"]["surfaceform"], trip["subject"]["uri"], trip["predicate"]["surfaceform"], trip["predicate"]["uri"], trip["object"]["surfaceform"], trip["object"]["uri"]])
            count += 1

    in_file.close()

    doc_df = pd.DataFrame(doc_list, columns=["id", "sentence", "date", "url"])
    doc_df.to_csv('test/test_' + str(args.size) + '.tsv', sep='\t')

    trip_df = pd.DataFrame(trip_list, columns=["text", "confidence", "subject", "subject_uri", "predicate", "predicate_uri", "object", "object_uri"])
    trip_df.to_csv('test/test_compare_' + str(args.size) + '.tsv', sep='\t')