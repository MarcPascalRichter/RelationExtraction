import pandas as pd

OUTPUT_PATH = '/output/'

WORTSCHATZ_PATH = OUTPUT_PATH + 'wortschatz/'

if __name__ == '__main__':    
    relations_file = 'gnd_relations.tsv'
    columns = ['gnd_subject_id', 'gnd_subject_type', 'wiki_subject_id', 'gnd_subject_name', 'gnd_predicate', 'gnd_object_id', 'wiki_object_id', 'gnd_object_name']
    relations_df = pd.read_csv(relations_file, sep='\t', names=columns, header=0)
    
    columns = ['text', 'date', 'url', 'confidence', 'subject', 'subject_uri', 'predicate', 'predicate_uri', 'object', 'object_uri']
    re_out_df_wiki = pd.DataFrame(columns=columns)
    for i in range(5):
        re_out_file_wiki = WORTSCHATZ_PATH + 'wikipedia_2018_out_2M_' + str(i) + '.tsv'
        df = pd.read_csv(re_out_file_wiki, sep='\t', names=columns, header=0)
        re_out_df_wiki = pd.concat([re_out_df_wiki, df], ignore_index=True)
        
    re_out_df_wiki['source'] = 'wikipedia'
            
    re_out_df_web = pd.DataFrame(columns=columns)
    for i in range(5):
        re_out_file_web = WORTSCHATZ_PATH + 'web_2019_out_2M_' + str(i) + '.tsv'
        df = pd.read_csv(re_out_file_web, sep='\t', names=columns, header=0)
        re_out_df_web = pd.concat([re_out_df_web, df], ignore_index=True)
        
    re_out_df_web['source'] = 'web'
        
    re_out_df_news = pd.DataFrame(columns=columns)
    for i in range(5):
        re_out_file_news = WORTSCHATZ_PATH + 'news_2021_out_2M_' + str(i) + '.tsv'
        df = pd.read_csv(re_out_file_news, sep='\t', names=columns, header=0)
        re_out_df_news = pd.concat([re_out_df_news, df], ignore_index=True)
        
    re_out_df_news['source'] = 'news'
    
    re_out_df = pd.concat([re_out_df_wiki, re_out_df_web, re_out_df_news], ignore_index=True)
    
    relations_df['merge'] = relations_df['wiki_subject_id'] + '_' + relations_df['wiki_object_id'].apply(lambda x: x[:4] if x.endswith('#dateTime') else x) + '_' + result_df['gnd_predicate'].apply(lambda x: 'go' if x == 'placeOfBirth' else ('gd' if x == 'dateOfBirth' else ('so' if x == 'placeOfDeath' else ('sd' if x == 'dateOfDeath' else 'other'))))
    re_out_df['merge'] = re_out_df['subject_uri'] + '_' + re_out_df['object_uri'].apply(lambda x: x[:4] if x.endswith('#dateTime') else x) + '_' + re_out_df['predicate'].apply(lambda x: 'go' if x == 'Geburtsort' else ('gd' if x == 'Geburtsdatum' else ('so' if x == 'Sterbeort' else ('sd' if x == 'Sterbedatum' else 'other'))))
    
    merged_df = pd.merge(relations_df, re_out_df, on='merge', how='inner')
    grouped_df = merged_df.groupby('merge').apply(lambda x: pd.Series({'subject': x['gnd_subject_name'].iloc[0], 'subject_id': x['gnd_subject_id'].iloc[0], 'predicate': x['gnd_predicate'].iloc[0], 'object': x['gnd_object_name'].iloc[0], 'object_id': x['gnd_object_id'].iloc[0], 'texts': x[['text', 'date', 'url', 'source', 'confidence']].values.tolist()})).reset_index()
    
    grouped_df.drop('merge', axis=1, inplace=True)
    grouped_df.to_csv(WORTSCHATZ_PATH + 'gnd_relations_with_texts.tsv', sep='\t')
    