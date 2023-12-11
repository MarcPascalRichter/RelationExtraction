from sklearn.utils import shuffle
from utils.utils import read_tsv


class Reader:
    def __init__(self, file, input_with_id):
		'''
		Initialisierung eines Reader Objekts
		
		:param file: Dateiname
		:param input_with_id: Flag, der angibt, ob es eine ID Spalte gibt
		'''
        self.file = file
        self.input_with_id = input_with_id
        self.column_names = ['id', 'sentence', 'date', 'url'] if input_with_id == 'yes' else ['sentence', 'date', 'url']

    
    def run(self, number=None, shuffle=False):
        '''
        Liest eine TSV-Datei ein und gibt die Daten als Pandas DataFrame zurück.

        :param number: Die Anzahl der Zeilen, die eingelesen werden sollen (optional)
		:param shuffle: Flag, das angibt, ob die Daten gemischt werden sollen (optional)
        :return: Ein Pandas DataFrame mit den eingelesenen Daten
        '''
        try:
            data = read_tsv(self.file, self.column_names)

            if number is not None:
                if shuffle:
                    data = shuffle(data)
                    data.reset_index(inplace=True, drop=True)
                data = data.iloc[:int(number)]
                
            if self.input_with_id == 'no':
                data.insert(0, 'id', range(0, 0 + len(data)))
                
            return data

        except Exception as e:
            raise Exception('Exception while reading TSV file:', e)
            