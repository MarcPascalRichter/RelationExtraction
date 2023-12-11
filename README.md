# Relationsextraktion
Die innerhalb der Masterarbeit ermittelten Ergebnisse mit dem Projekt *Wortschatz Leipzig* zur Erweiterung der *GND* (Deutsche Nationalbibliothek) sind in Datei `output/wortschatz/gnd_relations_with_texts.tsv` zu finden

Zum Training der REBEL-Komponente muss das Projekt `REBEL` (https://github.com/Babelscape/rebel) ausgeführt werden, zum Erstellen eines eigenen Datensatzes für Relationsextrktion muss das Projekt `cRocoDiLe` (https://github.com/Babelscape/crocodile) ausgeführt werden

## Ausführen von Relationsextraktion
1. Ausführen von `download_datasets.sh`, um alle benötigten Daten von Wikidata und GND herunterzuladen
2. Ausführen von `create_pipeline_data.py`, um für Pipeline benötigte Daten in korrektem Format zu erstellen
3. Ausführen von `start_re.py`, um Pipeline zu starten (siehe Code für benötigte Arguments)

## Abgleich von extrahierten Triples mit GND-Relationen
1. Auführen von `create_gnd_data.py`, um GND-Relationen zu erstellen
2. Ausführen von `match_gnd_re_results.py` (Dateipfade der Input-Dateien müssen im Code angepasst werden)

## Testen der Pipeline
1. Ausführen von `create_test_data.py` zum Erstellen eines eigenen Datensatzes für Relationsextraktion
2. Ausführen von `evaluate_re.py`, um Pipeline mit selbst erstelltem Datensatz zu testen (siehe Code für benötigte Arguments)
