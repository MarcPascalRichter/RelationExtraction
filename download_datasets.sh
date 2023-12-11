#!/usr/bin/env bash

mkdir datasets/wiki
mkdir datasets/gnd

echo "Download Wikipedia und Wikidata Dumps"
wikimapper download dewiki-latest --dir datasets/wiki

echo "Wikidata DB erstellen"
wikimapper create dewiki-latest --dumpdir datasets/wiki/ --target datasets/wiki/index_dewiki-latest.db

echo "Download Wikidata Triples"
wget -P datasets/wiki/ https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2

echo "Make csv file out of nt .."
bzcat datasets/wiki/latest-truthy.nt.bz2 | grep "/prop/direct/P" | sed -E 's/[<>"]//g'| sed -E 's/@.+//g' | cut -d" " -f1-3 | sed -E 's/\s/\t/g' |  sed -e 's/\(http:\/\/www.wikidata.org\/prop\/direct\/\|http:\/\/www.wikidata.org\/entity\/\)//g' > datasets/wiki/wikidata-triples.csv

echo "Download GND Data Dump"
wget -P datasets/gnd/ https://data.dnb.de/opendata/authorities-gnd_entityfacts.jsonld.gz

echo "Extract GND jsonld file"
gunzip -k "datasets/gnd/authorities-gnd_entityfacts.jsonld.gz"

echo "Split GND file in 5 files"
split -n 5 datasets/gnd/authorities-gnd_entityfacts.jsonld -d datasets/gnd/gnd

echo "Rename the 5 GND files"
mv datasets/gnd/gnd00 datasets/gnd/gnd0.jsonld
mv datasets/gnd/gnd01 datasets/gnd/gnd1.jsonld
mv datasets/gnd/gnd02 datasets/gnd/gnd2.jsonld
mv datasets/gnd/gnd03 datasets/gnd/gnd3.jsonld
mv datasets/gnd/gnd04 datasets/gnd/gnd4.jsonld