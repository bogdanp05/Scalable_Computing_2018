import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
#CSV to JSON Conversion
csvfile = open('./Project/src/main/resources/csv/mao2000.csv', 'r')
reader = csv.DictReader(csvfile)
mongo_client = MongoClient()
# db = mongo_client.music_data
db = mongo_client.music_data2

db.triplets2.drop()

header = [ "user", "song", "count"]

for each in reader:
    row = {}
    for field in header:
        row[field] = each[field]

    db.triplets2.insert(row)