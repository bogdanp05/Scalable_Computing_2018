import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
#CSV to JSON Conversion
# csvfile = open('./Project/src/main/resources/csv/train_triplets.csv', 'r')
csvfile = open('./Project/src/main/resources/csv/mao20k.csv', 'r')
reader = csv.DictReader(csvfile)
mongo_client = MongoClient()
# db = mongo_client.music_data
db = mongo_client.music_data2

db.triplets.drop()

header = ["user", "song", "count"]

# insert to mongodb in bulk
counter = 0
bulk_number = 8192
bulk = []
for each in reader:
    row = {}
    for field in header:
        row[field] = each[field]

    # db.triplets.insert(row)
    bulk.append(row)
    counter += 1

    if counter % bulk_number == 0:
        db.triplets.insert_many(bulk)
        bulk = []
        print(counter)

db.triplets.insert_many(bulk)
bulk = []
print(counter)
