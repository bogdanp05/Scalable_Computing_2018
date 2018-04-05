import csv
import sys
from pymongo import MongoClient

csv.field_size_limit(sys.maxsize)
# csvfile = open('./Project/src/main/resources/csv/train_triplets.csv', 'r')
# csvfile = open('./Project/src/main/resources/csv/mao200k.csv', 'r')
csvfile = open('./Project/src/main/resources/csv/lastfm.csv', 'r')
reader = csv.DictReader(csvfile)
mongo_client = MongoClient()
db = mongo_client.music_data
# db = mongo_client.music_data2

db.triplets.drop()

header = ["user", "song", "count"]

# insert to mongodb in bulk
counter = 0
bulk_number = 8192
bulk = []
for each in reader:
    if len(each["user"]) < 13 and len(each["song"]) < 60 and len(each["count"]) < 4:
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
    else:
        print(len(each["user"]))
        print(len(each["song"]))
        print(len(each["count"]))

db.triplets.insert_many(bulk)
bulk = []
print(counter)
