#!/usr/bin/env python
########
# Continuously insert records into a MongoDB collection, with random data
#
# Pre-requisite: > sudo pip install pymongo
#
########
import sys, random, datetime, time, pymongo

connection = pymongo.MongoClient('localhost', 27000)
db = connection['testdb']
count = 1
print
print ('Running'),

while True:
    db.records.insert({ 
        'date ': datetime.datetime.utcnow(),
        'count': count,
        'value': random.randint(0, 9999)
    });

    if (count % 5) == 0:
        print ('.'),

    sys.stdout.flush()
    count += 1
    time.sleep(0.1)

