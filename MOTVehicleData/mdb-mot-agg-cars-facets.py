#!/usr/bin/env python
#
# Performs a multi-faceted set of aggregations against the 'MOT' UK annual
# vehicle test result data, displaying summary statistics for various
# dimensions. The MOT data sset should first have been downloaded
# https://data.gov.uk/dataset/anonymised_mot_test) and then loaded into a
# MongoDB database called 'mot' in a collection called 'testresults', using
# the Python script 'mdb-mot-import-csv.py' (provided in the same directory as
# this script). The aggregation typically takes a few minutes to complete and
# display the results.
#
# An MOT is a UK annual check on a vehicle, and is mandatory for all cars over
# 3 years old, see: https://en.wikipedia.org/wiki/MOT_test
#
# Suggested index creation command, to run first, in the Mongo Shell:
#   > use mot
#   > db.testresults.ensureIndex({Make: 1, Model: 1})
#
# Usage (ensure py script is executable):
#   $ ./mdb-mot-agg-cars-facets.py
#
# Output:
# * Facet: CategorisedCarsByFuelType
# * Facet: BucketedCarMakesByAmountOfUniqueModels
# (see base of this file for an example of the aggregation output)
#
# Prerequisite:
# * Install PyMongo driver, eg:
#   $ sudo pip install pymongo
##
from datetime import datetime
from pymongo import MongoClient
from bson.son import SON
from pprint import pprint


# Constants
MONGODB_URL = 'mongodb://localhost:27017/'
SHOW_EXPLAIN_PLAN = False


####
# Perform a MongoDB Aggregation on the MOT Vehicle records.
####
def mot_vehicle_aggregate():
    client = MongoClient(MONGODB_URL)
    db = client.mot
    print 'Aggregation starting {0}'.format(datetime.now())
    facet1 = get_pipeline_categorised_cars_by_fuel_type()
    facet2 = get_pipeline_bucketed_car_makes_by_amount_of_unique_models()
    pipeline = [
        {'$facet': {
            'CategorisedCarsByFuelType': facet1,
            'BucketedCarMakesByAmountOfUniqueModels': facet2,
        }},
    ]

    if (SHOW_EXPLAIN_PLAN):
        result = db.command('aggregate', db.testresults.name,
                            pipeline=pipeline, explain=True)
    else:
        result = list(db.testresults.aggregate(pipeline))

    pprint(result)
    print 'Aggregation finished {0}'.format(datetime.now())
    db.close


####
# Aggregation pipeline for: CategorisedCarsByFuelType
####
def get_pipeline_categorised_cars_by_fuel_type():
    return [
        {'$group': {
            '_id': '$FuelType',
            'CarAmount': {'$sum': 1},
        }},
        {'$sort': SON([('CarAmount', -1)])},
        {'$project': {
            '_id': 0,
            'FuelType': '$_id',
            'CarAmount': 1
        }},
    ]


####
# Aggregation pipeline for: BucketedCarMakesByAmountOfUniqueModels
#
# Add following to bucket-Auto/output below to include list of Makes in the
# output:
#  'Makes': {'$push': {'Make': '$_id', 'ModelTypesCount': '$ModelTypes'}}
####
def get_pipeline_bucketed_car_makes_by_amount_of_unique_models():
    return [
        {'$match': {
            '$and': [
                {'Make': {'$ne': 'UNCLASSIFIED'}},
                {'Model': {'$ne': 'UNCLASSIFIED'}}
            ]
        }},
        {'$group': {
            '_id': {'Make': '$Make', 'Model': '$Model'},
        }},
        {'$group': {
            '_id': '$_id.Make',
            'ModelTypes': {'$sum': 1},
        }},
        {'$bucketAuto': {
            'groupBy': '$ModelTypes',
            'buckets': 20,
            'granularity': '1-2-5',
            'output': {
                'CarMakesInBucket': {'$sum': 1},
            }
        }},
        {'$project': {
            '_id': 0,
            'MinUniqueModels': '$_id.min',
            'MaxUniqueModels': '$_id.max',
            'CarMakesInBucket': 1
        }},
    ]


####
# Main
####
if __name__ == '__main__':
    mot_vehicle_aggregate()


"""
EXAMPLE OUTPUT:

[{u'BucketedCarMakesByAmountOfUniqueModels': [{u'CarMakesInBucket': 6731,
                                               u'MaxUniqueModels': 2.0,
                                               u'MinUniqueModels': 0.5},
                                              {u'CarMakesInBucket': 1438,
                                               u'MaxUniqueModels': 5.0,
                                               u'MinUniqueModels': 2.0},
                                              {u'CarMakesInBucket': 677,
                                               u'MaxUniqueModels': 20.0,
                                               u'MinUniqueModels': 5.0},
                                              {u'CarMakesInBucket': 411,
                                               u'MaxUniqueModels': 10000.0,
                                               u'MinUniqueModels': 20.0}],
  u'CategorisedCarsByFuelType': [{u'CarAmount': 67548883, u'FuelType': u'PE'},
                                 {u'CarAmount': 44613240, u'FuelType': u'DI'},
                                 {u'CarAmount': 157687, u'FuelType': u'EL'},
                                 {u'CarAmount': 144734, u'FuelType': u'HY'},
                                 {u'CarAmount': 109554, u'FuelType': u'OT'},
                                 {u'CarAmount': 91706, u'FuelType': u'LP'},
                                 {u'CarAmount': 4893, u'FuelType': u'ED'},
                                 {u'CarAmount': 3496, u'FuelType': u'GB'},
                                 {u'CarAmount': 2456, u'FuelType': u'FC'},
                                 {u'CarAmount': 508, u'FuelType': u'CN'},
                                 {u'CarAmount': 307, u'FuelType': u'GA'},
                                 {u'CarAmount': 241, u'FuelType': u'ST'},
                                 {u'CarAmount': 185, u'FuelType': u'LN'},
                                 {u'CarAmount': 45, u'FuelType': u'GD'},
                                 {u'CarAmount': 6, u'FuelType': u''}]}]
"""
