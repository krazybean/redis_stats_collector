#!/usr/bin/python
import os
import requests
import json
import bson
import argparse
from bson.json_util import dumps, loads
from datetime import datetime
from pymongo import MongoClient, Connection
from pprint import pprint

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

parser = argparse.ArgumentParser(description="""
    parse_redskull.py used to parse redskull stats and ship them to mongo
    """)
parser.add_argument('--purge',
                    action='store_true',
                    dest='purge',
                    help='--purge: removes all mongo documents in collections')
parser.add_argument('--run',
                    action='store_true',
                    dest='run',
                    help='--run: initiates the script to save to mongo')
parser.add_argument('--query',
                    action='store_true',
                    dest='query',
                    help='--query: queries 1 record for testing the shape')
options = parser.parse_args()
purge, run, query = options.purge, options.run, options.query


redskull = 'http://cdbp-n01.prod.iad3.clouddb.rackspace.net:8000/api/knownpods'

rs_data = requests.get(redskull)
rsdata = json.loads(rs_data.content)

try:
    mongo_conn = os.environ['MONGO_CONN']
except KeyError:
    raise Exception("ConfigReadError")

connection = Connection(mongo_conn)
db = connection['test_stats']

testing_collection = 'stats_test'

if run:
    MAX = 3
    for data in rsdata['Data'][:MAX]:
        try:
            body = {}
            db.testing_collection.ensure_index("Date", expireAfterSeconds=3*60)
            body[data['Name']] = {}
            body[data['Name']]['Date'] = datetime.utcnow()
            body[data['Name']]['Client'] = data['Master']['Info']['Client']
            body[data['Name']]['Memory'] = data['Master']['Info']['Memory']
            body[data['Name']]['Stats'] = data['Master']['Info']['Stats']
            body[data['Name']]['LatencyHistory'] = \
                data['Master']['LatencyHistory']
            body[data['Name']]['Persistence'] = \
                data['Master']['Info']['Persistence']
            body[data['Name']]['Commandstats'] = \
                data['Master']['Info']['Commandstats']
            db.testing_collection.insert(body)
            pprint(body)
        except TypeError:
            pass
if purge:
    # Flush out all the things:
    print db.testing_collection.remove()
    print "***** Flushed all records *****"
if query:
    queryset = db.testing_collection.find_one()
    for result in queryset:
        if result != '_id':
            pprint(dumps(queryset[result]))
