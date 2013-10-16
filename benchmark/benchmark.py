# To run this, install the following packages
#   pip install pymongo
#   pip install numpy
#
# Change the URL of the MongoClient connection 'DBURL'

import uuid
import time
import numpy
from pymongo import MongoClient

DBURL = 'mongodb://mongohost:27017/'
REPS  = 10

client = MongoClient(DBURL)

# Benchmark how fast we can create new collections. 
# 
# An important note about collections (and databases) in MongoDB is that they
# are created lazily - none of the above commands have actually performed any
# operations on the MongoDB server. Collections and databases are created when
# the first document is inserted into them.

create_coll_timings = list()
db = client.perftest
for c in range(0, 1024):
    t1 = time.time()
    collection = db["%s.workunits" % uuid.uuid1()]
    collection.insert({'val': None })
    td = time.time() - t1
    collection.drop()
    create_coll_timings.append(td)

print "\nResults for %s: " % DBURL
print "======================================================================"
print " Average time to create a collection: %f sec." % numpy.mean(create_coll_timings)
print ""



