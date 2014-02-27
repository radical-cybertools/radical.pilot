# To run this, install the following packages
#   pip install pymongo
#   pip install numpy
#
# Change the URL of the MongoClient connection 'DBURL'

import uuid
import time
import numpy
from copy import deepcopy
from pymongo import MongoClient

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("SAGAPILOT_DBURL")
if DBURL is None:
    print "ERROR: SAGAPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

wu = {
        "_UnitManagerID": "manager.objectID()",
        "_PilotID:": "pilot.objectID()",

        "Name": "BSON.STRING",
        "Description": {
            "Name": "BSON.STRING",
            "Executable": "BSON.STRING",
            "Arguments": ["BSON.STRING", "BSON.STRING", "..."],
            "Environment": "BSON.STRING",
            "StartTime": "BSON.STRING",
            "RunTime": "BSON.STRING",
            "WorkingDirectory": "BSON.STRING",
            "Input": "BSON.STRING",
            "Output": "BSON.STRING",
            "Error": "BSON.STRING",
            "Slots": "BSON.STRING"
        },
        "Info": {
            "State": "BSON.STRING",
            "SubmitTime": "BSON.Date",
            "StartTime": "BSON.Date",
            "EndTime": "BSON.Date" 
        }
    }

client = MongoClient(DBURL)

################################################################################
# Benchmark to see how fast we can create new collections. 
# 
# An important note about collections (and databases) in MongoDB is that they
# are created lazily - none of the above commands have actually performed any
# operations on the MongoDB server. Collections and databases are created when
# the first document is inserted into them.
#
print "\nResults for %s: " % DBURL
print "======================================================================"

create_coll_timings = list()
db = client.perftest
for c in range(0, 1024):
    t1 = time.time()
    collection = db["%s.workunits" % uuid.uuid1()]
    collection.insert({'val': None })
    td = time.time() - t1
    collection.drop()
    create_coll_timings.append(td)
print " Average time to create a collection: %f sec." % numpy.mean(create_coll_timings)

################################################################################
# Benchmark how fast we can create new documents.
#
# Documents can be work units submitted to a pilot manager for example.
#
db = client.perftest
collection = db.test.workunits


for i in [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384]:
    create_doc_timings = list()
    for d in range(0, 10):
        # create a list that contains 'i' entries
        insert = list()
        for x in range(0, i):
            insert.append(deepcopy(wu))


        t1 = time.time()
        collection.insert(insert)
        td = time.time() -t1
        create_doc_timings.append(td)
    collection.drop()
    print "Average time to add documents in bulks of %i: %f sec." % (i, numpy.mean(create_doc_timings))

print ""



