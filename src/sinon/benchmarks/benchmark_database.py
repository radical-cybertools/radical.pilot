#!/usr/bin/env python
# encoding: utf-8

"""Database conncetion layer benchmarks
"""

import sys
import time
import numpy
import uuid
from copy import deepcopy
from sinon.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://mongohost:27017/'
DBNAME = 'sinon_benchmark'

SAMPLE_WU = {
    "description"  : {"A": "foo", "B": "bar" },
    "queue_id"     : "none"
}

SAMPLE_PILOT = {
    "X": "foo",
    "Y": "bar"
}

# --------------------------------------------------------------------------
#
def benchmark__add_workunits():

    # new session
    sid = str(uuid.uuid4())
    s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)

    # new pilot
    pilot = {"X": "foo",
             "Y": "bar"
    }
    p_ids = s.insert_pilots([pilot])


    for i in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384]:
        create_doc_timings = list()
        for _ in range(0, 5):
            # create a list that contains 'i' entries
            insert = list()
            for _ in range(0, i):
                insert.append(deepcopy(SAMPLE_WU))
            t_1 = time.time()
            ##########################
            s.insert_workunits(p_ids[0], insert)
            ##########################
            t_d = time.time() -t_1
            create_doc_timings.append(t_d)
        print "Average time to add work units in bulks of %i: %f sec." % (i, numpy.mean(create_doc_timings))
    print "\n"
    s.delete()

# --------------------------------------------------------------------------
#
def benchmark__get_workunits(session):

    session.delete()

    for i in [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384]:
        create_doc_timings = list()
        for d in range(0, 10):
            # create a list that contains 'i' entries
            insert = list()
            for x in range(0, i):
                insert.append(deepcopy(SAMPLE_WU))
            s.work_units_add(insert)
            t1 = time.time()
            wus = s.work_units_get()
            td = time.time() -t1
            session.delete()
            create_doc_timings.append(td)
        print "Average time to get %s work units : %f sec." % (len(wus), numpy.mean(create_doc_timings))
    print "\n"
    session.delete()



# --------------------------------------------------------------------------
#
if __name__ == '__main__':
    # remove existing DB in case it exists
    print "Dropping %s" %DBNAME 
    client = MongoClient(DBURL)
    client.drop_database(DBNAME)

    print "\nResults for %s: " % DBURL
    print "----------------------------------------------------------------------"
    #benchmark__add_workunits(session=s)
    benchmark__add_workunits()

    # remove existing DB in case it exists
    print "Dropping %s" %DBNAME 
    client = MongoClient(DBURL)
    client.drop_database(DBNAME)

    sys.exit(0)