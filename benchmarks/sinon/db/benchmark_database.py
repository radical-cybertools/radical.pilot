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

DBNAME = 'radicalpilot_benchmark'

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
    s = Session.new(sid=sid, database_name=DBNAME)

    # new pilot
    pilot = {"X": "foo",
             "Y": "bar"
    }
    p_ids = s.insert_pilots([pilot])

    for i in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]:
        insert_workunits_timings = list()
        get_raw_work_units_timings = list()
        for _ in range(0, 5):
            # create a list that contains 'i' entries
            insert = list()
            for _ in range(0, i):
                insert.append(deepcopy(SAMPLE_WU))
            t_1 = time.time()
            ##########################
            s.insert_workunits(p_ids[0], insert)
            ##########################
            t_d1 = time.time() -t_1
            insert_workunits_timings.append(t_d1)

            t_2 = time.time()
            ############################
            wus = s.get_raw_workunits()
            ############################
            t_d2 = time.time() - t_2
            get_raw_work_units_timings.append(t_d2)

        print "Average time to add work units in bulks of %i: %f sec." % (i, numpy.mean(insert_workunits_timings))
        print "Average time to get %s work units in bulk: %f sec." % (len(wus), numpy.mean(get_raw_work_units_timings))

    print "\n"
    s.delete()

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
