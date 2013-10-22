#!/usr/bin/env python
# encoding: utf-8

"""Database conncetion layer benchmarks
"""

import sys
import time
import numpy
from copy import deepcopy
from sinon.db import Session

DBURL = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'

SAMPLE_WU = {
    "work_unit_id"  : "unique work unit ID",
    "description"   : {
        "x" : "y"
    },
    "assignment"    : { 
        "queue" : "queue id",
        "pilot" : "pilot id"
    }
}

# --------------------------------------------------------------------------
#
def benchmark__add_workunits():
    s = Session.new(db_url=DBURL, sid="benchmark")

    for i in [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384]:
        create_doc_timings = list()
        for d in range(0, 10):
            # create a list that contains 'i' entries
            insert = list()
            for x in range(0, i):
                insert.append(deepcopy(SAMPLE_WU))


            t1 = time.time()
            s.work_units_add(insert)
            td = time.time() -t1
            create_doc_timings.append(td)
        s.delete()
        print "Average time to add work units in bulks of %i: %f sec." % (i, numpy.mean(create_doc_timings))



# --------------------------------------------------------------------------
#
if __name__ == '__main__':
    print "\nResults for %s: " % DBURL
    print "======================================================================"

    benchmark__add_workunits()

    sys.exit(0)