""" (Compute) Unit tests
"""
import os
import sys
import radical.pilot
import unittest

import uuid
from copy import deepcopy
from radical.pilot.db import Session
from pymongo import MongoClient

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: radical.pilot_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = 'XYZ'


#-----------------------------------------------------------------------------
#
class TestIssue163(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def tearDown(self):
        # clean up after ourselves 
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__issue_163_part_1(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/163
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

         # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Get all configs,
        res = session.list_resource_configs()
        # ... and the entry specific for localhost
        s = res['localhost']

        # Build a new one based on localhost
        rc = radical.pilot.ResourceConfig(s)
        rc.name = 'testing123-localhost'
        # And set the queue to development to get a faster turnaround
        rc.default_queue = 'development'
        # Now add the entry back to the PM
        session.add_resource_config(rc)

        # Get all configs
        res = session.list_resource_configs()
        s = res['testing123-localhost']
        assert s['default_queue'] == 'development'

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource  = "testing123-localhost"
        pdesc.runtime   = 1 
        pdesc.cores     = 1
        pdesc.cleanup   = True

        pilot = pmgr.submit_pilots(pdesc)
        pilot.wait(timeout=2.0*60)
        
        # This passes only if the pilot started succesfully. 
        assert pilot.state == radical.pilot.states.DONE, "state: {0}".format(pilot.state)

        session.close(delete=False)


    #-------------------------------------------------------------------------
    #
    def test__issue_163_part_2(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/163
        """
        pass
