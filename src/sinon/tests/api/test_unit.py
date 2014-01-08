""" (Compute) Unit tests
"""

import sinon
import unittest

import uuid
from copy import deepcopy
from sinon.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
RESCFG = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'
DBNAME = 'sinon_test'

#-----------------------------------------------------------------------------
#
class TestUnit(unittest.TestCase):
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
    def test__unit_wait(self):
        """ Test if we can wait for different unit states. 
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm = sinon.PilotManager(session=session, resource_configurations=RESCFG)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = "localhost"
        cpd.cores             = 1
        cpd.run_time          = 1
        cpd.working_directory = "/tmp/sinon.unit-tests" 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = sinon.UnitManager(session=session, scheduler='round_robin')
        um.add_pilots(pilot)

        cudesc = sinon.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['10']
        
        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None
        assert cu.start_time is None
        assert cu.start_time is None

        print "AASDSADASDASDAS: %s" % cu.state

        cu.wait(sinon.states.RUNNING)
        assert cu.state == sinon.states.RUNNING
        assert cu.start_time is not None

        cu.wait(sinon.states.DONE)
        assert cu.state == sinon.states.DONE
        assert cu.stop_time is not None

        pm.cancel_pilots()








