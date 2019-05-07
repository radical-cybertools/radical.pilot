""" (Compute) Unit tests
"""
import os
import sys
import unittest

from pymongo import MongoClient

import radical.pilot as rp


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

DBNAME = os.getenv("RADICAL_PILOT_TEST_DBNAME", 'test')
if DBNAME is None:
    print "ERROR: RADICAL_PILOT_TEST_DBNAME (MongoDB database name) is not defined."
    sys.exit(1)


# -----------------------------------------------------------------------------
#
class TestResourceConfigs(unittest.TestCase):
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

    # -------------------------------------------------------------------------
    #
    def test__add_resource_config_1(self):
        """ Test if we can wait for different pilot states.
        """
        session = rp.Session(database_url=DBURL)


        rc = rp.ResourceConfig('test')
        session.add_resource_config(rc)
        session.get_resource_config('test')

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__add_resource_config_2(self):
        """ Test if we can wait for different pilot states.
        """
        session = rp.Session(database_url=DBURL)

        rc = rp.ResourceConfig("mylocalhost")
        rc.task_launch_method   = "LOCAL"
        rc.mpi_launch_method    = "MPIRUN"
        rc.job_manager_endpoint = "fork://localhost"
        rc.filesystem_endpoint  = "file://localhost/"
        rc.bootstrapper         = "default_bootstrapper.sh"

        pm = rp.PilotManager(session=session)
        session.add_resource_config(rc)

        pd = rp.ComputePilotDescription()
        pd.resource = "mylocalhost"
        pd.cores    = 1
        pd.runtime  = 1
        pd.sandbox = "/tmp/rp.sandbox.unittests"
        pd.cleanup = True

        pilot = pm.submit_pilots(pd)
        pilot.wait(timeout=300)
        pilot.cancel()

        session.close()

