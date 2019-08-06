""" (Compute) Unit tests
"""
import os
import sys
import unittest

from pymongo import MongoClient

import radical.pilot as rp



# -----------------------------------------------------------------------------
#
class TestResourceConfigs(unittest.TestCase):
    # silence deprecation warnings under py3

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
        session = rp.Session()


        rc = rp.ResourceConfig('test')
        session.add_resource_config(rc)
        session.get_resource_config('test')

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__add_resource_config_2(self):
        """ Test if we can wait for different pilot states.
        """
        session = rp.Session()

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

