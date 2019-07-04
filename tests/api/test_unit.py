""" (Compute) Unit tests
"""

import os
import sys
import radical.pilot
import unittest
import time
import uuid

from pymongo import MongoClient


#-----------------------------------------------------------------------------
#
class TestUnit(unittest.TestCase):
    # silence deprecation warnings under py3

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
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['10']

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None
        assert cu.start_time is None # MS: I dont understand this assertion

        cu.wait(state=[radical.pilot.AGENT_EXECUTING, radical.pilot.FAILED], timeout=5*60)
        assert cu.state == radical.pilot.AGENT_EXECUTING
        assert cu.start_time is not None

        cu.wait(timeout=5*60)
        assert cu.state == radical.pilot.DONE
        assert cu.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unit_cancel(self):
        """ Test if we can cancel a compute unit
        """
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.PMGR_ACTIVE, timeout=120)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["30"]

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None

        # Make sure it is running!
        cu.wait(state=radical.pilot.AGENT_EXECUTING, timeout=60)
        assert cu.state == radical.pilot.AGENT_EXECUTING
        assert cu.start_time is not None

        # Cancel the CU!
        cu.cancel()

        cu.wait(timeout=60)
        assert cu.state == radical.pilot.CANCELED
        assert cu.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unit_cancel_um(self):
        """ Test if we can cancel a compute unit through the UM
        """
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.PMGR_ACTIVE, timeout=240)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["60"]

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None

        # Make sure it is running!
        cu.wait(state=radical.pilot.AGENT_EXECUTING, timeout=60)
        assert cu.state == radical.pilot.AGENT_EXECUTING
        assert cu.start_time is not None

        # Cancel the CU!
        um.cancel_units(cu.uid)

        cu.wait(timeout=60)
        assert cu.state == radical.pilot.CANCELED
        assert cu.stop_time is not None

        session.close()
