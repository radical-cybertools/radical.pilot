""" (Compute) Unit tests
"""
import os
import sys
import radical.pilot as rp
import unittest

#-----------------------------------------------------------------------------
#
class TestIssue262(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves 
        pass

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__issue_262(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/18
        """
        session = rp.Session()
        pmgr = rp.PilotManager(session=session)

        # Create a local pilot with a million cores. This will most likely
        # fail as not enough cores will be available.  That means the pilot will
        # go quickly into failed state, and trigger the callback from above.
        pd = rp.ComputePilotDescription()
        pd.resource  = "local.localhost"
        pd.cores     = 1
        pd.runtime   = 1

        pilot = pmgr.submit_pilots(pd)

        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION)
        umgr.add_pilots(pilot)

        cud = rp.ComputeUnitDescription()
        cud.executable    = "/bin/sleep"
        cud.arguments     = ["10"]
        cud.cores         = 1
        cud.input_staging = ["/etc/group"]


        unit = umgr.submit_units(cud)
        umgr.wait_units()    

        for log_entry in pilot.log:
             ld = log_entry.as_dict()
             assert "timestamp" in ld
             assert "message"   in ld

             s = "%s" % log_entry
             assert type(s) == unicode

        for log_entry in unit.log:
            ld = log_entry.as_dict()
            assert "timestamp" in ld
            assert "message"   in ld

            s = "%s" % log_entry
            assert type(s) == unicode

        session.close()
