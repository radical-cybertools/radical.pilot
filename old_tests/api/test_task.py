""" Task tests
"""

import os
import sys
import radical.pilot
import tasktest
import time
import uuid

from pymongo import MongoClient


#-----------------------------------------------------------------------------
#
class TestTask(tasktest.TestCase):
    # silence deprecation warnings under py3

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__task_wait(self):
        """ Test if we can wait for different task states.
        """
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.PilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.tasktests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.TaskManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = radical.pilot.TaskDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['10']

        t = um.submit_tasks(cudesc)

        assert t is not None
        assert t.submission_time is not None
        assert t.start_time is None # MS: I dont understand this assertion

        t.wait(state=[radical.pilot.AGENT_EXECUTING, radical.pilot.FAILED], timeout=5*60)
        assert t.state == radical.pilot.AGENT_EXECUTING
        assert t.start_time is not None

        t.wait(timeout=5*60)
        assert t.state == radical.pilot.DONE
        assert t.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__task_cancel(self):
        """ Test if we can cancel a task
        """
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.PilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.tasktests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.TaskManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.PMGR_ACTIVE, timeout=120)

        cudesc = radical.pilot.TaskDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["30"]

        t = um.submit_tasks(cudesc)

        assert t is not None
        assert t.submission_time is not None

        # Make sure it is running!
        t.wait(state=radical.pilot.AGENT_EXECUTING, timeout=60)
        assert t.state == radical.pilot.AGENT_EXECUTING
        assert t.start_time is not None

        # Cancel the Task!
        t.cancel()

        t.wait(timeout=60)
        assert t.state == radical.pilot.CANCELED
        assert t.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__task_cancel_um(self):
        """ Test if we can cancel a task through the UM
        """
        session = radical.pilot.Session()

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.PilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.tasktests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(descriptions=cpd)

        um = radical.pilot.TaskManager(
            session=session,
            scheduler=radical.pilot.SCHEDULER_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.PMGR_ACTIVE, timeout=240)

        cudesc = radical.pilot.TaskDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["60"]

        t = um.submit_tasks(cudesc)

        assert t is not None
        assert t.submission_time is not None

        # Make sure it is running!
        t.wait(state=radical.pilot.AGENT_EXECUTING, timeout=60)
        assert t.state == radical.pilot.AGENT_EXECUTING
        assert t.start_time is not None

        # Cancel the Task!
        um.cancel_tasks(t.uid)

        t.wait(timeout=60)
        assert t.state == radical.pilot.CANCELED
        assert t.stop_time is not None

        session.close()
