""" Task Manager tests
"""

import os
import sys
import tasktest

from pymongo import MongoClient

import radical.pilot as rp


# -----------------------------------------------------------------------------
#
class TestTaskManager(tasktest.TestCase):
    # silence deprecation warnings under py3

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)


    # -------------------------------------------------------------------------
    #
    def test__taskmanager_create(self):
        """ Test if task manager creation works as expected.
        """
        session = rp.Session()
        assert session.list_task_managers() == [], "Wrong number of task managers"

        um1 = rp.TaskManager(session=session, scheduler='round_robin')
        assert session.list_task_managers() == [um1.uid], "Wrong list of task managers"

        um2 = rp.TaskManager(session=session, scheduler='round_robin')
        assert session.list_task_managers() == [um1.uid, um2.uid], "Wrong list of task managers"

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__taskmanager_reconnect(self):
        """ Test if task manager reconnection works as expected.
        """
        session = rp.Session()

        um = rp.TaskManager(session=session, scheduler='round_robin')
        assert session.list_task_managers() == [um.uid], "Wrong list of task managers"

        um_r = session.get_task_managers(task_manager_ids=um.uid)
        assert session.list_task_managers() == [um_r.uid], "Wrong list of task managers"

        assert um.uid == um_r.uid, "Task Manager IDs not matching!"

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__taskmanager_pilot_assoc(self):
        """ Test if task manager <-> pilot association works as expected.
        """
        session = rp.Session()

        pm = rp.PilotManager(session=session)

        cpd = rp.PilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/rp.sandbox.tasktests"
        cpd.cleanup = True

        p1 = pm.submit_pilots(descriptions=cpd)

        um = rp.TaskManager(session=session, scheduler='round_robin')
        assert um.list_pilots() == [], "Wrong list of pilots"

        um.add_pilots(p1)
        assert um.list_pilots() == [p1.uid], "Wrong list of pilots"

        # adding the same pilot twice should be ignored
        um.add_pilots(p1)
        assert um.list_pilots() == [p1.uid], "Wrong list of pilots"

        um.remove_pilots(p1.uid)
        assert um.list_pilots() == [], "Wrong list of pilots"

        pilot_list = []
        for x in range(0, 2):
            cpd = rp.PilotDescription()
            cpd.resource = "local.localhost"
            cpd.cores = 1
            cpd.runtime = 1
            cpd.sandbox = "/tmp/rp.sandbox.tasktests"
            cpd.cleanup = True
            p = pm.submit_pilots(descriptions=cpd)
            um.add_pilots(p)
            pilot_list.append(p)

        pl = um.list_pilots()
        assert len(pl) == 2, "Wrong number of associated pilots"
        for l in pilot_list:
            assert l in pilot_list, "Unknown pilot in list"
            um.remove_pilots(l.uid)

        assert um.list_pilots() == [], "Wrong list of pilots"

        session.close()
