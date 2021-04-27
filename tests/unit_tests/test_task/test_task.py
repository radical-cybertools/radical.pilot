# pylint: disable=unused-argument, no-value-for-parameter

import time

from unittest import TestCase, mock

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(rp.TaskManager, '__init__', return_value=None)
    def test_task_uid(self, mocked_init):

        tmgr = rp.TaskManager(None)
        tmgr._uid     = 'tmgr.0000'
        tmgr._uids    = list()
        tmgr._log     = mock.Mock()
        tmgr._prof    = mock.Mock()
        tmgr._session = mock.Mock(uid=str(time.time()))  # restart uid counter
        tmgr.advance  = mock.Mock()

        descr = rp.TaskDescription({})
        with self.assertRaises(ValueError):
            # no either `executable` or `kernel`
            rp.Task(tmgr, descr)

        descr = rp.TaskDescription({'uid'       : 'foo',
                                    'name'      : 'bar',
                                    'executable': './exec'})
        task = rp.Task(tmgr, descr)
        self.assertEqual(task.uid,  'foo')
        self.assertEqual(task.name, 'bar')
        self.assertEqual(task.state, rp.NEW)
        self.assertIsInstance(task.as_dict(), dict)

        with self.assertRaises(ValueError):
            # uid is not unique
            rp.Task(tmgr, descr)

        descr = rp.TaskDescription({'executable': './exec'})
        self.assertEqual(rp.Task(tmgr, descr).uid, 'task.000000')

        descr = rp.TaskDescription({'executable': './exec'})
        self.assertEqual(rp.Task(tmgr, descr).uid, 'task.000001')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(rp.UnitManager, '__init__', return_value=None)
    def test_task_uid_deprecated(self, mocked_init):

        umgr = rp.UnitManager(None)
        umgr._uid     = 'umgr.0000'
        umgr._uids    = list()
        umgr._log     = mock.Mock()
        umgr._prof    = mock.Mock()
        umgr._session = mock.Mock(uid=str(time.time()))  # restart uid counter
        umgr.advance  = mock.Mock()

        descr = rp.ComputeUnitDescription({})
        with self.assertRaises(ValueError):
            # no either `executable` or `kernel`
            rp.ComputeUnit(umgr, descr)

        descr = rp.ComputeUnitDescription({'uid'       : 'foo',
                                           'name'      : 'bar',
                                           'executable': './exec'})
        task = rp.ComputeUnit(umgr, descr)
        self.assertEqual(task.uid,  'foo')
        self.assertEqual(task.name, 'bar')
        self.assertEqual(task.state, rp.NEW)
        self.assertIsInstance(task.as_dict(), dict)

        with self.assertRaises(ValueError):
            # uid is not unique
            rp.ComputeUnit(umgr, descr)

        descr = rp.ComputeUnitDescription({'executable': './exec'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'task.000000')

        descr = rp.ComputeUnitDescription({'executable': './exec'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'task.000001')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.test_task_uid()
    tc.test_task_uid_deprecated()


# ------------------------------------------------------------------------------

