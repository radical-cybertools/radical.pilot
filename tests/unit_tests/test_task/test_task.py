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
            rp.Task(tmgr, descr, 'test')

        descr = rp.TaskDescription({'uid'       : 'foo.1',
                                    'name'      : 'bar',
                                    'executable': './exec'})
        task = rp.Task(tmgr, descr, 'test')
        self.assertEqual(task.uid,  'foo.1')
        self.assertEqual(task.name, 'bar')
        self.assertEqual(task.state, rp.NEW)
        self.assertIsInstance(task.as_dict(), dict)

        with self.assertRaises(ValueError):
            # uid is not unique
            rp.Task(tmgr, descr, 'test')

        descr = rp.TaskDescription({'executable': './exec'})
        self.assertEqual(rp.Task(tmgr, descr, 'test').uid, 'task.000000')

        descr = rp.TaskDescription({'executable': './exec'})
        self.assertEqual(rp.Task(tmgr, descr, 'test').uid, 'task.000001')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.test_task_uid()


# ------------------------------------------------------------------------------

