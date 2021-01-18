#!/usr/bin/env python3
# pylint: disable=unused-argument, no-value-for-parameter
import time

from   unittest import mock
from   unittest import TestCase

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    @mock.patch.object(rp.UnitManager, '__init__', return_value=None)
    def test_task_uid(self, mocked_init):

        tmgr = rp.UnitManager(session=None)
        tmgr.advance = mock.Mock(return_value=True)
        tmgr._uids    = list()
        tmgr._uid     = 'tmgr.0000'
        tmgr._log     = mock.Mock()
        tmgr._prof    = mock.Mock()
        tmgr._session = mock.Mock()
        tmgr._session.uid = str(time.time())  # restart uid counter

        descr = rp.ComputeUnitDescription({'executable': 'true', 'uid': 'foo'})
        self.assertEqual(rp.ComputeUnit(tmgr, descr).uid, 'foo')

        with self.assertRaises(ValueError):
            rp.ComputeUnit(tmgr, descr)

        descr = rp.ComputeUnitDescription({'executable': 'true'})
        self.assertEqual(rp.ComputeUnit(tmgr, descr).uid, 'task.000000')

        descr = rp.ComputeUnitDescription({'executable': 'true', 'uid': 'bar'})
        self.assertEqual(rp.ComputeUnit(tmgr, descr).uid, 'bar')

        with self.assertRaises(ValueError):
            rp.ComputeUnit(tmgr, descr)

        descr = rp.ComputeUnitDescription({'executable': 'true'})
        self.assertEqual(rp.ComputeUnit(tmgr, descr).uid, 'task.000001')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.test_task_uid()


# ------------------------------------------------------------------------------

