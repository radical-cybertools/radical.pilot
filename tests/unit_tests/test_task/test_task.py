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

        umgr = rp.UnitManager(session=None)
        umgr.advance = mock.Mock(return_value=True)
        umgr._uids    = list()
        umgr._uid     = 'umgr.0000'
        umgr._log     = mock.Mock()
        umgr._prof    = mock.Mock()
        umgr._session = mock.Mock()
        umgr._session.uid = str(time.time())  # restart uid counter

        descr = rp.ComputeUnitDescription({'executable': 'true', 'uid': 'foo'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'foo')

        with self.assertRaises(ValueError):
            rp.ComputeUnit(umgr, descr)

        descr = rp.ComputeUnitDescription({'executable': 'true'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'unit.000000')

        descr = rp.ComputeUnitDescription({'executable': 'true', 'uid': 'bar'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'bar')

        with self.assertRaises(ValueError):
            rp.ComputeUnit(umgr, descr)

        descr = rp.ComputeUnitDescription({'executable': 'true'})
        self.assertEqual(rp.ComputeUnit(umgr, descr).uid, 'unit.000001')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.test_task_uid()


# ------------------------------------------------------------------------------

