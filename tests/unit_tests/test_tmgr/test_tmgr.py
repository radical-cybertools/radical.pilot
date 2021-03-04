#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = "Copyright 2013-2021, http://radical.rutgers.edu"
__license__ = "MIT"

import glob
import os

from unittest import TestCase
from unittest import mock

import radical.utils as ru

from radical.pilot.task_manager import TaskManager


class TestTaskManager(TestCase):


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(TaskManager, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_add_pilots(self, mocked_init, mocked_Logger):

        self.maxDiff = None
        global_pilots = []

        def publish_side_effect(rpc, pilot):
            nonlocal global_pilots
            global_pilots.append(pilot)

        component = TaskManager()
        component._uid = 'tmgr.0000'
        component.publish = mock.MagicMock(side_effect=publish_side_effect)
        component._pilots_lock = ru.RLock('tmgr.pilots_lock')
        component._log = mocked_Logger
        component._pilots = {}

        mocked_pilot = ru.Munch({"uid":'pilot.0000'})
        component.add_pilots(mocked_pilot)
        result = {'cmd': 'add_pilots', 'arg': {'tmgr': 'tmgr.0000', 'pilots': [{'uid': 'pilot.0000'}]}}
        self.assertEqual(global_pilots[0], result)

        with self.assertRaises(ValueError):
            component.add_pilots(mocked_pilot)
