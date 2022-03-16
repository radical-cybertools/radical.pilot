#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import threading as mt

from unittest import TestCase
from unittest import mock

import radical.utils           as ru
import radical.pilot.constants as rpc

from radical.pilot.task_manager import TaskManager


# ------------------------------------------------------------------------------
#
class TMGRTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(TaskManager, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_add_pilots(self, mocked_logger, mocked_init):

        global_pilots = []

        def publish_side_effect(rpc, pilot):
            print(type(pilot), pilot)
            nonlocal global_pilots
            global_pilots.append(pilot)

        component = TaskManager(None)
        component._uid = 'tmgr.0000'
        component.publish = mock.MagicMock(side_effect=publish_side_effect)
        component._pilots_lock = ru.RLock('tmgr.pilots_lock')
        component._log = mocked_logger
        component._pilots = {}

        p_desc = {'uid': 'pilot.0000'}
        component.add_pilots(p_desc)
        self.assertEqual(component._pilots['pilot.0000'], p_desc)
        # self.assertEqual(global_pilots[0], result)

        with self.assertRaises(ValueError):
            component.add_pilots(p_desc)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(TaskManager, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_register_callback(self, mocked_logger, mocked_init):

        component = TaskManager(None)
        component._uid       = 'tmgr.0001'
        component._tcb_lock  = mt.Lock()
        component._log       = mocked_logger
        component._tasks     = {}
        component._callbacks = {}

        # register callback

        with self.assertRaises(ValueError):
            # there is no task with provided "uid"
            component.register_callback(cb=None, uid='unknown_task_uid')

        with self.assertRaises(ValueError):
            # metric is not valid
            component.register_callback(cb=None, metric='non_valid_metric')

        cb = mock.Mock()
        component.register_callback(cb)
        self.assertIn(rpc.TASK_STATE, component._callbacks)
        self.assertIn('*',            component._callbacks[rpc.TASK_STATE])
        self.assertIn(id(cb),         component._callbacks[rpc.TASK_STATE]['*'])

        # unregister callback

        with self.assertRaises(ValueError):
            # there is no task with provided "uid"
            component.unregister_callback(uid='unknown_task_uid')

        with self.assertRaises(ValueError):
            # metric is not valid
            component.unregister_callback(metrics='non_valid_metric')

        with self.assertRaises(ValueError):
            # metric is not in callbacks
            component.unregister_callback(metrics=rpc.WAIT_QUEUE_SIZE)

        component._tasks['task.0000'] = mock.Mock()
        with self.assertRaises(ValueError):
            # task is known, but call back for this task is not set
            component.unregister_callback(uid='task.0000')

        component.unregister_callback()
        # no callbacks for earlier set metric
        self.assertFalse(component._callbacks[rpc.TASK_STATE]['*'])


# ------------------------------------------------------------------------------

