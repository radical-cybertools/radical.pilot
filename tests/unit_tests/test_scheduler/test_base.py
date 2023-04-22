#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pytest

import threading            as mt
import radical.utils        as ru
import radical.pilot.states as rps

from unittest import mock, TestCase

import radical.pilot.agent.scheduler.base as rpa_sb
AgentSchedulingComponent = rpa_sb.AgentSchedulingComponent

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestBaseScheduling(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        # provided JSON file (with test cases) should NOT contain any comments
        cls._test_cases = ru.read_json('%s/test_cases/test_base.json' % base)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    @mock.patch.object(ru.zmq.RegistryClient, '__init__', return_value=None)
    @mock.patch.object(ru.zmq.RegistryClient, 'put', return_value=None)
    @mock.patch.object(ru.zmq.RegistryClient, 'close', return_value=None)
    @mock.patch('radical.pilot.agent.scheduler.base.mp')
    @mock.patch('radical.utils.get_hostname', return_value=None)
    @mock.patch('radical.utils.env_eval')
    def test_initialize(self, mocked_env_eval, mocked_hostname, mocked_mp,
                        mocked_reg_close, mocked_reg_put, mocked_reg_init,
                        mocked_init):

        sched = AgentSchedulingComponent(cfg=None, session=None)
        sched._configure          = mock.Mock()
        sched._schedule_tasks     = mock.Mock()
        sched._log                = mock.Mock()
        sched._prof               = mock.Mock()
        sched.slot_status         = mock.Mock()
        sched.work                = mock.Mock()
        sched.unschedule_cb       = mock.Mock()
        sched.register_input      = mock.Mock()
        sched.register_subscriber = mock.Mock()
        sched.nodes               = []
        sched._partitions         = {}

        for c in self._test_cases['initialize']:

            def _mock_get(_c, name):
                return _c['registry'][name]

            from functools import partial

            mock_get   = partial(_mock_get, c)
            sched._cfg = ru.Config(from_dict=c['config'])
            with mock.patch.object(ru.zmq.RegistryClient, 'get', mock_get):
                if 'RuntimeError' in c['result']:
                    with pytest.raises(RuntimeError):
                        sched.initialize()
                else:
                    sched.initialize()
                    self.assertEqual(sched.nodes, c['result'])

            return


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_change_slot_states(self, mocked_init):

        sched = AgentSchedulingComponent(cfg=None, session=None)

        for c in self._test_cases['change_slots']:
            sched.nodes = c['nodes']
            if c['result'] == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    sched._change_slot_states(slots=c['slots'],
                                              new_state=c['new_state'])
            else:
                sched._change_slot_states(slots=c['slots'],
                                          new_state=c['new_state'])
                self.assertEqual(sched.nodes, c['result'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_slot_status(self, mocked_init):

        rpa_sb._debug = True
        sched = AgentSchedulingComponent(cfg=None, session=None)
        sched._log = ru.Logger('foo', targets=None, level='DEBUG_5')

        for c in self._test_cases['slot_status']:
            sched.nodes = c['nodes']
            self.assertEqual(sched.slot_status(), c['result'])

        # if log is NOT enabled for `logging.DEBUG`
        sched._log._debug_level = 0
        self.assertIsNone(sched.slot_status())
        rpa_sb._debug = False


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    @mock.patch.object(AgentSchedulingComponent, 'schedule_task')
    @mock.patch.object(AgentSchedulingComponent, '_change_slot_states')
    def test_try_allocation(self, mocked_change_slot_states,
                            mocked_schedule_task, mocked_init):

        component = AgentSchedulingComponent(None, None)
        component._active_cnt    = 0
        component._log           = ru.Logger('foo', targets=None, level='OFF')
        component._prof          = mock.Mock()
        component._prof.prof     = mock.Mock(return_value=True)

        # FIXME: the try_allocation part in the test config has no results?
        for c in self._test_cases['try_allocation']:

            # FIXME: what the heck are we actually testing if schedule_task
            #        is mocked?

            task = c['task']
            component.schedule_task = mock.Mock(return_value=c['slots'])
            component._try_allocation(task=task)

            self.assertEqual(task['slots'], c['slots'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    @mock.patch.object(AgentSchedulingComponent, 'advance', return_value=None)
    def test_control_cb(self, mocked_advance, mocked_init):

        log_messages = ''

        def _log_debug(*args):
            nonlocal log_messages
            log_messages += args[0]
            if len(args) > 1:
                log_messages = log_messages % args[1:]

        sched = AgentSchedulingComponent(cfg=None, session=None)
        sched._log = mock.Mock()
        sched._log.debug.side_effect = _log_debug

        sched._lock         = mt.Lock()
        sched._raptor_lock  = mt.Lock()

        task0000            = {}
        sched._waitpool     = {'task.0000': task0000}
        sched._raptor_tasks = {}

        msg = {'cmd': '', 'arg': {'uids': ['task.0000', 'task.0001']}}
        sched._control_cb(topic=None, msg=msg)

        self.assertTrue(sched._log.debug.called)
        self.assertIn('command ignored', log_messages)

        msg['cmd'] = 'cancel_tasks'
        sched._control_cb(topic=None, msg=msg)

        # task from `waitpool` was cancelled
        self.assertFalse(sched._waitpool)
        self.assertEqual(task0000['target_state'], rps.CANCELED)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestBaseScheduling()
    tc.setUpClass()
    tc.test_initialize()
    tc.test_change_slot_states()
    tc.test_slot_status()
    tc.test_try_allocation()


# ------------------------------------------------------------------------------

