#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pytest
import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

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
                print([_c, name])
                return _c['registry'][name]

            from functools import partial
            print(c)
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
    @mock.patch('radical.utils.Logger')
    def test_slot_status(self, mocked_logger, mocked_init):

        sched = AgentSchedulingComponent(cfg=None, session=None)
        sched._log = mocked_logger

        for c in self._test_cases['slot_status']:
            sched.nodes = c['nodes']
            self.assertEqual(sched.slot_status(), c['result'])

        # if log is NOT enabled for `logging.DEBUG`
        sched._log.isEnabledFor.return_value = False
        self.assertIsNone(sched.slot_status())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    @mock.patch.object(AgentSchedulingComponent, 'schedule_task')
    @mock.patch.object(AgentSchedulingComponent, '_change_slot_states')
    def test_try_allocation(self, mocked_change_slot_states,
                            mocked_schedule_task, mocked_init):

        component = AgentSchedulingComponent(None, None)
        component._active_cnt    = 0
        component._log           = mock.Mock()
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

