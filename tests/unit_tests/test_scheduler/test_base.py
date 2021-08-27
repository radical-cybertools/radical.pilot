#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

TEST_CASES_DIR = 'tests/unit_tests/test_scheduler/test_cases'


# ------------------------------------------------------------------------------
#
class TestBaseScheduling(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        # provided JSON file (with test cases) should NOT contain any comments
        cls._test_cases = ru.read_json('%s/test_base.json' % TEST_CASES_DIR,
                                       filter_comments=False)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.scheduler.base.ru.zmq')
    @mock.patch('radical.pilot.agent.scheduler.base.mp')
    def test_initialize(self, mocked_mp, mocked_zmq, mocked_init):

        sched = AgentSchedulingComponent(cfg = None, session = None)
        sched._configure          = mock.Mock()
        sched._schedule_tasks     = mock.Mock()
        sched._log                = mock.Mock()
        sched._prof               = mock.Mock()
        sched.slot_status         = mock.Mock()
        sched.work                = mock.Mock()
        sched.unschedule_cb       = mock.Mock()
        sched.register_input      = mock.Mock()
        sched.register_subscriber = mock.Mock()

        for c in self._test_cases['initialize']:
            sched._cfg = ru.Config(from_dict=c['config'])

            if c['result'] == 'RuntimeError':
                # not set: `node_list` or `cores_per_node` or `gpus_per_node`
                with self.assertRaises(RuntimeError):
                    sched.initialize()
            else:
                sched.initialize()
                self.assertEqual(ru.demunch(sched.nodes), c['result'])


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

        component = AgentSchedulingComponent()
        component._allocate_slot = mock.Mock(side_effect=[None,
                                                          {'slot':'test_slot'}])
        component._active_cnt    = 0
        component._log           = mock.Mock()
        component._prof          = mock.Mock()
        component._prof.prof     = mock.Mock(return_value=True)
        component._wait_pool     = list()
        component._wait_lock     = threading.RLock()
        component._slot_lock     = threading.RLock()

        tests = self.setUp()['try_allocation']
        for input_data, result in zip(tests['setup'], tests['results']):
            component.schedule_task = mock.Mock(
                return_value=input_data['scheduled_task_slots'])

            task = input_data['task']
            component._try_allocation(task=task)

            # test task's slots
            self.assertEqual(task['slots'], result['slots'])

            # test environment variable(s)
            self.assertEqual(task['description']['environment'],
                             result['description']['environment'])


    # --------------------------------------------------------------------------
    #
    # @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    # @mock.patch('radical.utils.Logger')
    # def test_handle_cuda(self, mocked_logger, mocked_init):
    #
    #     tests     = self._test_cases['handle_cuda']:
    #     setups    = tests['setup']
    #     tasks     = tests['task']
    #     results   = tests['results']
    #     component = AgentSchedulingComponent(cfg=None, session=None)
    #     component._log = mocked_logger
    #
    #     for setup, task, result in zip(setups, tasks, results):
    #         component._cfg = setup
    #         if result == 'ValueError':
    #             with self.assertRaises(ValueError):
    #                 component._handle_cuda(task)
    #         else:
    #             component._handle_cuda(task)
    #             task_env = task['description']['environment']
    #             if result == 'KeyError':
    #                 with self.assertRaises(KeyError):
    #                     self.assertIsNone(task_env['CUDA_VISIBLE_DEVICES'])
    #             else:
    #                 self.assertEqual(task_env['CUDA_VISIBLE_DEVICES'], result)


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

