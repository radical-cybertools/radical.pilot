#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import copy
import glob

from unittest import TestCase
from unittest import mock

import radical.utils           as ru
import radical.pilot.constants as rpc

from radical.pilot.agent.scheduler.continuous import Continuous


TEST_CASES_DIR = 'tests/unit_tests/test_scheduler/test_cases_continuous'


# ------------------------------------------------------------------------------
#
class TestContinuous(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._test_cases = []
        for f in glob.glob('%s/task.*.json' % TEST_CASES_DIR):
            cls._test_cases.append(ru.read_json(f))

        cls._config = ru.read_json('%s/test_continuous.json' % TEST_CASES_DIR)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0000'
        component._log = mocked_Logger

        for rm_info, resource_cfg, result in zip(
                self._config['configure']['rm_info'],
                self._config['configure']['resource_cfg'],
                self._config['configure']['result']):

            component._rm_info           = rm_info
            component._rm_lm_info        = rm_info['lm_info']
            component._rm_node_list      = rm_info['node_list']
            component._rm_cores_per_node = rm_info['cores_per_node']
            component._rm_gpus_per_node  = rm_info['gpus_per_node']
            component._rm_lfs_per_node   = rm_info['lfs_per_node']
            component._rm_mem_per_node   = rm_info['mem_per_node']
            component._cfg               = ru.Config(from_dict={
                'pid'         : 'pid.0000',
                'rm_info'     : rm_info,
                'resource_cfg': resource_cfg
            })

            component._configure()
            self.assertEqual(component.nodes, result)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_find_resources(self, mocked_init, mocked_configure, mocked_Logger):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0001'
        component._log = mocked_Logger

        for test_case in self._test_cases:

            # value for `slot['lfs']['path']` is taken
            # from `self._rm_lfs_per_node['path']` directly
            component._rm_lfs_per_node = {'size': 1024, 'path': '/dev/null'}

            td = test_case['task']['description']
            alc_slots = component._find_resources(
                node=test_case['setup']['nodes'][0],
                find_slots=td['cpu_processes'],
                cores_per_slot=td['cpu_threads'],
                gpus_per_slot=td['gpu_processes'],
                lfs_per_slot=td['lfs_per_process'],
                mem_per_slot=td['mem_per_process'],
                partial=False)

            if alc_slots is None:
                # tests should be defined in a way, so task would fit 1 node
                raise AssertionError('Test should be redefined (%s)' %
                                     test_case['task']['uid'])

            self.assertEqual(alc_slots, test_case['result']['slots']['nodes'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_schedule_task(self, mocked_init, mocked_configure, mocked_Logger):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0002'
        component._log = mocked_Logger

        for test_case in self._test_cases:

            nodes = test_case['setup']['nodes']

            component._rm_cores_per_node = len(nodes[0]['cores'])
            component._rm_gpus_per_node  = len(nodes[0]['gpus'])
            component._rm_lfs_per_node   = nodes[0]['lfs']
            component._rm_mem_per_node   = nodes[0]['mem']
            component._rm_lm_info        = dict()
            component._rm_partitions     = dict()
            component._colo_history      = dict()
            component._tagged_nodes      = set()
            component._scattered         = None
            component._node_offset       = 0
            component.nodes              = nodes

            task = test_case['task']
            slots = component.schedule_task(task)

            self.assertEqual(slots, test_case['result']['slots'])
            self.assertEqual(component._colo_history,
                             test_case['result']['colo_history'])

            # check tags exclusiveness (default: exclusive=False)

            task['description'].update({'tags': {'colocate': 'exclusive_tag',
                                                 'exclusive': True},
                                        'cpu_processes': 1,
                                        'cpu_threads': 1,
                                        'gpu_processes': 0})
            pre_sched_n_tagged_nodes = len(component._tagged_nodes)
            component.schedule_task(task)
            post_sched_n_tagged_nodes = len(component._tagged_nodes)
            if pre_sched_n_tagged_nodes < len(component.nodes):
                # new task got exclusive node(s)
                self.assertEqual(post_sched_n_tagged_nodes,
                                 pre_sched_n_tagged_nodes + 1)
            else:
                # new task shared allocated nodes
                self.assertEqual(post_sched_n_tagged_nodes,
                                 pre_sched_n_tagged_nodes)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_unschedule_task(self, mocked_init, mocked_Logger):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0003'
        component._log = mocked_Logger

        for test_case in self._test_cases:

            component.nodes = copy.deepcopy(test_case['setup']['nodes'])

            task = {'description': test_case['task']['description'],
                    'slots'      : test_case['result']['slots']}

            # set corresponding cores/gpus as busy
            component._change_slot_states(task['slots'], rpc.BUSY)
            # check that nodes got changed
            self.assertNotEqual(component.nodes, test_case['setup']['nodes'])

            component.unschedule_task(task)

            # nodes are back to the initial state
            self.assertEqual(component.nodes, test_case['setup']['nodes'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestContinuous()
    tc.test_configure()
    tc.test_unschedule_task()
    tc.test_find_resources()
    tc.test_schedule_task()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
