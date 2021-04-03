#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import copy
import glob
<<<<<<< HEAD
import os
=======
>>>>>>> devel

from unittest import TestCase
from unittest import mock

import radical.utils           as ru

from   radical.pilot.agent.scheduler.continuous import Continuous

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
<<<<<<< HEAD

        test_case, _ = self.setUp()
        for rm_info, cfg, result in zip(test_case['configure']['rm_info'],
                                        test_case['configure']['cfg'],
                                        test_case['configure']['result']):
=======

        # 1) without blocked cores; 2) with blocked cores;
        blocked_cores_list = [[], [0, 1]]

        for rm_info in self._config['configure']['rm_info']:
>>>>>>> devel

            component._rm_info           = rm_info
            component._rm_lm_info        = rm_info['lm_info']
            component._rm_node_list      = rm_info['node_list']
            component._rm_cores_per_node = rm_info['cores_per_node']
            component._rm_gpus_per_node  = rm_info['gpus_per_node']
            component._rm_lfs_per_node   = rm_info['lfs_per_node']
            component._rm_mem_per_node   = rm_info['mem_per_node']
            component._cfg               = ru.Munch(cfg)
            component._configure()

            self.assertEqual(component.nodes, result)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch('radical.utils.Logger')
<<<<<<< HEAD
    def test_find_resources(self, mocked_init, mocked_configure, mocked_logger):

        component = Continuous(cfg=None, session=None)
        result = [{'uid': 2,
                   'name': 'a',
                   'core_map': [[0, 1, 2, 3]],
                   'gpu_map': [[0], [1]],
                   'lfs': {'size': 1234, 'path': '/dev/null'},
                   'mem': 1024}]
        component.node = {'name'  : 'a',
                          'uid'   : 2,
                          'cores' : [0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0],
                          'lfs'   : {"size": 1234,
                                     "path" : "/dev/null"},
                          'mem'   : 1024,
                          'gpus'  : [0, 0]}
        component._log = mocked_logger
        component._rm_lfs_per_node = {"path" : "/dev/null", "size" : 1234}
        component.cores_per_slot   = 4
        component.gpus_per_slot    = 2
        component.lfs_per_slot     = 1234
        component.mem_per_slot     = 1024
        component.find_slot        = 1

        test_slot = component._find_resources(
                node=component.node,
                find_slots=component.find_slot,
                cores_per_slot=component.cores_per_slot,
                gpus_per_slot=component.gpus_per_slot,
                lfs_per_slot=component.lfs_per_slot,
                mem_per_slot=component.mem_per_slot,
                partial='None')

        self.assertEqual(test_slot, result)
=======
    def test_find_resources(self, mocked_init, mocked_configure, mocked_Logger):

        test_case = self._test_cases[0]
>>>>>>> devel

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0001'
        component._log = mocked_Logger
        component._rm_lfs_per_node = {'size': 1024, 'path': '/dev/null'}

        td = test_case['task']['description']
        new_slot = component._find_resources(
            node=self._config['allocate']['nodes'][0][0],
            find_slots=td['cpu_processes'],
            cores_per_slot=td['cpu_threads'],
            gpus_per_slot=td['gpu_processes'],
            lfs_per_slot=td['lfs_per_process'],
            mem_per_slot=td['mem_per_process'],
            partial=False)

        self.assertEqual(new_slot, test_case['setup']['lm']['slots']['nodes'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_schedule_task(self, mocked_init, mocked_configure, mocked_Logger):

        test_case = self._test_cases[0]
        nodes = self._config['allocate']['nodes'][0]

<<<<<<< HEAD
        _, cfg = self.setUp()
        for task_descr in cfg:
            component = Continuous(cfg=None, session=None)
            task = dict()
            task['uid'] = task_descr['task']['uid']
            task['description'] = task_descr['task']['description']
            component.nodes = task_descr['setup']['lm']['slots']['nodes']
            component._tag_history       = dict()
            component._rm_cores_per_node = 32
            component._rm_gpus_per_node  = 2
            component._rm_lfs_per_node   = {"size": 0, "path": "/dev/null"}
            component._rm_mem_per_node   = 1024
            component._rm_lm_info        = dict()
            component._rm_partitions     = dict()
            component._log               = mocked_Logger
            component._dvm_host_list     = None
            component._node_offset       = 0
            test_slot = {'cores_per_node': 32,
                         'gpus_per_node': 2,
                         'lfs_per_node': {'path': '/dev/null', 'size': 0},
                         'lm_info': {},
                         'mem_per_node': 1024,
                         'nodes': [{'core_map': [[0]],
                                    'gpu_map' : [[0]],
                                    'lfs': {'path': '/dev/null', 'size': 1234},
                                    'mem': 128,
                                    'name': 'a',
                                    'uid': 1}]}
            slot = component.schedule_task(task)
            self.assertEqual(slot, test_slot)
            self.assertEqual(component._tag_history, {})
=======
        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0002'
        component._log = mocked_Logger
        component._rm_cores_per_node = len(nodes[0]['cores'])
        component._rm_gpus_per_node  = len(nodes[0]['gpus'])
        component._rm_lfs_per_node   = nodes[0]['lfs']
        component._rm_mem_per_node   = nodes[0]['mem']
        component._rm_lm_info        = dict()
        component._rm_partitions     = dict()
        component._tag_history       = dict()
        component._tagged_nodes      = set()
        component._scattered         = None
        component._node_offset       = 0
>>>>>>> devel

        component.nodes              = nodes

        task = {'uid': test_case['task']['uid'],
                'description': test_case['task']['description']}

        self.assertEqual(component.schedule_task(task),
                         test_case['setup']['lm']['slots'])

        # check tags exclusiveness (default: exclusive=False)

        # initial tag is set
        self.assertEqual(component._tag_history, {'tag.0000': [1, 1]})

        # schedule tasks with new [exclusive] tags
        node_uids = [n['uid'] for n in nodes]
        task_tags = ['tag.e.%s' % u for u in node_uids]

        for task_tag in task_tags:
            # the number of new exclusive tags is equal to the number of
            # provided nodes, thus, considering earlier defined tag, there are
            # not enough nodes for exclusive tags
            task['description']['tags'] = {'colocate' : task_tag,
                                           'exclusive': True}
            component.schedule_task(task)

        task_tags.insert(0, 'tag.0000')  # bring initial tag to the list of tags
        node_uids.append(node_uids[-1])  # last node will be reused
        tag_history = {task_tags[i]: [u, u] for i, u in enumerate(node_uids)}
        self.assertEqual(component._tag_history, tag_history)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_unschedule_task(self, mocked_init, mocked_Logger):

        test_case = self._test_cases[0]
        nodes = self._config['allocate']['nodes'][1]

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0002'
        component._log = mocked_Logger

        component.nodes = copy.deepcopy(nodes)

        task = {'description': test_case['task']['description'],
                'slots'      : test_case['setup']['lm']['slots']}

        self.assertEqual(nodes[0], component.nodes[0])
        component.unschedule_task(task)
        self.assertNotEqual(nodes[0], component.nodes[0])


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
