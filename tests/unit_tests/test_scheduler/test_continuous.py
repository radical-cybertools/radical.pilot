#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = "Copyright 2013-2021, http://radical.rutgers.edu"
__license__ = "MIT"

import glob

from unittest import TestCase
from unittest import mock

import radical.utils as ru

from radical.pilot.agent.scheduler.continuous import Continuous


TEST_CASES_DIR = 'tests/unit_tests/test_scheduler/test_cases_continuous'


# ------------------------------------------------------------------------------
#
class TestContinuous(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):

        ret  = list()
        pat  = '%s/task*.json' % TEST_CASES_DIR

        for fin in glob.glob(pat):
            test_cases = ru.read_json(fin)
            ret.append(test_cases)

        cfg_fname = '%s/test_continuous.json' % TEST_CASES_DIR
        cls.cfg_tests = ru.read_json(cfg_fname)
        cls.tasks = ret


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0000'
        component._log = mocked_Logger

        for rm_info, cfg, result in zip(self.cfg_tests['configure']['rm_info'],
                                        self.cfg_tests['configure']['cfg'],
                                        self.cfg_tests['configure']['result']):

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


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch.object(Continuous, '_find_resources',
                       return_value=[{'name'    : 'a',
                                      'uid'     : 1,
                                      'core_map': [[0]],
                                      'gpu_map' : [[0]],
                                      'lfs'     : {'path': '/dev/null',
                                                   'size': 1234},
                                      'mem'     : 128}])
    @mock.patch.object(Continuous, '_change_slot_states',
                       return_value=True)
    @mock.patch('radical.utils.Logger')
    def test_schedule_task(self,
                           mocked_init,
                           mocked_configure,
                           mocked_find_resources,
                           mocked_change_slot_states,
                           mocked_Logger):

        for task_descr in self.tasks:
            component = Continuous(cfg=None, session=None)
            task = dict()
            task['uid'] = task_descr['task']['uid']
            task['description'] = task_descr['task']['description']
            nodes = self.cfg_tests['allocate']['nodes'][0]
            component.nodes = nodes
            component._colo_history      = dict()
            component._rm_cores_per_node = 32
            component._rm_gpus_per_node  = 2
            component._rm_lfs_per_node   = {"size": 0, "path": "/dev/null"}
            component._rm_mem_per_node   = 1024
            component._rm_lm_info        = dict()
            component._rm_partitions     = dict()
            component._log               = mocked_Logger
            component._tagged_nodes      = set()
            component._scattered         = None
            component._dvm_host_list     = None
            component._node_offset       = 0

            slot = component.schedule_task(task)
            self.assertEqual(slot, task_descr['results']['slots'])
            self.assertEqual(component._colo_history, task_descr['results']['colo_history'])

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
            colo_history = {task_tags[i]: [u, u] for i, u in enumerate(node_uids)}
            self.assertEqual(component._colo_history, colo_history)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_change_slot_states',
                       return_value=True)
    @mock.patch('radical.utils.Logger')
    def test_unschedule_task(self, mocked_init, mocked_change_slot_states,
                                   mocked_Logger):

        for task_descr in self.tasks:
            task = {
                    'description': task_descr['task']['description'],
                    'slots'      : task_descr['setup']['lm']['slots']
                   }
            component = Continuous(cfg=None, session=None)
            component.nodes = task_descr['setup']['nodes']
            component._log  = mocked_Logger

            component.unschedule_task(task)

            self.assertEqual(component.nodes[0]['cores'], [0,1,2,3,4,5,6,7])
            self.assertEqual(component.nodes[0]['gpus'], [0,1,2])


if __name__ == '__main__':

    tc = TestContinuous()
    tc.test_configure()
    tc.test_unschedule_task()
    tc.test_find_resources()
    tc.test_schedule_task()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
