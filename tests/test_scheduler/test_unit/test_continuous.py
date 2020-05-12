# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import glob
import pytest
import shutil
import unittest

import radical.utils as ru
from   radical.pilot.agent.scheduler.continuous import Continuous


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
class TestContinuous(unittest.TestCase):

    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        fname      = 'tests/test_scheduler/test_unit/test_cases/test_continuous.json'
        test_cases = ru.read_json(fname)

        return [test_cases.pop('cfg'),
                test_cases['allocate']['nodes'],
                test_cases['allocate']['slots'],
                test_cases['allocate']['trigger']]


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_configure(self, mocked_init):

        cfg,_,_,_ = self.setUp()
        component = Continuous(cfg=cfg, session=None)
        component._cfg = cfg
        component._rm_node_list = [["a", 1],
                                   ["b", 2],["c",3]]
        component._rm_cores_per_node = 8
        component._rm_gpus_per_node  = 2
        component._rm_lfs_per_node   = {"path": "/dev/null", "size": 0}
        component._rm_mem_per_node   = 128

        for i in range (len(cfg['rm_info'])):
            rm_info = cfg['rm_info'][i]
            try:
                self.assertEqual(component._rm_node_list, rm_info['node_list'])
                self.assertEqual(component._rm_cores_per_node, rm_info['cores_per_node'])
                self.assertEqual(component._rm_gpus_per_node, rm_info['gpus_per_node'])
                self.assertEqual(component._rm_lfs_per_node, rm_info['lfs_per_node'])
                self.assertEqual(component._rm_mem_per_node, rm_info['mem_per_node'])
                component._configure()

            except:
                with pytest.raises(AssertionError):
                    component._configure()
                    raise


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    def test_find_resources(self,
                            mocked_init,
                            mocked_configure):

        _,_,cfg,_ = self.setUp()
        component = Continuous(cfg=cfg, session=None)
        component.node = {'name'  : 'a',
                          'uid'   : 2,
                          'cores' : [0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 0],
                          'lfs'   : {"size": 1234,
                                     "path" : "/dev/null"},
                          'mem'   : 1024,
                          'gpus'  : [0, 0]}

        component._rm_lfs_per_node = {"path" : "/dev/null", "size" : 1234}
        component.cores_per_slot   = 16
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

        component.slot = [{'name'     : 'a',
                           'uid'      : 2,
                           'core_map' : [[0, 1, 2, 3, 4, 5, 6, 7, 8,
                                          9, 10, 11, 12, 13, 14, 15]],
                           'gpu_map'  : [[0], [1]],
                           'lfs'      : {'path': '/dev/null',
                                         'size' : 1234},
                           'mem'      : 1024}]

        for i in range (len(cfg)):
            try:
                self.assertEqual([cfg[i]], test_slot)
            except:
                with pytest.raises(AssertionError):
                    raise


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
    def test_schedule_unit(self,
                           mocked_init,
                           mocked_configure,
                           mocked_find_resources):

        _,_,_,cfg = self.setUp()
        component = Continuous(cfg=cfg, session=None)

        unit = dict()
        unit['uid'] = 'unit.000000'
        unit['description'] = {'environment': {},
                                "cpu_process_type" : 'null',
                                "gpu_process_type" : 'null',
                                "cpu_threads"      : 1,
                                "gpu_processes"    : 0,
                                "cpu_processes"    : 1,
                                "mem_per_process"  : 128,
                                "lfs_per_process"  : 0}

        unit['slots'] = {"nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                    "cores" : [1], 
                                    "core_map": [[0]],
                                    "name": "a",
                                    "gpus": [1],
                                    "gpu_map": [[0]],
                                    "uid": 1,
                                    "mem": None}]}            
        component.nodes = unit['slots']['nodes']
        component._rm_cores_per_node = 32
        component._rm_gpus_per_node  = 2
        component._rm_lfs_per_node   = {"size": 0, "path": "/dev/null"}
        component._rm_mem_per_node   = 1024
        component._rm_lm_info = 'INFO'
        component._log = ru.Logger('dummy')
        component._node_offset = 0 
        test_slot =  {'cores_per_node': 32,
                      'gpus_per_node': 2,
                      'lfs_per_node': {'path': '/dev/null', 'size': 0},
                      'lm_info': 'INFO',
                      'mem_per_node': 1024,
                      'nodes': [{'core_map': [[0]],
                                 'gpu_map' : [[0]],
                                 'lfs': {'path': '/dev/null', 'size': 1234},
                                 'mem': 128,
                                 'name': 'a',
                                 'uid': 1}]}
        try:
            self.assertEqual(component.schedule_unit(unit), test_slot)
        except:
            with pytest.raises(AssertionError):
                raise
 

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_unschedule_unit(self, mocked_init):

        component = Continuous(cfg=None, session=None)
        unit = dict()
        unit['description'] = {'environment': {}}
        unit['slots'] = {"cores_per_node": 16,
                         "lfs_per_node": {"size": 0, "path": "/dev/null"},
                         "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "cores" : [1], 
                                "core_map": [[0]],
                                "name": "a",
                                "gpus": [1],
                                "gpu_map": [[0]],
                                "uid": 1,"mem": None}],
                         "lm_info": "INFO",
                         "gpus_per_node": 0,}
        component.nodes = unit['slots']['nodes']
        try:
            self.assertEqual(component.unschedule_unit(unit), None)
        except:
            pass
