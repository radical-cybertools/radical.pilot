# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"
import os
import json
import glob
import pytest
import shutil
import unittest
import radical.utils as ru
from radical.pilot.agent.scheduler.continuous import Continuous

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def setUp():
    test_cases = json.load(open(os.path.abspath('unit_test_cases_continuous_scheduler.json')))

    return test_cases.pop('cfg'),test_cases['allocate']['nodes'],test_cases['allocate']['slots'],test_cases['allocate']['trigger']


# ------------------------------------------------------------------------------
#
def tearDown():


    rp = glob.glob('%s/rp.session.*' % os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)


class TestContinuous(unittest.TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_configure(mocked_init,
                       mocked_agent,
                       mocked_method):


        cfg,_,_,_ = setUp()
        component = Continuous(cfg=cfg, session=None)
        component._cfg = cfg
        component._rm_node_list = [["a", 1],
                                   ["b", 2],["c",3]]
        component._rm_cores_per_node = 8
        component._rm_gpus_per_node  = 2
        component._rm_lfs_per_node   = {"path": "/dev/null", "size": 0}
        component._rm_mem_per_node   = 128

        for i in range (len(cfg['rm_info'])):
            try:
                assert component._rm_node_list == cfg['rm_info'][i]['node_list']
                assert component._rm_cores_per_node == cfg['rm_info'][i]['cores_per_node']
                assert component._rm_gpus_per_node == cfg['rm_info'][i]['gpus_per_node']
                assert component._rm_lfs_per_node == cfg['rm_info'][i]['lfs_per_node']
                assert component._rm_mem_per_node == cfg['rm_info'][i]['mem_per_node']
                component._configure()

            except :
                with pytest.raises(AssertionError) as e:
                    component._configure()
                    raise
        tearDown()


# ------------------------------------------------------------------------------
#
    @mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    def test_find_resources(mocked_init,
                            mocked_agent,
                            mocked_configure,
                            mocked_method):


        _,_,cfg,_ = setUp()
        component = Continuous(cfg=cfg, session=None)
        component.node = {'name' : 'a',
                          'uid': 2,
                          'cores': [0, 0, 0, 0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 0, 0, 0, 0],
                          'lfs': {"size": 1234, "path": "/dev/null"},
                          'mem': 1024,
                          'gpus': [0, 0]}
        component._rm_lfs_per_node   = {"path": "/dev/null", "size": 1234}
        component.cores_per_slot = 16
        component.gpus_per_slot = 2
        component.lfs_per_slot = 1234
        component.mem_per_slot = 1024
        component.find_slot = 1

        test_slot = component._find_resources(node           =component.node,
                                              find_slots     =component.find_slot,
                                              cores_per_slot =component.cores_per_slot,
                                              gpus_per_slot  =component.gpus_per_slot,
                                              lfs_per_slot   =component.lfs_per_slot,
                                              mem_per_slot   =component.mem_per_slot,
                                              partial        ='None')
        component.slot = [{'name': 'a',
                           'uid': 2,
                           'core_map': [[0, 1, 2, 3, 4, 5, 6, 7, 8,
                                         9, 10, 11, 12, 13, 14, 15]],
                           'gpu_map': [[0], [1]],
                           'lfs': {'path': '/dev/null', 'size': 1234},
                           'mem': 1024}]

        for i in range (len(cfg)):
            try:
                assert [cfg[i]] == test_slot
            except:
                with pytest.raises(AssertionError) as e:
                    raise
        tearDown()


# ------------------------------------------------------------------------------
# 
    @mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch.object(Continuous, '_iterate_nodes', return_value=None)
    @mock.patch.object(Continuous, '_find_resources',
                       return_value=[{'name': 'a',
                                      'uid': 2,
                                      'core_map': [[0, 1, 2, 3, 4, 5, 6, 7, 8,
                                                    9, 10, 11, 12, 13, 14, 15]],
                                      'gpu_map': [[0], [1]],
                                      'lfs': {'path': '/dev/null', 'size': 1234},
                                      'mem': 1024}])
    def test_schedule_unit(mocked_init,
                           mocked_agent,
                           mocked_configure,
                           mocked_method,
                           mocked_iterate_nodes,
                           mocked_find_resources):

        _,_,_,cfg = setUp()
        component = Continuous(cfg=None, session=None)
        cud =  {"environment" : {},
                          "cpu_process_type" : 'null',
                          "gpu_process_type" : 'null',
                          "cpu_threads" : 1,
                          "gpu_processes" : 0,
                          "cpu_processes" :  1,
                          "mem_per_process": 128,
                          "lfs_per_process" : 0}

        component._uid = 1
        component._log = ru.Logger('dummy')
        component.req_slots      = cud['cpu_processes']
        component.cores_per_slot = cud['cpu_threads']
        component.gpus_per_slot  = cud['gpu_processes']
        component.lfs_per_slot   = cud['lfs_per_process']
        component.mem_per_slot   = cud['mem_per_process']
        component._schedule_units()

        for i in range (len(cfg)):
            try:
                assert cfg[i]['cpu_processes']   == component.req_slots     
                assert cfg[i]['cpu_threads']     == component.cores_per_slot
                assert cfg[i]['gpu_processes']   == component.gpus_per_slot  
                assert cfg[i]['lfs_per_process']['size'] == component.lfs_per_slot   
                assert cfg[i]['mem_per_process'] == component.mem_per_slot  
            except:
                pass
