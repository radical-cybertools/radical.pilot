import os
import glob
import json
import shutil
import unittest

import radical.utils as ru
import radical.pilot as rp

from radical.pilot.agent.executing.popen import Popen


try: 
    import mock 
except ImportError: 
    from unittest import mock

session_id = 'rp.session.test.cuda'



class TestStagingInputComponent(unittest.TestCase):


    def setUp(self):

        # Unit configuration
        self.unit_gpu = dict()
        self.unit_gpu['description'] = {"arguments": [],
                                        "cleanup": False,
                                        "cpu_process_type": None,
                                        "cpu_processes": 1,
                                        "cpu_thread_type": "OpenMP",
                                        "cpu_threads": 1,
                                        "environment": {}, 
                                        "executable": "/bin/date",
                                        "gpu_process_type": "CUDA",
                                        "gpu_processes": 1,
                                        "gpu_thread_type": None,
                                        "gpu_threads": 1,
                                        "input_staging": [],
                                        "kernel": None,
                                        "name": None,
                                        "output_staging": [],
                                        "pilot": None,
                                        "post_exec": [],
                                        "pre_exec": [],
                                        "restartable": False,
                                        "stderr": None,
                                        "stdout": None
                                       }
        self.unit_gpu['slots'] = "slots": {"cores_per_node": 7, 
                                           "gpus_per_node": 1, 
                                           "lm_info": {"version_info": {"FORK": {"version": "0.42", 
                                                                                 "version_detail": "There is no spoon"}}}, 
                                           "nodes": [["localhost", "localhost_0", [[0]], [0]]]
                                          }

        self.unit_cpu = dict()
        self.unit_cpu['description'] = {"arguments": [],
                                        "cleanup": False,
                                        "cpu_process_type": None,
                                        "cpu_processes": 1,
                                        "cpu_thread_type": "OpenMP",
                                        "cpu_threads": 1,
                                        "environment": {}, 
                                        "executable": "/bin/date",
                                        "gpu_process_type": None,
                                        "gpu_processes": 0,
                                        "gpu_thread_type": None,
                                        "gpu_threads": 0,
                                        "input_staging": [],
                                        "kernel": None,
                                        "name": None,
                                        "output_staging": [],
                                        "pilot": None,
                                        "post_exec": [],
                                        "pre_exec": [],
                                        "restartable": False,
                                        "stderr": None,
                                        "stdout": None
                                       }
        self.unit_cpu['slots'] = "slots": {"cores_per_node": 7, 
                                           "gpus_per_node": 1, 
                                           "lm_info": {"version_info": {"FORK": {"version": "0.42", 
                                                                                 "version_detail": "There is no spoon"}}}, 
                                           "nodes": [["localhost", "localhost_0", [[0]], []]]
                                          }

        
    def tearDown(self):
        
        pass


    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'advance')
    @mock.patch.object(Popen, 'spawn')
    @mock.patch.object(Popen,'publish')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_gpu_unit(self, mocked_init, mocked_advance, mocked_spawn, mocked_publish, mocked_profiler, mocked_raise_on):

        component       = Poper(cfg={'Testing'}, session=session_id)
        component._prof = mocked_profiler
        component._log  = ru.get_logger('dummy')
        component._mpi_launcher = 'mpirun'
        component._task_launcher = 'ssh'


        # Call the component's '_handle_unit' function
        # Should perform all of the actionables 
        component._handle_unit(self.unit_gpu)

        # Verify the actionables were done...
        self.assertTrue(self.unit_gpu['environment'].get('CUDA_VISIBLE_DEVICES',None),0)

    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'advance')
    @mock.patch.object(Popen, 'spawn')
    @mock.patch.object(Popen,'publish')
    @mock.patch.object(ru.Profiler, 'prof')
    @mock.patch('radical.utils.raise_on')
    def test_cpu_unit(self, mocked_init, mocked_advance, mocked_spawn, mocked_publish, mocked_profiler, mocked_raise_on):

        component       = Poper(cfg={'Testing'}, session=session_id)
        component._prof = mocked_profiler
        component._log  = ru.get_logger('dummy')
        component._mpi_launcher = 'mpirun'
        component._task_launcher = 'ssh'


        # Call the component's '_handle_unit' function
        # Should perform all of the actionables 
        component._handle_unit(self.unit_cpu)

        # Verify the actionables were done...
        self.assertTrue(self.unit_gpu['environment'].get('CUDA_VISIBLE_DEVICES',None),None)
                                                     

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestStagingInputComponent)
    unittest.TextTestRunner(verbosity=2).run(suite)
