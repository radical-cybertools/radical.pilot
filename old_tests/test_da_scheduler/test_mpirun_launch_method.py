import os
import shutil
import errno
import unittest
import json

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.mpirun import MPIRun

try:
    import mock
except ImportError:
    from unittest import mock

session_id = 'rp.session.test.mpirun'

class TestMPIRUNlaunchMethod(unittest.TestCase):


    def setUp(self):

        self._session = rp.Session(uid=session_id)

        self._cu = dict()

        self._cu['description'] = {"arguments": [],
                                   "cleanup": False,
                                   "cpu_process_type": 'MPI',
                                   "cpu_processes": 4,
                                   "cpu_thread_type": "OpenMP",
                                   "cpu_threads": 1,
                                   "environment": {}, 
                                   "executable": "test_exe",
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
        self._cu['uid'] = 'unit.000000'
        self._cu['slots'] = {'nodes': [{'name': 'node1',
                                        'uid': 1,
                                        'core_map': [[0],[1]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}},
                                        {'name': 'node2',
                                        'uid': 2,
                                        'core_map': [[0],[1]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}}],
                             'cores_per_node': 16,
                             'gpus_per_node': 0,
                             'lfs_per_node': 100,
                             'task_offsets': [2],
                             'lm_info': {'cores_per_node':16}
                            }

        return

    def tearDown(self):

        self._session.close(cleanup=True)
        shutil.rmtree(os.getcwd()+'/'+session_id)

    @mock.patch.object(MPIRun,'__init__',return_value=None)
    @mock.patch('radical.utils.raise_on')       
    def test_mpirun_construct(self,mocked_init,mocked_raise_on):
        launch_method = MPIRun(cfg={'Testing'}, session = self._session)
        launch_method.launch_command = 'mpirun'
        launch_method.mpi_version = None
        launch_method.mpi_flavor = None
        launch_method._log  = ru.Logger('dummy')

        mpirun_cmd, _ = launch_method.construct_command(self._cu, launch_script_hop=1)
        
        self.assertTrue(mpirun_cmd == 'mpirun -np 4 -host node1,node1,node2,node2  test_exe')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestMPIRUNlaunchMethod)
    unittest.TextTestRunner(verbosity=2).run(suite)
