import os
import shutil
import errno
import unittest
import json

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.aprun import APRun

try:
    import mock
except ImportError:
    from unittest import mock

session_id = 'rp.session.test.aprun'


class TestAPRUNlaunchMethod(unittest.TestCase):

    def setUp(self):

        self._session = rp.Session()

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
                                        'uid': 'node1',
                                        'core_map': [[0]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}},
                                       {'name': 'node2',
                                        'uid': 'node2',
                                        'core_map': [[0]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}}],
                             'cores_per_node': 16,
                             'gpus_per_node': 0,
                             'lfs_per_node': 100,
                             'task_offsets': [2],
                             'lm_info': {'cores_per_node': 16}
                             }

        return

    def tearDown(self):

        self._session.close(cleanup=True)

    @mock.patch.object(APRun, '__init__', return_value=None)

    @mock.patch('radical.utils.raise_on')
    def test_aprun_construct(self, mocked_init, mocked_raise_on):
        launch_method = APRun(cfg={'Testing'}, session=self._session)
        launch_method.launch_command = 'aprun'
        launch_method._log  = ru.get_logger('dummy')

        aprun_cmd, _ = launch_method.construct_command(
            self._cu, launch_script_hop=1)

        print ''
        print aprun_cmd
        print 'aprun -n 2 -N 1 -L node1,node2 -d 1 -cc 0 test_exe'
        self.assertTrue(aprun_cmd == 'aprun -n 2 -N 1 -L node1,node2 -d 1 -cc 0 test_exe')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestAPRUNlaunchMethod)
    unittest.TextTestRunner(verbosity=3).run(suite)
