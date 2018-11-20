import os
import shutil
import errno
import unittest
import json

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.orte import ORTE

try:
    import mock
except ImportError:
    from unittest import mock

session_id = 'rp.session.test.orte'

class TestORTElaunchMethod(unittest.TestCase):


    def setUp(self):

        self._session = rp.Session(uid=session_id)

        self._cu = dict()

        self._cu['description'] = {"arguments": [],
                                   "cleanup": False,
                                   "cpu_process_type": '',
                                   "cpu_processes": 1,
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
                                        'uid': 'node_1',
                                        'core_map': [[0]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}}],
                             'cores_per_node': 16,
                             'gpus_per_node': 0,
                             'lfs_per_node': 100,
                             'lm_info': {'dvm_uri': 'test'}
                            }

        return

    def tearDown(self):

        self._session.close(cleanup=True)
        shutil.rmtree(os.getcwd()+'/'+session_id)

    @mock.patch.object(ORTE,'__init__',return_value=None)
    @mock.patch('radical.utils.raise_on')       
    def test_orte_nompi_construct(self,mocked_init,mocked_raise_on):
        launch_method = ORTE(cfg={'Testing'}, session = self._session)
        launch_method.launch_command = 'orterun'
        launch_method._log = ru.get_logger('dummy')

        orte_cmd, _ = launch_method.construct_command(self._cu, launch_script_hop=1)
        
        os.environ['LD_LIBRARY_PATH'] = ''
        os.environ['PYTHONPATH']=''
        env_string  = ' '.join(['-x "%s"' % (var)
                                for var in ['LD_LIBRARY_PATH','PATH','PYTHONPATH']
                                if  var in os.environ])

        self.assertTrue(orte_cmd == 'orterun  --hnp "test" -np 1 --bind-to none -host 1 %s  test_exe' % env_string)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = unittest.TestLoader().loadTestsFromTestCase(TestORTElaunchMethod)
    unittest.TextTestRunner(verbosity=2).run(suite)
