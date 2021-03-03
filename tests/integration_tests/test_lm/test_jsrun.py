# pylint: disable=protected-access, unused-argument, no-value-for-parameter
import os
import subprocess as sp

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.jsrun import JSRUN


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = 'summit'

        for host in resources.keys():
            if host in hostname:
                cls.host = host
                cls.resource = resources[host]
                break

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):
        component = JSRUN(name=None, cfg=None, session=None)
        component._log = mocked_Logger
        component._configure()
        self.assertEqual(component.launch_command, self.resource['jsrun_path'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_command(self, mocked_init, mocked_Logger):
        cfg = {
                  "task": {
                      "uid":         "task.000000",
                      "description": {"executable"   : "/bin/echo",
                                      "arguments"    : ["Hello from Summit"],
                                      "cpu_processes" : 1
                                     },
                      "task_sandbox_path": ".",
                      "slots": {
                              "cores_per_node": 42,
                              "gpus_per_node" : 6,
                              "lfs_per_node"  : 0,
                              "nodes"         : [{"name"    : "1",
                                                  "uid"     : 1,
                                                  "core_map": [[0]],
                                                  "gpu_map" : [],
                                                  "lfs"     : {"size": 0,
                                                               "path": None}
                              }]
                          }
                  }
              }
        component = JSRUN(name=None, cfg=None, session=None)
        component._log = mocked_Logger
        component._configure()

        command, hop = component.construct_command(cfg["task"], None)

        p = sp.Popen(command, stdout=sp.PIPE,
                         stderr=sp.PIPE, shell=True)
        p.wait()

        output = p.stdout.readlines()
        self.assertEqual(output, [b'Hello from Summit\n'])

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

