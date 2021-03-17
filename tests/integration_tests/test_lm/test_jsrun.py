# pylint: disable=protected-access, unused-argument, no-value-for-parameter
import os
from glob import glob
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

        cls.host = 'summit'
        cls.resource = resources[cls.host]
        hostfile = os.environ['LSB_DJOB_HOSTFILE']
        with open(hostfile) as hosts:
            nodes = hosts.readlines()
        cls.node_name = nodes[1].strip()

        cls.task_test_cases = []
        for task_file in glob(os.path.dirname(__file__) + '/../test_config/task*.json'):
            cls.task_test_cases.append(ru.read_json(task_file))

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

        for test_case in self.task_test_cases:
            task = test_case['task']
            result = test_case['result']
            for i in range(len(result)):
                if '{node}' in result[i]:
                    print(result[i])
                    result[i] = result[i].format(node=self.node_name)

            component = JSRUN(name=None, cfg=None, session=None)
            component._log = mocked_Logger
            component._configure()

            command, _ = component.construct_command(task, None)

            p = sp.Popen(command, stdout=sp.PIPE,
                         stderr=sp.PIPE, shell=True)
            p.wait()

            output = p.stdout.readlines()
            output = [x.decode('utf-8') for x in output]
            self.assertEqual(output,result)

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

