#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter
import os
import glob
import pytest

import subprocess    as sp
import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.jsrun import JSRUN


base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):

        path         = '%s/../test_config/resources.json' % base
        resources    = ru.read_json(path)
        cls.host     = 'summit'
        cls.resource = resources[cls.host]
        hostfile     = os.environ.get('LSB_DJOB_HOSTFILE')

        if not hostfile:
            return

        with ru.ru_open(hostfile) as hosts:
            nodes = hosts.readlines()

        cls.node_name       = nodes[1].strip()
        cls.task_test_cases = []

        for task_file in glob.glob('%s/../test_config/task*.json' % base):
            cls.task_test_cases.append(ru.read_json(task_file))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @pytest.mark.skipif(
        'LSB_DJOB_HOSTFILE' not in os.environ,
        reason='test needs to run in LSF allocation')
    def test_configure(self, mocked_init):

        component = JSRUN('', {}, None, None, None)
        lm_info = component._init_from_scratch({}, '')
        component._init_from_info(lm_info)

        self.assertEqual(component._command, self.resource['jsrun_path'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @pytest.mark.skipif(
        'LSB_DJOB_HOSTFILE' not in os.environ,
        reason='test needs to run in LSF allocation')
    def test_command(self, mocked_init):

        for test_case in self.task_test_cases:
            task = test_case['task']
            result = test_case['result']
            for i in range(len(result)):
                if '{node}' in result[i]:
                    print(result[i])
                    result[i] = result[i].format(node=self.node_name)

            log  = mock.Mock()
            prof = mock.Mock()

            component = JSRUN(name=None, lm_cfg=None, rm_info=None,
                                         log=log, prof=prof)
            component._init_from_scratch(None, None)

            # FIXME
            command, _ = component.construct_command(task, None)

            p = sp.Popen(command, stdout=sp.PIPE,
                         stderr=sp.PIPE, shell=True)
            p.wait()

            output = p.stdout.readlines()
            output = [x.decode('utf-8') for x in output]
            self.assertEqual(output,result)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.setUpClass()
    tc.test_configure()
    tc.test_command()


# ------------------------------------------------------------------------------

