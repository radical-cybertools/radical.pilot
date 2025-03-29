#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import socket

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.ssh import SSH


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    def setUp(self) -> dict:
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = socket.gethostname()

        if ru.is_localhost(hostname):
            hostname = 'localhost'

        for host in resources.keys():
            if host in hostname:
                return resources[host]

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__',   return_value=None)
    def test_configure(self, mocked_init):
        cfg = self.setUp()

        if not cfg:
            return

        component = SSH('', {}, None, None, None)
        component._log = mock.Mock()
        lm_info = component.init_from_scratch({}, '')
        component.init_from_info(lm_info)

        command = cfg['ssh_path'] + ' -o StrictHostKeyChecking=no -o ControlMaster=auto'
        self.assertEqual(component._command, command)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.setUpClass()
    tc.test_configure()


# ------------------------------------------------------------------------------

