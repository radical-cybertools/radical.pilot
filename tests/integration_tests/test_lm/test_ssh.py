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

        for host in resources.keys():
            if host in hostname:
                return resources[host]

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):
        cfg = self.setUp()
        component = SSH(name=None, cfg=None, session=None)
        component._log = mocked_Logger
        component._cfg = {}
        component.env_removables = []
        component._configure()
        command = cfg['ssh_path'] + ' -o StrictHostKeyChecking=no -o ControlMaster=auto'
        self.assertEqual(component.launch_command, command)
    # --------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
