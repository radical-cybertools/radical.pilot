# pylint: disable=protected-access, unused-argument, no-value-for-parameter
import os
import socket

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.mpirun import MPIRun


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    def setUp(self) -> dict:
        path = os.path.dirname(__file__) + '../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = socket.gethostname()

        for host in resources.keys():
            if host in hostname:
                return resources[host]

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__',   return_value=None)
    def test_configure(self, mocked_init):
        cfg = self.setUp()
        component = MPIRun(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.env_removables = []
        component._configure()
        self.assertEqual(component.launch_command, cfg['mpirun_path'])
        self.assertEqual(component.mpi_flavor, cfg['mpi_flavor'])
        self.assertEqual(component.mpi_version, cfg['mpi_version'])
    # --------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
