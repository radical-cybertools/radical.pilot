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
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = socket.gethostname()

        for host in resources.keys():
            if host in hostname:
                cls.host = host
                cls.resource = resources[host]
                break

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        component = MPIRun(name=None, cfg=None, session=None)
        component.name = 'mpirun'
        component._log = mocked_Logger
        component._cfg = ru.Munch({'resource': self.host})
        component.env_removables = []
        component._configure()

        self.assertEqual(component.launch_command, self.resource['mpirun_path'])
        self.assertEqual(component.mpi_version, self.resource['mpi_version'])
        self.assertEqual(component.mpi_flavor, self.resource['mpi_flavor'])
    # --------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
