#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter


import os
import socket

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.mpirun import MPIRun

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        path      = '%s/../test_config/resources.json' % base
        resources = ru.read_json(path)
        hostname  = socket.gethostname()

        if ru.is_localhost(hostname):
            hostname = 'localhost'

        cls.host = None
        for host in resources.keys():
            if host in hostname:
                cls.host     = host
                cls.resource = resources[host]
                break


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__',   return_value=None)
    def test_configure(self, mocked_init):

        if not self.host:
            return

        component = MPIRun('', {}, None, None, None)
        component.name = 'MPIRUN'
        component._log = mock.Mock()
        lm_info = component._init_from_scratch({}, '')
        component._init_from_info(lm_info)

        self.assertEqual(component._command,     self.resource['mpirun_path'])
        self.assertEqual(component._mpi_version, self.resource['mpi_version'])
        self.assertEqual(component._mpi_flavor,  self.resource['mpi_flavor'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.setUpClass()
    tc.test_configure()


# ------------------------------------------------------------------------------

