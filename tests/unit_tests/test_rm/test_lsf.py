#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager     import RMInfo
from radical.pilot.agent.resource_manager.lsf import LSF

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class LSFTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._host_file_path = '%s/test_cases/nodelist.lsf' % base

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info(self, mocked_logger, mocked_init):

        rm_lsf = LSF(cfg=None, log=None, prof=None)
        rm_lsf._log = mocked_logger

        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

        rm_info = RMInfo({'sockets_per_node': 1,
                          'gpus_per_node'   : 0})
        rm_info = rm_lsf._update_info(rm_info)

        self.assertEqual(rm_info.node_list, [['nodes1', '1'], ['nodes2', '2']])
        self.assertEqual(rm_info.cores_per_node,   20)
        self.assertEqual(rm_info.cores_per_socket, 20)
        self.assertEqual(rm_info.gpus_per_socket,  0)

        rm_info = RMInfo({'sockets_per_node': 2,
                          'gpus_per_node'   : 2})
        rm_info = rm_lsf._update_info(rm_info)

        self.assertEqual(rm_info.cores_per_node,   20)
        self.assertEqual(rm_info.cores_per_socket, 10)
        self.assertEqual(rm_info.gpus_per_socket,  1)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info_error(self, mocked_logger, mocked_init):

        rm_lsf = LSF(cfg=None, log=None, prof=None)
        rm_lsf._log = mocked_logger

        if 'LSB_DJOB_HOSTFILE' in os.environ:
            del os.environ['LSB_DJOB_HOSTFILE']
        with self.assertRaises(RuntimeError):
            rm_lsf._update_info(None)

        # having 2 sockets per node and 1 GPU per node will cause an error
        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path
        rm_info = RMInfo({'sockets_per_node': 2,
                          'gpus_per_node'   : 1})
        with self.assertRaises(AssertionError):
            rm_lsf._update_info(rm_info)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = LSFTestCase()
    tc.test_update_info()
    tc.test_update_info_error()


# ------------------------------------------------------------------------------
