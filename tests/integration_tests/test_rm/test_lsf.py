#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pytest

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager     import RMInfo
from radical.pilot.agent.resource_manager.lsf import LSF


# ------------------------------------------------------------------------------
#
class LSFTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):

        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        cls.resource = ru.read_json(path)['summit']

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF, '__init__',   return_value=None)
    @pytest.mark.skipif(
        'LSB_DJOB_HOSTFILE' not in os.environ,
        reason='test needs to run in LSF allocation')
    def test_update_info(self, mocked_init):

        if not self.resource:
            return

        rm_lsf = LSF(cfg=None, log=None, prof=None)
        rm_lsf._log = mock.Mock()

        rm_info = rm_lsf._update_info(RMInfo({'sockets_per_node': 2,
                                              'gpus_per_node'   : 6}))

        self.assertEqual(rm_info.cores_per_socket,
                         self.resource['cores_per_socket'])
        self.assertEqual(rm_info.gpus_per_socket,
                         self.resource['gpus_per_socket'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = LSFTestCase()
    tc.setUpClass()
    tc.test_update_info()


# ------------------------------------------------------------------------------

