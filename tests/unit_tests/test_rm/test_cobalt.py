#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager        import RMInfo
from radical.pilot.agent.resource_manager.cobalt import Cobalt


# ------------------------------------------------------------------------------
#
class CobaltTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info(self, mocked_logger, mocked_init):

        os.environ['COBALT_PARTNAME'] = '1'

        rm_cobalt = Cobalt(cfg=None, log=None, prof=None)
        rm_cobalt._log = mocked_logger

        rm_info = rm_cobalt._update_info(RMInfo())

        self.assertEqual(rm_info.node_list, [['nid00001', '1']])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info_error(self, mocked_logger, mocked_init):

        rm_cobalt = Cobalt(cfg=None, log=None, prof=None)
        rm_cobalt._log = mocked_logger

        for cobalt_env_var in ['COBALT_NODEFILE', 'COBALT_PARTNAME']:
            if cobalt_env_var in os.environ:
                del os.environ[cobalt_env_var]
        with self.assertRaises(RuntimeError):
            # both $COBALT_NODEFILE and $COBALT_PARTNAME were not set
            rm_cobalt._update_info(None)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = CobaltTestCase()
    tc.test_update_info()


# ------------------------------------------------------------------------------
