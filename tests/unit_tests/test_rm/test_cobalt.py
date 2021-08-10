# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager.cobalt import Cobalt


# ------------------------------------------------------------------------------
#
class TestCobalt(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_logger, mocked_init):

        os.environ['COBALT_PARTNAME'] = '1'

        component = Cobalt(cfg=None, session=None)
        component._log = mocked_logger
        component._cfg = ru.Config(from_dict={'cores_per_node': 16})
        component._configure()

        self.assertEqual(component.node_list, [['nid00001', '1']])
        self.assertEqual(component.cores_per_node, 16)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestCobalt()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
