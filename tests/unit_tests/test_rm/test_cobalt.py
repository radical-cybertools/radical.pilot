
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.cobalt import Cobalt


class TestCobalt(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.sh_callout', side_effect=[('node1', None, None),
                                                         ('16', None, None)])
    def test_configure(self, mocked_init, mocked_raise_on, mocked_sh_callout):

        # Test 1 no config file
        os.environ['COBALT_PARTSIZE'] = '1'

        component = Cobalt(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {'cores_per_node': None}
        component._configure()

        self.assertEqual(component.node_list, [['node1','node1']])
        self.assertEqual(component.cores_per_node, 16)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})


if __name__ == '__main__':

    tc = TestCobalt()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
