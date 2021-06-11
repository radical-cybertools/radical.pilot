# pylint: disable=unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
class NewResourceManager(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _configure(self):
        self.cores_per_node = 1
        self.node_list = [['node_name', 'node.0000']]


# ------------------------------------------------------------------------------
#
class TestBaseResourceManager(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.resource_manager.base.rpa.LaunchMethod')
    def test_init_from_scratch(self, mocked_lm, mocked_init):

        rm_base = ResourceManager(cfg=None, session=None)
        rm_base.name = 'ResourceManager'
        rm_base._log = mock.Mock()

        # required config keys: `cores`, `gpus`
        with self.assertRaises(KeyError):
            rm_base._cfg = {'cores': 1}
            rm_base._init_from_scratch()

        with self.assertRaises(KeyError):
            rm_base._cfg = {'gpus': 1}
            rm_base._init_from_scratch()

        default_cfg = ru.Munch(from_dict={
            'cores': 1,
            'gpus': 0,
            'resource_cfg': {'order': [],
                             'launch_methods': {'SRUN': {}}}
        })

        # RuntimeError if `self.cores_per_node` or `self.node_list` not set,
        # these attributes are set by an overwritten method `_configure`
        with self.assertRaises(RuntimeError):
            rm_base._cfg = default_cfg
            rm_base._init_from_scratch()

        rm = NewResourceManager(cfg=None, session=None)
        rm.name = 'NewResourceManager'
        rm._cfg = default_cfg
        rm._log = rm._prof = mock.Mock()

        mocked_lm.create.return_value = mocked_lm
        rm._init_from_scratch()

        self.assertEqual(rm._launchers['SRUN'], mocked_lm)

        # check that `self.rm_info` has all necessary keys
        rm_info_keys = ['name', 'node_list', 'partitions', 'agent_nodes',
                        'cores_per_node', 'gpus_per_node', 'lfs_per_node',
                        'mem_per_node', 'service_node']
        for rm_info_key in rm.rm_info.keys():
            self.assertIn(rm_info_key, rm_info_keys)

# ------------------------------------------------------------------------------
# pylint: enable=unused-argument
