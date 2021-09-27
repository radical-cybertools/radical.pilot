# pylint: disable=unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from radical.pilot.agent.resource_manager import ResourceManager

from unittest import mock, TestCase


# ------------------------------------------------------------------------------
#
class NewResourceManager(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _configure(self):
        self.cores_per_node = 1
        self.node_list = [['node', 'node.0000']]


# ------------------------------------------------------------------------------
#
class TestBaseResourceManager(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_init(self):

        # required config keys: `cores`, `gpus`
        with self.assertRaises(KeyError):
            _ = ResourceManager(cfg={'cores': 1}, session=mock.Mock())  # noqa

        with self.assertRaises(KeyError):
            _ = ResourceManager(cfg={'gpus': 1}, session=mock.Mock())  # noqa

        default_cfg = {'cores': 1, 'gpus': 0}

        # RuntimeError if `self.cores_per_node` or `self.node_list` not set,
        # these attributes are set by an overwritten method `_configure`
        with self.assertRaises(RuntimeError):
            _ = ResourceManager(cfg=default_cfg, session=mock.Mock())  # noqa

        component = NewResourceManager(cfg=default_cfg, session=mock.Mock())

        self.assertEqual(component.name, 'NewResourceManager')
        self.assertEqual(component._cfg, default_cfg)

        # check that `self.rm_info` has all necessary keys
        rm_info_keys = ['name', 'lm_info', 'node_list', 'partitions',
                        'agent_nodes', 'cores_per_node', 'gpus_per_node',
                        'lfs_per_node', 'mem_per_node', 'service_node']
        for rm_info_key in component.rm_info.keys():
            self.assertIn(rm_info_key, rm_info_keys)


# ------------------------------------------------------------------------------
# pylint: enable=unused-argument
