
# pylint: disable=protected-access, unused-argument
import os

import radical.utils as ru
from radical.pilot.agent.rm.slurm import Slurm

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Slurm, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes2'])
def test_configure(mocked_init, mocked_raise_on, mocked_expand_hostlist):

    # Test 1 no config file
    os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
    os.environ['SLURM_NPROCS'] = '48'
    os.environ['SLURM_NNODES'] = '2'
    os.environ['SLURM_CPUS_ON_NODE'] = '24'

    component = Slurm(cfg=None, session=None)
    component._log = ru.get_logger('dummy')
    component._cfg = {}
    component.lm_info = {'cores_per_node': None}
    component._configure()

    assert component.node_list == [['nodes1','nodes1'],['nodes2','nodes2']]
    assert component.cores_per_node == 24
    assert component.gpus_per_node == 0
    assert component.lfs_per_node == {'path': None, 'size': 0}

    # Test 2 config file
    os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
    os.environ['SLURM_NPROCS'] = '48'
    os.environ['SLURM_NNODES'] = '2'
    os.environ['SLURM_CPUS_ON_NODE'] = '24'

    component = Slurm(cfg=None, session=None)
    component._log = ru.get_logger('dummy')
    component._cfg = {'cores_per_node': 24,
                      'gpus_per_node': 1,
                      'lfs_path_per_node': 'test/',
                      'lfs_size_per_node': 100}
    component.lm_info = {'cores_per_node': None}
    component._configure()
    assert component.node_list == [['nodes1','nodes1'],['nodes2','nodes2']]
# ------------------------------------------------------------------------------
