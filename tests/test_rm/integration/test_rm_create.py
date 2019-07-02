import os
import glob
import shutil

import radical.pilot as rp

from radical.pilot.agent import rm as rpa_rm

# ------------------------------------------------------------------------------
#
cur_dir       = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------------------
#
def setUp(resource):

    session      = rp.Session()
    cfg          = session.get_resource_config(resource=resource)

    return cfg, session


# ------------------------------------------------------------------------------
#
def tearDown():

    rp = glob.glob('%s/rp.session.*' % cur_dir)

    for fold in rp:
        shutil.rmtree(fold)


# ------------------------------------------------------------------------------
#
def test_rm_create_fork():

    cfg, session = setUp('local.localhost')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    # The structure of the node list is
    # [[node1 name, node1 uid],[node2 name, node2 uid]]
    # The node name and uid can be the same

    # Check if the lrms object has the expected lrms_info dict
    # This dict is required by the scheduler and lm
    '''
    assert lrms.lrms_info == {'agent_nodes'     : {},
                              'gpus_per_node'   : 6,
                              'cores_per_node'  : 20,
                              'cores_per_socket': 10,
                              'gpus_per_socket' : 3,
                              'sockets_per_node': 2,
                              'lfs_per_node'    : {'path': None, 'size': 0},
                              'lm_info'         : {},
                              'name'            : 'LSF_SUMMIT',
                              'node_list'       : [['r0c0n11', 1],
                                                   ['r0c0n18', 2],
                                                   ['r0c1n15', 3],
                                                   ['r0c1n16', 4]]}
    '''
    tearDown()


def test_rm_create_pbspro():

    cfg = session.get_resource_config(resource='epsrc.archer_aprun')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    tearDown()


# ------------------------------------------------------------------------------

